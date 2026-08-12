from collections.abc import Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

from src.config.settings import get_settings
from src.connections.models import ConnectionNotFoundError
from src.connections.repository import LegacyEnvConnectionRepository
from src.observability.db_observer import attach_engine_observer


_settings = get_settings()
_engine_cache: dict[str, Engine] = {}
_sessionmaker_cache: dict[str, sessionmaker] = {}


def available_connections() -> dict[str, str]:
    return LegacyEnvConnectionRepository().urls()


def resolve_database_url(connection_id: str | None) -> str:
    connections = available_connections()
    cid = connection_id or "default"
    if cid not in connections:
        raise ConnectionNotFoundError(f"Connection '{cid}' was not found.")
    return connections[cid]


def _build_engine_kwargs(database_url: str) -> dict:
    kwargs = {
        "future": True,
        "pool_pre_ping": True,
        "pool_recycle": _settings.db_pool_recycle_seconds,
        "pool_timeout": _settings.db_pool_timeout_seconds,
    }
    drivername = make_url(database_url).drivername
    connect_args: dict = {}
    if drivername.startswith("postgresql"):
        connect_args["connect_timeout"] = _settings.db_connect_timeout_seconds
    if connect_args:
        kwargs["connect_args"] = connect_args
    return kwargs


def get_engine(connection_id: str | None = None) -> Engine:
    cid = connection_id or "default"
    if cid in _engine_cache:
        return _engine_cache[cid]
    resolved_url = resolve_database_url(cid)
    engine = create_engine(resolved_url, **_build_engine_kwargs(resolved_url))
    attach_engine_observer(engine)
    _engine_cache[cid] = engine
    return engine


def get_session_factory(connection_id: str | None = None) -> sessionmaker:
    cid = connection_id or "default"
    if cid in _sessionmaker_cache:
        return _sessionmaker_cache[cid]
    factory = sessionmaker(
        bind=get_engine(cid), autoflush=False, autocommit=False, future=True
    )
    _sessionmaker_cache[cid] = factory
    return factory


def get_db_session(connection_id: str | None = None) -> Iterator[Session]:
    factory = get_session_factory(connection_id)
    db = factory()
    try:
        yield db
    finally:
        db.close()


def check_connection(connection_id: str | None = None) -> tuple[bool, str | None]:
    cid = connection_id or "default"
    try:
        engine = get_engine(cid)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True, None
    except SQLAlchemyError as exc:
        return False, _safe_connection_error_code(str(exc))


def _safe_connection_error_code(message: str) -> str:
    normalized = message.lower()
    if "password" in normalized or "authentication" in normalized or "access denied" in normalized:
        return "authentication_failed"
    if "ssl" in normalized or "tls" in normalized or "certificate" in normalized:
        return "tls_failed"
    if "unsupported" in normalized or "server version" in normalized:
        return "unsupported_version"
    if "invalid" in normalized or "could not parse" in normalized:
        return "invalid_config"
    if "introspect" in normalized or "information_schema" in normalized:
        return "introspection_failed"
    return "unreachable"


def connections_health() -> dict[str, dict[str, str | bool]]:
    health: dict[str, dict[str, str | bool]] = {}
    for cid in available_connections().keys():
        ok, error = check_connection(cid)
        health[cid] = {"healthy": ok, "error": error or ""}
    return health
