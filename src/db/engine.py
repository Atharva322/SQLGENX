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


def available_connections(owner_id: str | None = None) -> dict[str, str]:
    urls = LegacyEnvConnectionRepository().urls()
    if owner_id:
        from src.connections.service import get_connection_service

        for connection in get_connection_service().repository.list(owner_id):
            urls[connection.id] = "<runtime>"
    return urls


def resolve_database_url(connection_id: str | None, owner_id: str | None = None) -> str:
    legacy_connections = LegacyEnvConnectionRepository().urls()
    cid = connection_id or "default"
    if cid in legacy_connections:
        return legacy_connections[cid]
    if owner_id:
        from src.connections.service import get_connection_service

        runtime = get_connection_service().resolve_runtime_url(owner_id, cid)
        if runtime is not None:
            return runtime[0]
    raise ConnectionNotFoundError(f"Connection '{cid}' was not found.")


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
    if drivername.startswith("mysql"):
        connect_args["connect_timeout"] = _settings.db_connect_timeout_seconds
    if connect_args:
        kwargs["connect_args"] = connect_args
    return kwargs


def connection_adapter_key(connection_id: str | None, owner_id: str | None = None) -> str:
    cid = connection_id or "default"
    legacy_connections = LegacyEnvConnectionRepository().urls()
    if cid in legacy_connections:
        driver = make_url(legacy_connections[cid]).drivername.split("+", 1)[0]
        if driver in {"postgresql", "postgres"}:
            return "postgresql"
        if driver in {"mysql", "mariadb"}:
            return "mysql"
        if driver in {"mssql", "sqlserver"}:
            return "sqlserver"
        return driver
    if owner_id:
        from src.connections.service import get_connection_service

        return get_connection_service().get_runtime_record(owner_id, cid).adapter_key
    raise ConnectionNotFoundError(f"Connection '{cid}' was not found.")


def _engine_cache_key(connection_id: str | None, owner_id: str | None = None) -> str:
    cid = connection_id or "default"
    legacy_connections = LegacyEnvConnectionRepository().urls()
    if cid in legacy_connections:
        return cid
    if owner_id:
        from src.connections.service import get_connection_service

        runtime = get_connection_service().resolve_runtime_url(owner_id, cid)
        if runtime is not None:
            _, version = runtime
            return f"{owner_id}:{cid}:v{version}"
    return cid


def dispose_connection_engine(connection_id: str, owner_id: str | None = None) -> None:
    prefix = f"{owner_id}:{connection_id}:" if owner_id else connection_id
    for key in [key for key in _engine_cache if key == prefix or key.startswith(prefix)]:
        _engine_cache.pop(key).dispose()
        _sessionmaker_cache.pop(key, None)


def get_engine(connection_id: str | None = None, owner_id: str | None = None) -> Engine:
    cache_key = _engine_cache_key(connection_id, owner_id)
    if cache_key in _engine_cache:
        return _engine_cache[cache_key]
    resolved_url = resolve_database_url(connection_id, owner_id=owner_id)
    engine = create_engine(resolved_url, **_build_engine_kwargs(resolved_url))
    attach_engine_observer(engine)
    _engine_cache[cache_key] = engine
    return engine


def get_session_factory(connection_id: str | None = None, owner_id: str | None = None) -> sessionmaker:
    cache_key = _engine_cache_key(connection_id, owner_id)
    if cache_key in _sessionmaker_cache:
        return _sessionmaker_cache[cache_key]
    factory = sessionmaker(
        bind=get_engine(connection_id, owner_id=owner_id), autoflush=False, autocommit=False, future=True
    )
    _sessionmaker_cache[cache_key] = factory
    return factory


def get_db_session(connection_id: str | None = None, owner_id: str | None = None) -> Iterator[Session]:
    factory = get_session_factory(connection_id, owner_id=owner_id)
    db = factory()
    try:
        yield db
    finally:
        db.close()


def check_connection(connection_id: str | None = None, owner_id: str | None = None) -> tuple[bool, str | None]:
    cid = connection_id or "default"
    try:
        engine = get_engine(cid, owner_id=owner_id)
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


def connections_health(owner_id: str | None = None) -> dict[str, dict[str, str | bool]]:
    health: dict[str, dict[str, str | bool]] = {}
    for cid in available_connections(owner_id=owner_id).keys():
        ok, error = check_connection(cid, owner_id=owner_id)
        health[cid] = {"healthy": ok, "error": error or ""}
    return health
