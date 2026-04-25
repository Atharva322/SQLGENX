from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.config.settings import get_settings


_settings = get_settings()
_engine_cache: dict[str, Engine] = {}
_sessionmaker_cache: dict[str, sessionmaker] = {}


def available_connections() -> dict[str, str]:
    urls = {"default": _settings.database_url}
    urls.update(_settings.connection_urls())
    return urls


def resolve_database_url(connection_id: str | None) -> str:
    connections = available_connections()
    if connection_id and connection_id in connections:
        return connections[connection_id]
    return connections["default"]


def get_engine(connection_id: str | None = None) -> Engine:
    cid = connection_id or "default"
    if cid in _engine_cache:
        return _engine_cache[cid]
    engine = create_engine(resolve_database_url(cid), future=True, pool_pre_ping=True)
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
