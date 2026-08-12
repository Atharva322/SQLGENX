from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field
from sqlalchemy.engine.url import URL, make_url

ConnectionVerificationState = Literal["legacy_env", "untested", "verified", "failed"]
ConnectionHealthState = Literal["unknown", "healthy", "unhealthy"]
ConnectionErrorCode = Literal[
    "invalid_config",
    "destination_blocked",
    "authentication_failed",
    "tls_failed",
    "unreachable",
    "unsupported_version",
    "introspection_failed",
]


class ConnectionNotFoundError(LookupError):
    pass


class PublicConnection(BaseModel):
    id: str
    display_name: str
    adapter_key: str
    dialect: str
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    tls_mode: str | None = None
    verification_state: ConnectionVerificationState = "legacy_env"
    health_state: ConnectionHealthState = "unknown"
    last_tested_at: str | None = None
    schema_fingerprint: str | None = None
    safe_error_code: ConnectionErrorCode | None = None
    created_at: str
    updated_at: str


class AdapterCatalogItem(BaseModel):
    key: str
    display_name: str
    release_state: Literal["hidden", "experimental", "verified"]
    sqlglot_dialect: str
    driver_name: str
    default_port: int | None = None
    supported_server_versions: list[str] = Field(default_factory=list)
    capabilities: dict[str, Any] = Field(default_factory=dict)


def public_connection_from_url(connection_id: str, database_url: str) -> PublicConnection:
    parsed = make_url(database_url)
    now = datetime.now(timezone.utc).isoformat()
    return PublicConnection(
        id=connection_id,
        display_name="Default" if connection_id == "default" else connection_id,
        adapter_key=_adapter_key(parsed),
        dialect=_dialect(parsed),
        host=parsed.host,
        port=parsed.port,
        database=parsed.database,
        username=parsed.username,
        tls_mode=_tls_mode(parsed),
        created_at=now,
        updated_at=now,
    )


def _adapter_key(url: URL) -> str:
    driver = url.drivername.split("+", 1)[0]
    if driver in {"postgresql", "postgres"}:
        return "postgresql"
    if driver in {"mysql", "mariadb"}:
        return "mysql"
    return driver


def _dialect(url: URL) -> str:
    if _adapter_key(url) == "postgresql":
        return "postgres"
    if _adapter_key(url) == "mysql":
        return "mysql"
    return url.drivername.split("+", 1)[0]


def _tls_mode(url: URL) -> str | None:
    query = dict(url.query)
    return query.get("sslmode") or query.get("ssl")
