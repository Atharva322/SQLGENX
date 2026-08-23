from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator
from sqlalchemy.engine.url import URL, make_url

ConnectionVerificationState = Literal["legacy_env", "untested", "verified", "failed"]
ConnectionHealthState = Literal["unknown", "healthy", "unhealthy"]
ConnectionPrepareStatus = Literal["not_started", "preparing", "ready", "failed", "stale"]
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


class ConnectionAccessError(PermissionError):
    pass


class PostgresConnectionConfig(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=5432, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)
    tls_mode: Literal["disable", "prefer", "require", "verify-ca", "verify-full"] = "prefer"


class MySQLConnectionConfig(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=3306, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)
    tls_mode: Literal["disable", "prefer", "require", "verify-ca", "verify-full"] = "prefer"
    charset: str = Field(default="utf8mb4", min_length=1, max_length=64)


class SQLServerConnectionConfig(BaseModel):
    host: str = Field(min_length=1, max_length=255)
    port: int = Field(default=1433, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=128)
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)
    tls_mode: Literal["disable", "require"] = "require"
    odbc_driver: str = Field(default="ODBC Driver 18 for SQL Server", min_length=1, max_length=128)
    trust_server_certificate: bool = False


class SQLiteConnectionConfig(BaseModel):
    host: str = Field(default="localhost", min_length=1, max_length=255)
    port: int = Field(default=1, ge=1, le=65535)
    database: str = Field(min_length=1, max_length=512)
    username: str = Field(default="sqlite", min_length=1, max_length=128)
    password: str = Field(default="sqlite-local", min_length=1, max_length=1024)
    tls_mode: Literal["disable"] = "disable"
    read_only: bool = True


class SnowflakeConnectionConfig(BaseModel):
    account_identifier: str = Field(min_length=1, max_length=255, pattern=r"^[A-Za-z0-9_.-]+$")
    warehouse: str = Field(min_length=1, max_length=128)
    role: str | None = Field(default=None, min_length=1, max_length=128)
    database: str = Field(min_length=1, max_length=128)
    schema_name: str = Field(default="PUBLIC", min_length=1, max_length=128, alias="schema")
    username: str = Field(min_length=1, max_length=128)
    password: str = Field(min_length=1, max_length=1024)
    authenticator: Literal["snowflake"] = "snowflake"
    query_timeout_seconds: int = Field(default=30, ge=1, le=600)
    tls_mode: Literal["require"] = "require"

    @property
    def host(self) -> str:
        return f"{self.account_identifier}.snowflakecomputing.com"

    @property
    def port(self) -> int:
        return 443


ConnectionConfig = (
    PostgresConnectionConfig
    | MySQLConnectionConfig
    | SQLServerConnectionConfig
    | SQLiteConnectionConfig
    | SnowflakeConnectionConfig
)


class ConnectionTestRequest(BaseModel):
    adapter_key: Literal["postgresql", "mysql", "sqlserver", "sqlite", "snowflake"]
    config: ConnectionConfig

    @model_validator(mode="before")
    @classmethod
    def _coerce_config_for_adapter(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        adapter_key = data.get("adapter_key")
        config = data.get("config")
        if not isinstance(config, dict):
            return data
        if adapter_key == "postgresql":
            data["config"] = PostgresConnectionConfig(**config)
        elif adapter_key == "mysql":
            data["config"] = MySQLConnectionConfig(**config)
        elif adapter_key == "sqlserver":
            data["config"] = SQLServerConnectionConfig(**config)
        elif adapter_key == "sqlite":
            data["config"] = SQLiteConnectionConfig(**config)
        elif adapter_key == "snowflake":
            data["config"] = SnowflakeConnectionConfig(**config)
        return data


class ConnectionCreateRequest(ConnectionTestRequest):
    id: str = Field(min_length=3, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    display_name: str = Field(min_length=1, max_length=120)


class ConnectionUpdateRequest(BaseModel):
    display_name: str | None = Field(default=None, min_length=1, max_length=120)
    config: ConnectionConfig | None = None


class ConnectionTestResponse(BaseModel):
    ok: bool
    safe_error_code: ConnectionErrorCode | None = None
    schema_fingerprint: str | None = None


class ConnectionPrepareResponse(BaseModel):
    connection_id: str
    owner_id: str
    ready: bool
    status: ConnectionPrepareStatus
    schema_fingerprint: str | None = None
    table_count: int = 0
    prepared_at: str | None = None
    elapsed_ms: int | None = None
    safe_error_code: ConnectionErrorCode | None = None


class StoredConnection(BaseModel):
    id: str
    owner_id: str
    display_name: str
    adapter_key: str
    dialect: str
    host: str
    port: int
    database: str
    username: str
    tls_mode: str | None = None
    secret_id: str
    version: int = 1
    verification_state: ConnectionVerificationState = "verified"
    health_state: ConnectionHealthState = "unknown"
    last_tested_at: str | None = None
    schema_fingerprint: str | None = None
    safe_error_code: ConnectionErrorCode | None = None
    adapter_metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: str
    updated_at: str


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
    version: int = 1
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
        version=1,
        created_at=now,
        updated_at=now,
    )


def public_connection_from_stored(record: StoredConnection) -> PublicConnection:
    return PublicConnection(
        id=record.id,
        display_name=record.display_name,
        adapter_key=record.adapter_key,
        dialect=record.dialect,
        host=record.host,
        port=record.port,
        database=record.database,
        username=record.username,
        tls_mode=record.tls_mode,
        verification_state=record.verification_state,
        health_state=record.health_state,
        last_tested_at=record.last_tested_at,
        schema_fingerprint=record.schema_fingerprint,
        safe_error_code=record.safe_error_code,
        version=record.version,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


def _adapter_key(url: URL) -> str:
    driver = url.drivername.split("+", 1)[0]
    if driver in {"postgresql", "postgres"}:
        return "postgresql"
    if driver in {"mysql", "mariadb"}:
        return "mysql"
    if driver in {"mssql", "sqlserver"}:
        return "sqlserver"
    if driver == "sqlite":
        return "sqlite"
    if driver == "snowflake":
        return "snowflake"
    return driver


def _dialect(url: URL) -> str:
    if _adapter_key(url) == "postgresql":
        return "postgres"
    if _adapter_key(url) == "mysql":
        return "mysql"
    if _adapter_key(url) == "sqlserver":
        return "tsql"
    if _adapter_key(url) == "sqlite":
        return "sqlite"
    if _adapter_key(url) == "snowflake":
        return "snowflake"
    return url.drivername.split("+", 1)[0]


def _tls_mode(url: URL) -> str | None:
    query = dict(url.query)
    return query.get("sslmode") or query.get("ssl")
