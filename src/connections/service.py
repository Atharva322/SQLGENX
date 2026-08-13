from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy import create_engine

from src.adapters.mysql import mysql_adapter
from src.adapters.postgresql import postgresql_adapter
from src.adapters.sqlserver import sqlserver_adapter
from src.config.settings import get_settings
from src.connections.models import (
    ConnectionCreateRequest,
    ConnectionNotFoundError,
    ConnectionTestRequest,
    ConnectionTestResponse,
    ConnectionUpdateRequest,
    PublicConnection,
    StoredConnection,
    public_connection_from_stored,
)
from src.connections.network_policy import NetworkPolicy
from src.connections.repository import (
    LegacyEnvConnectionRepository,
    runtime_connection_repository,
)
from src.connections.secrets import get_secret_store
from src.db.schema_introspector import clear_schema_summary_cache
from src.db.engine import dispose_connection_engine


DEMO_OWNER_ID = "demo-user"


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class ConnectionService:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.repository = runtime_connection_repository()
        self.legacy = LegacyEnvConnectionRepository()
        self.secrets = get_secret_store()
        self.network_policy = NetworkPolicy()

    def test(self, request: ConnectionTestRequest) -> ConnectionTestResponse:
        adapter = self._adapter(request.adapter_key)
        decision = self.network_policy.validate_destination(request.config.host, request.config.port)
        if not decision.allowed:
            return ConnectionTestResponse(ok=False, safe_error_code=decision.safe_error_code)  # type: ignore[arg-type]
        adapter.validate_config(request.config)
        url = adapter.build_url(request.config)
        engine = create_engine(url, **adapter.engine_options(
            self.settings.db_connect_timeout_seconds,
            self.settings.db_pool_timeout_seconds,
            self.settings.db_pool_recycle_seconds,
        ))
        try:
            ok, error = adapter.test_connection(engine)
            if not ok:
                return ConnectionTestResponse(ok=False, safe_error_code=error)
            schema = adapter.inspect_schema(engine)
            return ConnectionTestResponse(ok=True, schema_fingerprint=schema.get("schema_fingerprint"))
        finally:
            engine.dispose()

    def create(self, owner_id: str, request: ConnectionCreateRequest) -> PublicConnection:
        adapter = self._adapter(request.adapter_key)
        test_result = self.test(ConnectionTestRequest(adapter_key=request.adapter_key, config=request.config))
        if not test_result.ok:
            return self._failed_public(owner_id, request, test_result)
        timestamp = now_utc()
        secret_id = f"conn_{owner_id}_{request.id}_{uuid4().hex[:12]}"
        self.secrets.put(secret_id, request.config.password)
        record = StoredConnection(
            id=request.id,
            owner_id=owner_id,
            display_name=request.display_name,
            adapter_key=request.adapter_key,
            dialect=adapter.dialect,
            host=request.config.host,
            port=request.config.port,
            database=request.config.database,
            username=request.config.username,
            tls_mode=request.config.tls_mode,
            secret_id=secret_id,
            version=1,
            verification_state="verified",
            health_state="healthy",
            last_tested_at=timestamp,
            schema_fingerprint=test_result.schema_fingerprint,
            created_at=timestamp,
            updated_at=timestamp,
        )
        self.repository.upsert(record)
        clear_schema_summary_cache()
        return public_connection_from_stored(record)

    def update(self, owner_id: str, connection_id: str, request: ConnectionUpdateRequest) -> PublicConnection:
        existing = self.repository.get(owner_id, connection_id)
        timestamp = now_utc()
        config = request.config
        secret_id = existing.secret_id
        schema_fingerprint = existing.schema_fingerprint
        verification_state = existing.verification_state
        health_state = existing.health_state
        safe_error_code = existing.safe_error_code
        last_tested_at = existing.last_tested_at
        if config is not None:
            test_result = self.test(ConnectionTestRequest(adapter_key=existing.adapter_key, config=config))
            if not test_result.ok:
                updated = existing.model_copy(
                    update={
                        "display_name": request.display_name or existing.display_name,
                        "verification_state": "failed",
                        "health_state": "unhealthy",
                        "safe_error_code": test_result.safe_error_code,
                        "last_tested_at": timestamp,
                        "updated_at": timestamp,
                    }
                )
                self.repository.upsert(updated)
                return public_connection_from_stored(updated)
            self.secrets.delete(existing.secret_id)
            secret_id = f"conn_{owner_id}_{connection_id}_{uuid4().hex[:12]}"
            self.secrets.put(secret_id, config.password)
            schema_fingerprint = test_result.schema_fingerprint
            verification_state = "verified"
            health_state = "healthy"
            safe_error_code = None
            last_tested_at = timestamp
        updated = existing.model_copy(
            update={
                "display_name": request.display_name or existing.display_name,
                "host": config.host if config else existing.host,
                "port": config.port if config else existing.port,
                "database": config.database if config else existing.database,
                "username": config.username if config else existing.username,
                "tls_mode": config.tls_mode if config else existing.tls_mode,
                "secret_id": secret_id,
                "version": existing.version + 1,
                "verification_state": verification_state,
                "health_state": health_state,
                "last_tested_at": last_tested_at,
                "schema_fingerprint": schema_fingerprint,
                "safe_error_code": safe_error_code,
                "updated_at": timestamp,
            }
        )
        self.repository.upsert(updated)
        dispose_connection_engine(connection_id, owner_id=owner_id)
        clear_schema_summary_cache()
        return public_connection_from_stored(updated)

    def delete(self, owner_id: str, connection_id: str) -> PublicConnection:
        record = self.repository.delete(owner_id, connection_id)
        self.secrets.delete(record.secret_id)
        dispose_connection_engine(connection_id, owner_id=owner_id)
        clear_schema_summary_cache()
        return public_connection_from_stored(record)

    def list_public(self, owner_id: str) -> list[PublicConnection]:
        return [*self.legacy.list_public(), *self.repository.public_list(owner_id)]

    def get_public(self, owner_id: str, connection_id: str) -> PublicConnection:
        legacy = self.legacy.public_by_id(connection_id)
        if legacy is not None:
            return legacy
        return public_connection_from_stored(self.repository.get(owner_id, connection_id))

    def get_runtime_record(self, owner_id: str, connection_id: str) -> StoredConnection:
        return self.repository.get(owner_id, connection_id)

    def resolve_runtime_url(self, owner_id: str, connection_id: str) -> tuple[str, int] | None:
        try:
            record = self.repository.get(owner_id, connection_id)
        except ConnectionNotFoundError:
            return None
        config = record_to_config(record, self.secrets.get(record.secret_id))
        return self._adapter(record.adapter_key).build_url(config), record.version

    def _adapter(self, adapter_key: str):
        if adapter_key == "postgresql":
            return postgresql_adapter
        if adapter_key == "mysql":
            return mysql_adapter
        if adapter_key == "sqlserver":
            return sqlserver_adapter
        raise ValueError(f"Unsupported adapter: {adapter_key}")

    def _failed_public(self, owner_id: str, request: ConnectionCreateRequest, result: ConnectionTestResponse) -> PublicConnection:
        timestamp = now_utc()
        return PublicConnection(
            id=request.id,
            display_name=request.display_name,
            adapter_key=request.adapter_key,
            dialect=self._adapter(request.adapter_key).dialect,
            host=request.config.host,
            port=request.config.port,
            database=request.config.database,
            username=request.config.username,
            tls_mode=request.config.tls_mode,
            verification_state="failed",
            health_state="unhealthy",
            last_tested_at=timestamp,
            safe_error_code=result.safe_error_code,
            created_at=timestamp,
            updated_at=timestamp,
        )


def record_to_config(record: StoredConnection, password: str):
    from src.connections.models import MySQLConnectionConfig, PostgresConnectionConfig, SQLServerConnectionConfig

    payload = {
        "host": record.host,
        "port": record.port,
        "database": record.database,
        "username": record.username,
        "password": password,
        "tls_mode": record.tls_mode or "prefer",
    }
    if record.adapter_key == "mysql":
        return MySQLConnectionConfig(**payload)
    if record.adapter_key == "sqlserver":
        return SQLServerConnectionConfig(**payload)
    return PostgresConnectionConfig(**payload)


_CONNECTION_SERVICE = ConnectionService()


def get_connection_service() -> ConnectionService:
    return _CONNECTION_SERVICE
