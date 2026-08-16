from __future__ import annotations

from pathlib import Path
import shutil
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text

from src.adapters.base import AdapterCapability, AdapterInfo
from src.adapters.registry import AdapterRegistry, AdapterRegistryError
from src.adapters.snowflake import snowflake_adapter
from src.adapters.sqlite import sqlite_adapter
from src.adapters.sqlserver import sqlserver_adapter
from src.api.main import app
from src.config.settings import get_settings
from src.connections.models import (
    ConnectionNotFoundError,
    ConnectionTestRequest,
    MySQLConnectionConfig,
    PostgresConnectionConfig,
    SQLiteConnectionConfig,
    SnowflakeConnectionConfig,
    SQLServerConnectionConfig,
    public_connection_from_url,
)
from src.db.engine import resolve_database_url
from src.db.engine import _safe_connection_error_code
from src.services.query_service import QueryService


def test_unknown_connection_ids_fail_closed() -> None:
    with pytest.raises(ConnectionNotFoundError):
        resolve_database_url("missing_connection")

    with pytest.raises(ConnectionNotFoundError):
        QueryService()._normalize_connection_id("missing_connection")


def test_schema_endpoint_unknown_connection_returns_not_found() -> None:
    client = TestClient(app)
    response = client.get("/v1/schema?connection_id=missing_connection")

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "connection_not_found"
    assert "postgresql://" not in str(response.json()).lower()


def test_query_endpoint_unknown_connection_returns_not_found() -> None:
    client = TestClient(app)
    response = client.post(
        "/v1/query",
        json={"question": "Show all employees", "connection_id": "missing_connection"},
    )

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "connection_not_found"
    assert "postgresql://" not in str(response.json()).lower()


def test_public_connection_redacts_url_credentials() -> None:
    public = public_connection_from_url(
        "analytics",
        "postgresql://report_user:super-secret@db.example.com:5432/warehouse?sslmode=require",
    )
    payload = public.model_dump()

    assert payload["id"] == "analytics"
    assert payload["adapter_key"] == "postgresql"
    assert payload["host"] == "db.example.com"
    assert payload["username"] == "report_user"
    assert payload["tls_mode"] == "require"
    assert "super-secret" not in str(payload)
    assert "postgresql://" not in str(payload).lower()


def test_connection_health_error_codes_are_sanitized() -> None:
    assert _safe_connection_error_code("password authentication failed for user secret_user") == "authentication_failed"
    assert _safe_connection_error_code("connection refused at db.internal:5432") == "unreachable"


def test_adapter_registry_filters_release_states() -> None:
    registry = AdapterRegistry(
        [
            AdapterInfo(
                key="postgresql",
                display_name="PostgreSQL",
                release_state="verified",
                sqlglot_dialect="postgres",
                driver_name="psycopg2",
                default_port=5432,
                capabilities=AdapterCapability(True, True, True, True, True),
            ),
            AdapterInfo(
                key="mysql",
                display_name="MySQL",
                release_state="experimental",
                sqlglot_dialect="mysql",
                driver_name="pymysql",
                default_port=3306,
            ),
        ]
    )

    assert [adapter.key for adapter in registry.list()] == ["postgresql"]
    assert [adapter.key for adapter in registry.list(include_experimental=True)] == ["mysql", "postgresql"]
    with pytest.raises(AdapterRegistryError, match="duplicate"):
        registry.register(
            AdapterInfo(
                key="postgresql",
                display_name="Duplicate",
                release_state="hidden",
                sqlglot_dialect="postgres",
                driver_name="psycopg2",
                default_port=5432,
            )
        )


def test_adapters_endpoint_exposes_verified_by_default() -> None:
    from src.config.settings import get_settings

    get_settings.cache_clear()
    client = TestClient(app)

    default_response = client.get("/v1/adapters")
    assert default_response.status_code == 200
    assert [item["key"] for item in default_response.json()["adapters"]] == ["mysql", "postgresql"]

    dev_response = client.get("/v1/adapters?include_experimental=true")
    assert dev_response.status_code == 200
    assert [item["key"] for item in dev_response.json()["adapters"]] == ["mysql", "postgresql"]


def test_adapters_endpoint_requires_server_side_experimental_gate(monkeypatch) -> None:
    from src.config.settings import get_settings

    monkeypatch.setenv("CONNECTION_ADAPTER_EXPERIMENTAL_CATALOG_ENABLED", "true")
    get_settings.cache_clear()
    client = TestClient(app)

    dev_response = client.get("/v1/adapters?include_experimental=true")
    get_settings.cache_clear()

    assert dev_response.status_code == 200
    assert {item["key"] for item in dev_response.json()["adapters"]} == {
        "postgresql",
        "mysql",
        "sqlserver",
        "sqlite",
        "snowflake",
    }


def test_sqlserver_adapter_builds_odbc_url_without_leaking_in_public_catalog() -> None:
    config = SQLServerConnectionConfig(
        host="sql.example.com",
        port=1433,
        database="warehouse",
        username="report_user",
        password="super-secret",
        tls_mode="require",
        odbc_driver="ODBC Driver 18 for SQL Server",
        trust_server_certificate=False,
    )

    url = sqlserver_adapter.build_url(config)
    options = sqlserver_adapter.engine_options(5, 10, 1800)

    assert url.startswith("mssql+pyodbc://report_user:super-secret@sql.example.com:1433/warehouse")
    assert "driver=ODBC+Driver+18+for+SQL+Server" in url
    assert "Encrypt=yes" in url
    assert "TrustServerCertificate=no" in url
    assert options["connect_args"]["timeout"] == 5


def test_connection_config_is_coerced_by_adapter_key_defaults() -> None:
    payload = {
        "config": {
            "host": "db.example.com",
            "database": "warehouse",
            "username": "report_user",
            "password": "super-secret",
        }
    }

    pg = ConnectionTestRequest(adapter_key="postgresql", **payload)
    mysql = ConnectionTestRequest(adapter_key="mysql", **payload)
    sqlserver = ConnectionTestRequest(adapter_key="sqlserver", **payload)

    assert isinstance(pg.config, PostgresConnectionConfig)
    assert pg.config.port == 5432
    assert isinstance(mysql.config, MySQLConnectionConfig)
    assert mysql.config.port == 3306
    assert isinstance(sqlserver.config, SQLServerConnectionConfig)
    assert sqlserver.config.port == 1433
    assert sqlserver.config.odbc_driver == "ODBC Driver 18 for SQL Server"

    sqlite = ConnectionTestRequest(
        adapter_key="sqlite",
        config={"database": "local.sqlite3"},
    )
    assert isinstance(sqlite.config, SQLiteConnectionConfig)
    assert sqlite.config.host == "localhost"
    assert sqlite.config.port == 1
    assert sqlite.config.read_only is True

    snowflake = ConnectionTestRequest(
        adapter_key="snowflake",
        config={
            "account_identifier": "acme-dev",
            "warehouse": "ANALYTICS_WH",
            "role": "REPORTING_ROLE",
            "database": "SAMPLE_COMPANY",
            "schema": "PUBLIC",
            "username": "report_user",
            "password": "super-secret",
        },
    )
    assert isinstance(snowflake.config, SnowflakeConnectionConfig)
    assert snowflake.config.host == "acme-dev.snowflakecomputing.com"
    assert snowflake.config.port == 443
    assert snowflake.config.query_timeout_seconds == 30


def test_snowflake_adapter_builds_dedicated_account_url_and_options() -> None:
    config = SnowflakeConnectionConfig(
        account_identifier="acme-dev",
        warehouse="ANALYTICS_WH",
        role="REPORTING_ROLE",
        database="SAMPLE_COMPANY",
        schema="PUBLIC",
        username="report_user",
        password="super-secret",
        query_timeout_seconds=45,
    )

    url = snowflake_adapter.build_url(config)
    options = snowflake_adapter.engine_options(5, 10, 1800)

    assert url.startswith("snowflake://report_user:super-secret@acme-dev/SAMPLE_COMPANY/PUBLIC")
    assert "warehouse=ANALYTICS_WH" in url
    assert "role=REPORTING_ROLE" in url
    assert "authenticator=snowflake" in url
    assert "application=SQLGENX" in url
    assert options["connect_args"]["login_timeout"] == 5
    assert options["connect_args"]["network_timeout"] == 5


def test_snowflake_url_redacts_public_metadata_without_secret() -> None:
    public = public_connection_from_url(
        "warehouse",
        "snowflake://report_user:super-secret@acme-dev/SAMPLE_COMPANY/PUBLIC?warehouse=ANALYTICS_WH",
    )
    payload = public.model_dump()

    assert payload["adapter_key"] == "snowflake"
    assert payload["dialect"] == "snowflake"
    assert payload["host"] == "acme-dev"
    assert "super-secret" not in str(payload)
    assert "snowflake://" not in str(payload).lower()


def _workspace_sqlite_allowed_dir() -> Path:
    allowed = Path(".tmp") / "sqlite-tests" / uuid4().hex
    allowed.mkdir(parents=True)
    return allowed.resolve()


def test_sqlite_adapter_enforces_allowed_directory(monkeypatch) -> None:
    allowed = _workspace_sqlite_allowed_dir()
    monkeypatch.setenv("SQLITE_ALLOWED_DIRECTORY", str(allowed))
    get_settings.cache_clear()

    inside = SQLiteConnectionConfig(database="reports.sqlite3")
    outside = SQLiteConnectionConfig(database=str(allowed.parent / "outside.sqlite3"))
    bad_suffix = SQLiteConnectionConfig(database="reports.txt")

    try:
        url = sqlite_adapter.build_url(inside)

        assert url.startswith("sqlite:///")
        assert "reports.sqlite3" in url
        with pytest.raises(ValueError, match="outside SQLITE_ALLOWED_DIRECTORY"):
            sqlite_adapter.validate_config(outside)
        with pytest.raises(ValueError, match="must end"):
            sqlite_adapter.validate_config(bad_suffix)
    finally:
        get_settings.cache_clear()
        shutil.rmtree(allowed, ignore_errors=True)


def test_sqlite_adapter_inspects_explains_and_enforces_query_only(monkeypatch) -> None:
    allowed = _workspace_sqlite_allowed_dir()
    db_path = allowed / "reports.sqlite3"
    monkeypatch.setenv("SQLITE_ALLOWED_DIRECTORY", str(allowed))
    get_settings.cache_clear()
    config = SQLiteConnectionConfig(database=str(db_path))

    engine = create_engine(sqlite_adapter.build_url(config), **sqlite_adapter.engine_options(5, 10, 1800))
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT NOT NULL)"))
            conn.execute(text("INSERT INTO departments (name) VALUES ('Engineering')"))

        schema = sqlite_adapter.inspect_schema(engine)
        with engine.connect() as conn:
            sqlite_adapter.configure_read_only(conn)
            plan = sqlite_adapter.explain(conn, "SELECT name FROM departments")
            with pytest.raises(Exception):
                conn.execute(text("INSERT INTO departments (name) VALUES ('Sales')"))
    finally:
        engine.dispose()
        get_settings.cache_clear()
        shutil.rmtree(allowed, ignore_errors=True)

    assert "departments" in {table["table"] for table in schema["tables"]}
    assert plan
