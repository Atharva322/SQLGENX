from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from src.adapters.base import AdapterCapability, AdapterInfo
from src.adapters.registry import AdapterRegistry, AdapterRegistryError
from src.api.main import app
from src.connections.models import ConnectionNotFoundError, public_connection_from_url
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
                driver_name="mysql2",
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
    assert [item["key"] for item in default_response.json()["adapters"]] == ["postgresql"]

    dev_response = client.get("/v1/adapters?include_experimental=true")
    assert dev_response.status_code == 200
    assert [item["key"] for item in dev_response.json()["adapters"]] == ["postgresql"]


def test_adapters_endpoint_requires_server_side_experimental_gate(monkeypatch) -> None:
    from src.config.settings import get_settings

    monkeypatch.setenv("CONNECTION_ADAPTER_EXPERIMENTAL_CATALOG_ENABLED", "true")
    get_settings.cache_clear()
    client = TestClient(app)

    dev_response = client.get("/v1/adapters?include_experimental=true")
    get_settings.cache_clear()

    assert dev_response.status_code == 200
    assert {item["key"] for item in dev_response.json()["adapters"]} == {"postgresql", "mysql"}
