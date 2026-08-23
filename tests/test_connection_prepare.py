from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.main import app
from src.config.settings import get_settings
from src.connections.prepare import get_connection_prepare_service
from src.connections.service import DEMO_OWNER_ID


def _schema() -> dict:
    return {
        "tables": [
            {
                "table": "departments",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "VARCHAR", "nullable": False},
                ],
            }
        ],
        "schema_fingerprint": "schema123",
    }


def test_prepare_status_defaults_to_not_started_without_secrets() -> None:
    get_connection_prepare_service().invalidate(DEMO_OWNER_ID, "default")
    client = TestClient(app)

    response = client.get("/v1/connections/default/prepare")

    assert response.status_code == 200
    body = response.json()
    assert body["ready"] is False
    assert body["status"] == "not_started"
    assert body["connection_id"] == "default"
    assert "postgresql://" not in str(body).lower()
    assert "text2sql_pass" not in str(body)


def test_prepare_default_connection_warms_linking_and_returns_ready(monkeypatch) -> None:
    calls: list[str] = []
    get_connection_prepare_service().invalidate(DEMO_OWNER_ID, "default")
    monkeypatch.setattr("src.connections.prepare.get_schema_summary", lambda **kwargs: _schema())
    monkeypatch.setattr("src.connections.prepare.select_relevant_feedback_examples", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        "src.connections.prepare.run_schema_linking",
        lambda **kwargs: calls.append(str(kwargs["question"])),
    )
    client = TestClient(app)

    response = client.post("/v1/connections/default/prepare")

    assert response.status_code == 200
    body = response.json()
    assert body["ready"] is True
    assert body["status"] == "ready"
    assert body["schema_fingerprint"] == "schema123"
    assert body["table_count"] == 1
    assert calls
    assert "postgresql://" not in str(body).lower()


def test_prepare_unknown_connection_returns_not_found() -> None:
    client = TestClient(app)

    response = client.post("/v1/connections/missing/prepare")

    assert response.status_code == 404
    assert response.json()["detail"]["error"] == "connection_not_found"


def test_query_returns_schema_not_ready_when_strict_guard_enabled(monkeypatch) -> None:
    get_connection_prepare_service().invalidate(DEMO_OWNER_ID, "default")
    monkeypatch.setenv("REQUIRE_PREPARED_SCHEMA_CONTEXT", "true")
    get_settings.cache_clear()
    client = TestClient(app)

    response = client.post("/v1/query", json={"question": "List departments", "connection_id": "default"})

    get_settings.cache_clear()
    assert response.status_code == 409
    body = response.json()["detail"]
    assert body["error"] == "schema_not_ready"
    assert body["connection_id"] == "default"
    assert "postgresql://" not in str(body).lower()
