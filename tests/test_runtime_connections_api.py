from __future__ import annotations

from fastapi.testclient import TestClient

from src.api.main import app
from src.connections.models import ConnectionTestResponse
from src.connections.service import get_connection_service
from src.db.engine import resolve_database_url


def _runtime_payload(connection_id: str = "unit_pg") -> dict:
    return {
        "id": connection_id,
        "display_name": "Unit Postgres",
        "adapter_key": "postgresql",
        "config": {
            "host": "localhost",
            "port": 5432,
            "database": "sample_company",
            "username": "text2sql_user",
            "password": "super-secret-password",
            "tls_mode": "disable",
        },
    }


def test_runtime_connection_crud_redacts_secrets_and_scopes_owner(monkeypatch) -> None:
    service = get_connection_service()
    monkeypatch.setattr(
        service,
        "test",
        lambda request: ConnectionTestResponse(ok=True, schema_fingerprint="abc123"),
    )
    client = TestClient(app)
    connection_id = "unit_pg_redacted"

    created = client.post(
        "/v1/connections",
        headers={"X-Owner-Id": "owner-a"},
        json=_runtime_payload(connection_id),
    )
    assert created.status_code == 200
    body = created.json()
    assert body["id"] == connection_id
    assert body["verification_state"] == "verified"
    assert body["version"] == 1
    assert "super-secret-password" not in str(body)
    assert "postgresql://" not in str(body).lower()

    listed = client.get("/v1/connections", headers={"X-Owner-Id": "owner-a"})
    assert listed.status_code == 200
    assert connection_id in {item["id"] for item in listed.json()["connections"]}
    assert "super-secret-password" not in str(listed.json())

    hidden = client.get(f"/v1/connections/{connection_id}", headers={"X-Owner-Id": "owner-b"})
    assert hidden.status_code == 404
    assert hidden.json()["detail"]["error"] == "connection_not_found"

    resolved_url = resolve_database_url(connection_id, owner_id="owner-a")
    assert "super-secret-password" in resolved_url

    deleted = client.delete(f"/v1/connections/{connection_id}", headers={"X-Owner-Id": "owner-a"})
    assert deleted.status_code == 200


def test_runtime_connection_failed_create_is_not_persisted(monkeypatch) -> None:
    service = get_connection_service()
    monkeypatch.setattr(
        service,
        "test",
        lambda request: ConnectionTestResponse(ok=False, safe_error_code="authentication_failed"),
    )
    client = TestClient(app)
    connection_id = "unit_pg_failed"

    response = client.post(
        "/v1/connections",
        headers={"X-Owner-Id": "owner-fail"},
        json=_runtime_payload(connection_id),
    )
    assert response.status_code == 400
    assert response.json()["detail"]["safe_error_code"] == "authentication_failed"

    hidden = client.get(f"/v1/connections/{connection_id}", headers={"X-Owner-Id": "owner-fail"})
    assert hidden.status_code == 404
    assert "super-secret-password" not in str(response.json())
