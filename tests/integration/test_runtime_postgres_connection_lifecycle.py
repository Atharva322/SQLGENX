from __future__ import annotations

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.engine.url import make_url

from src.api import main
from src.api.main import app
from src.config.settings import get_settings
from src.db.engine import check_connection
from src.services.query_service import QueryService


def _payload_from_database_url(connection_id: str) -> dict:
    url = make_url(get_settings().database_url)
    return {
        "id": connection_id,
        "display_name": "Runtime PostgreSQL",
        "adapter_key": "postgresql",
        "config": {
            "host": url.host or "localhost",
            "port": url.port or 5432,
            "database": url.database or "sample_company",
            "username": url.username or "text2sql_user",
            "password": url.password or "text2sql_pass",
            "tls_mode": dict(url.query).get("sslmode", "disable"),
        },
    }


@pytest.mark.integration
def test_runtime_postgres_connection_lifecycle_and_query(monkeypatch) -> None:
    ok, error = check_connection("default")
    if not ok:
        pytest.skip(f"PostgreSQL unavailable: {error}")

    monkeypatch.setenv("LLM_PROVIDER", "deterministic")
    monkeypatch.setenv("LLM_MODEL", "sqlgenx-fixture-v1")
    get_settings.cache_clear()
    monkeypatch.setattr(main, "service", QueryService())

    client = TestClient(app)
    connection_id = f"runtime_pg_{uuid4().hex[:8]}"
    owner = "integration-owner"
    headers = {"X-Owner-Id": owner}

    tested = client.post("/v1/connections/test", json=_payload_from_database_url(connection_id))
    assert tested.status_code == 200
    assert tested.json()["ok"] is True
    assert tested.json()["schema_fingerprint"]

    created = client.post("/v1/connections", headers=headers, json=_payload_from_database_url(connection_id))
    assert created.status_code == 200
    assert created.json()["verification_state"] == "verified"
    assert created.json()["schema_fingerprint"] == tested.json()["schema_fingerprint"]

    schema = client.get(f"/v1/connections/{connection_id}/schema", headers=headers)
    assert schema.status_code == 200
    assert {table["table"] for table in schema.json()["tables"]}

    blocked = client.get(f"/v1/connections/{connection_id}", headers={"X-Owner-Id": "other-owner"})
    assert blocked.status_code == 404

    query = client.post(
        "/v1/query",
        headers=headers,
        json={"question": "List departments", "connection_id": connection_id},
    )
    assert query.status_code == 200
    assert query.json()["connection_id"] == connection_id
    assert query.json()["execution_meta"]["schema_introspection_count"] >= 0

    updated = client.patch(
        f"/v1/connections/{connection_id}",
        headers=headers,
        json={"display_name": "Runtime PostgreSQL Updated"},
    )
    assert updated.status_code == 200
    assert updated.json()["version"] == 2

    deleted = client.delete(f"/v1/connections/{connection_id}", headers=headers)
    assert deleted.status_code == 200

    missing = client.get(f"/v1/connections/{connection_id}", headers=headers)
    assert missing.status_code == 404
