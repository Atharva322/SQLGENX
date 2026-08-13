from __future__ import annotations

from pathlib import Path
import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import SQLAlchemyError

from src.api import main
from src.api.main import app
from src.config.settings import get_settings
from src.services.query_service import QueryService


MYSQL_URL = os.getenv(
    "MYSQL_TEST_URL",
    "mysql+pymysql://text2sql_user:text2sql_pass@localhost:3306/sample_company?charset=utf8mb4",
)


def _payload(connection_id: str) -> dict:
    url = make_url(MYSQL_URL)
    return {
        "id": connection_id,
        "display_name": "Runtime MySQL",
        "adapter_key": "mysql",
        "config": {
            "host": url.host or "localhost",
            "port": url.port or 3306,
            "database": url.database or "sample_company",
            "username": url.username or "text2sql_user",
            "password": url.password or "text2sql_pass",
            "tls_mode": "disable",
            "charset": url.query.get("charset", "utf8mb4"),
        },
    }


def _ensure_mysql_schema() -> bool:
    engine = create_engine(MYSQL_URL, future=True)
    try:
        with engine.begin() as conn:
            for statement in Path("benchmarks/db/init_mysql.sql").read_text(encoding="utf-8").split(";"):
                if statement.strip():
                    conn.execute(text(statement))
        return True
    except SQLAlchemyError:
        return False
    finally:
        engine.dispose()


@pytest.mark.integration
def test_runtime_mysql_connection_lifecycle_and_query(monkeypatch) -> None:
    if not _ensure_mysql_schema():
        pytest.skip("MySQL unavailable.")

    monkeypatch.setenv("LLM_PROVIDER", "deterministic")
    monkeypatch.setenv("LLM_MODEL", "sqlgenx-fixture-v1")
    get_settings.cache_clear()
    monkeypatch.setattr(main, "service", QueryService())

    client = TestClient(app)
    connection_id = f"runtime_mysql_{uuid4().hex[:8]}"
    owner = "mysql-owner"
    headers = {"X-Owner-Id": owner}

    tested = client.post("/v1/connections/test", json=_payload(connection_id))
    assert tested.status_code == 200
    assert tested.json()["ok"] is True
    assert tested.json()["schema_fingerprint"]

    created = client.post("/v1/connections", headers=headers, json=_payload(connection_id))
    assert created.status_code == 200
    assert created.json()["adapter_key"] == "mysql"
    assert created.json()["dialect"] == "mysql"
    assert created.json()["verification_state"] == "verified"

    schema = client.get(f"/v1/connections/{connection_id}/schema", headers=headers)
    assert schema.status_code == 200
    assert "departments" in {table["table"] for table in schema.json()["tables"]}

    blocked = client.get(f"/v1/connections/{connection_id}", headers={"X-Owner-Id": "other-owner"})
    assert blocked.status_code == 404

    query = client.post(
        "/v1/query",
        headers=headers,
        json={"question": "List departments", "connection_id": connection_id},
    )
    assert query.status_code == 200
    query_body = query.json()
    assert query_body["connection_id"] == connection_id
    assert query_body["sql"].startswith("SELECT")
    assert "departments" in query_body["sql"].lower()
    assert query_body["execution_meta"]["query_execution_count"] >= 1
    assert query_body["results"]
    assert "error" not in str(query_body["results"]).lower()

    updated = client.patch(
        f"/v1/connections/{connection_id}",
        headers=headers,
        json={"display_name": "Runtime MySQL Updated"},
    )
    assert updated.status_code == 200
    assert updated.json()["version"] == 2

    deleted = client.delete(f"/v1/connections/{connection_id}", headers=headers)
    assert deleted.status_code == 200

    missing = client.get(f"/v1/connections/{connection_id}", headers=headers)
    assert missing.status_code == 404
