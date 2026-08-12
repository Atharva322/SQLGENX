from fastapi.testclient import TestClient

from src.api.main import app


def test_connections_endpoint_returns_default() -> None:
    client = TestClient(app)
    response = client.get("/v1/connections")
    assert response.status_code == 200
    body = response.json()
    assert "connections" in body
    ids = {connection["id"] for connection in body["connections"]}
    assert "default" in ids
    assert "postgresql://" not in str(body).lower()
    assert "text2sql_pass" not in str(body)
