from fastapi.testclient import TestClient

from src.api.main import app


def test_connections_endpoint_returns_default() -> None:
    client = TestClient(app)
    response = client.get("/v1/connections")
    assert response.status_code == 200
    body = response.json()
    assert "connections" in body
    assert "default" in body["connections"]
