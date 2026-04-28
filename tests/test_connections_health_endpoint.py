from fastapi.testclient import TestClient

from src.api.main import app


def test_connections_health_endpoint(monkeypatch) -> None:
    client = TestClient(app)
    monkeypatch.setattr(
        "src.api.main.connections_health",
        lambda: {
            "default": {"healthy": True, "error": ""},
            "docker_internal": {"healthy": False, "error": "connection refused"},
        },
    )
    response = client.get("/v1/connections/health")
    assert response.status_code == 200
    body = response.json()
    assert "connections" in body
    assert body["connections"]["default"]["healthy"] is True
    assert body["connections"]["docker_internal"]["healthy"] is False
