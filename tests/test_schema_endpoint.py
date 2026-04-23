from fastapi.testclient import TestClient

from src.api.main import app


def test_schema_endpoint_returns_tables_key() -> None:
    client = TestClient(app)
    response = client.get('/v1/schema')
    assert response.status_code == 200
    body = response.json()
    assert 'tables' in body
    assert isinstance(body['tables'], list)
