from fastapi.testclient import TestClient

from src.api.main import app


def test_query_endpoint_returns_response_shape() -> None:
    client = TestClient(app)
    response = client.post('/v1/query', json={'question': 'What is total revenue by region?'})
    assert response.status_code == 200
    body = response.json()
    assert 'sql' in body
    assert 'results' in body
    assert 'confidence' in body
    assert 'signals' in body
    assert 'warnings' in body
    assert 'execution_meta' in body
