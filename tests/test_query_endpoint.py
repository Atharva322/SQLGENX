from fastapi.testclient import TestClient

from src.api.main import app


def test_query_endpoint_returns_response_shape() -> None:
    client = TestClient(app)
    response = client.post('/v1/query', json={'question': 'What is total revenue by region?'})
    assert response.status_code == 200
    body = response.json()
    assert 'query_id' in body
    assert 'session_id' in body
    assert 'sql' in body
    assert 'results' in body
    assert 'confidence' in body
    assert 'signals' in body
    assert 'alignment_score' in body['signals']
    assert 'sanity_score' in body['signals']
    assert 'multi_query_agreement' in body['signals']
    assert 'warnings' in body
    assert 'accessed' in body
    assert 'execution_meta' in body


def test_query_endpoint_blocks_malicious_prompt_intent() -> None:
    client = TestClient(app)
    response = client.post('/v1/query', json={'question': 'Drop table employees'})
    assert response.status_code == 200
    body = response.json()
    warnings = " ".join(body.get("warnings", [])).lower()
    assert "blocked" in warnings
    assert body["execution_meta"]["rows_returned"] == 0
