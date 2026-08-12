from fastapi.testclient import TestClient

from src.api.main import app
from src.runtime.async_runtime import AsyncRuntimeOverloaded, AsyncRuntimeTimeout


def test_query_endpoint_returns_response_shape() -> None:
    client = TestClient(app)
    response = client.post("/v1/query", json={"question": "What is total revenue by region?"})
    assert response.status_code == 200
    body = response.json()
    assert "query_id" in body
    assert "connection_id" in body
    assert "session_id" in body
    assert "sql" in body
    assert "results" in body
    assert "confidence" in body
    assert "signals" in body
    assert "alignment_score" in body["signals"]
    assert "sanity_score" in body["signals"]
    assert "multi_query_agreement" in body["signals"]
    assert "warnings" in body
    assert "accessed" in body
    assert "execution_meta" in body
    assert "linking_meta" in body
    assert "constraint_meta" in body


def test_query_endpoint_blocks_malicious_prompt_intent() -> None:
    client = TestClient(app)
    response = client.post("/v1/query", json={"question": "Drop table employees"})
    assert response.status_code == 200
    body = response.json()
    warnings = " ".join(body.get("warnings", [])).lower()
    assert "blocked" in warnings
    assert body["execution_meta"]["rows_returned"] == 0


def test_query_endpoint_returns_structured_overload(monkeypatch) -> None:
    async def overloaded(*args, **kwargs):
        raise AsyncRuntimeOverloaded(retry_after_seconds=3, queue_depth=2, capacity=4)

    monkeypatch.setattr("src.api.main.service.process_question_async", overloaded)
    client = TestClient(app)
    response = client.post("/v1/query", json={"question": "What is total revenue by region?"})

    assert response.status_code == 503
    assert response.headers["retry-after"] == "3"
    body = response.json()["detail"]
    assert body["error"] == "query_runtime_overloaded"
    assert body["queue_depth"] == 2
    assert "postgresql://" not in str(body).lower()


def test_query_endpoint_returns_structured_timeout(monkeypatch) -> None:
    async def timeout(*args, **kwargs):
        raise AsyncRuntimeTimeout(timeout_seconds=0.01, stage="total_request")

    monkeypatch.setattr("src.api.main.service.process_question_async", timeout)
    client = TestClient(app)
    response = client.post("/v1/query", json={"question": "What is total revenue by region?"})

    assert response.status_code == 504
    body = response.json()["detail"]
    assert body["error"] == "query_runtime_timeout"
    assert body["timeout_stage"] == "total_request"
    assert "postgresql://" not in str(body).lower()
