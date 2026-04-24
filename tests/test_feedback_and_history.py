from fastapi.testclient import TestClient

from src.api.main import app


def test_history_filters_by_session() -> None:
    client = TestClient(app)
    payload_a = client.post(
        "/v1/query", json={"question": "Show all employees", "session_id": "s1"}
    ).json()
    client.post("/v1/query", json={"question": "Show all sales", "session_id": "s2"})

    history_s1 = client.get("/v1/history", params={"session_id": "s1"})
    assert history_s1.status_code == 200
    items = history_s1.json()["items"]
    assert any(item["query_id"] == payload_a["query_id"] for item in items)
    assert all(item["session_id"] == "s1" for item in items)


def test_feedback_endpoint_stores_verdict() -> None:
    client = TestClient(app)
    generated = client.post(
        "/v1/query", json={"question": "What is total sales by region?", "session_id": "s_fb"}
    )
    assert generated.status_code == 200
    body = generated.json()

    feedback = client.post(
        "/v1/feedback",
        json={
            "query_id": body["query_id"],
            "session_id": body["session_id"],
            "verdict": "incorrect",
            "notes": "Expected a grouped result.",
        },
    )
    assert feedback.status_code == 200
    fb_body = feedback.json()
    assert fb_body["stored"] is True
    assert fb_body["query_id"] == body["query_id"]
