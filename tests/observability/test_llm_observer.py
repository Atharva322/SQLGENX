from types import SimpleNamespace

from src.llm.client import LLMClient
from src.observability.request_trace import RequestTrace


def test_deterministic_provider_records_attempt_success_and_tokens(monkeypatch) -> None:
    settings = SimpleNamespace(
        llm_provider="deterministic",
        llm_model="sqlgenx-fixture-v1",
        openai_api_key="",
        anthropic_api_key="",
    )
    monkeypatch.setattr("src.llm.client.get_settings", lambda: settings)
    client = LLMClient()
    trace = RequestTrace(request_id="qry_provider")
    client.observer = trace

    generated = client.generate_structured_sql("Show total revenue by region", "prompt")

    assert "SUM(amount)" in generated.sql
    assert trace.provider_attempt_count == 1
    assert trace.provider_success_count == 1
    assert trace.llm_token_usage["total_tokens"] == 40
    assert "sqlgenx-fixture-v1" in str(trace.snapshot().llm_operations)


def test_placeholder_response_sends_zero_provider_requests(monkeypatch) -> None:
    settings = SimpleNamespace(
        llm_provider="openai",
        llm_model="gpt-test",
        openai_api_key="",
        anthropic_api_key="",
    )
    monkeypatch.setattr("src.llm.client.get_settings", lambda: settings)
    client = LLMClient()
    trace = RequestTrace(request_id="qry_placeholder")
    client.observer = trace

    generated = client.generate_structured_sql("Show anything", "prompt")

    assert generated.sql
    assert trace.provider_attempt_count == 0
    assert trace.llm_call_count == 0
