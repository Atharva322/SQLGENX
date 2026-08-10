import pytest

from src.observability.request_trace import RequestTrace, redact_secrets


def test_timer_records_duration_on_success() -> None:
    trace = RequestTrace(request_id="qry_test")

    with trace.timer("prompt_build_ms"):
        pass

    assert "prompt_build_ms" in trace.stage_durations_ms
    assert trace.stage_durations_ms["prompt_build_ms"] >= 0
    assert trace.total_duration_ms() >= trace.stage_durations_ms["prompt_build_ms"]


def test_timer_records_duration_and_failure_stage_on_exception() -> None:
    trace = RequestTrace(request_id="qry_test")

    with pytest.raises(RuntimeError):
        with trace.timer("llm_generation_ms"):
            raise RuntimeError("provider timeout")

    assert trace.stage_durations_ms["llm_generation_ms"] >= 0
    assert trace.failure_stage == "llm_generation_ms"


def test_counters_cannot_become_negative() -> None:
    trace = RequestTrace(request_id="qry_test")

    trace.increment_llm_calls(-10)
    trace.increment_db_round_trips(-10)

    assert trace.llm_call_count == 0
    assert trace.db_round_trip_count == 0


def test_redaction_removes_secret_values() -> None:
    payload = {
        "database_url": "postgresql://user:pass@localhost/db",
        "nested": {"OPENAI_API_KEY": "sk-secret", "safe": "visible"},
    }

    redacted = redact_secrets(payload)

    assert redacted["database_url"] == "[REDACTED]"
    assert redacted["nested"]["OPENAI_API_KEY"] == "[REDACTED]"
    assert redacted["nested"]["safe"] == "visible"
