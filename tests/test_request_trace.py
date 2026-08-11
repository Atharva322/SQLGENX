import pytest

from src.observability.request_trace import RequestTrace, redact_secrets


def test_timer_records_duration_on_success() -> None:
    trace = RequestTrace(request_id="qry_test")

    with trace.timer("prompt_format_ms"):
        pass

    assert "prompt_format_ms" in trace.stage_durations_ms
    assert trace.stage_durations_ms["prompt_format_ms"] >= 0
    assert trace.total_duration_ms() >= trace.stage_durations_ms["prompt_format_ms"]


def test_timer_records_duration_and_failure_stage_on_exception() -> None:
    trace = RequestTrace(request_id="qry_test")

    with pytest.raises(RuntimeError):
        with trace.timer("primary_sql_generation_ms"):
            raise RuntimeError("provider timeout")

    assert trace.stage_durations_ms["primary_sql_generation_ms"] >= 0
    assert trace.failure_stage == "primary_sql_generation_ms"


def test_nested_stage_policy_rejects_double_counting() -> None:
    trace = RequestTrace(request_id="qry_test")

    with pytest.raises(RuntimeError):
        with trace.timer("schema_introspection_ms"):
            with trace.timer("feedback_load_ms"):
                pass


def test_finalization_is_idempotent_and_adds_framework_overhead() -> None:
    trace = RequestTrace(request_id="qry_test")
    with trace.timer("schema_introspection_ms"):
        pass

    trace.finish()
    first = trace.snapshot()
    trace.finish()
    second = trace.snapshot()

    assert first.total_duration_ms == second.total_duration_ms
    assert "framework_overhead_ms" in first.stage_durations_ms
    assert first.trace_coverage_ratio >= 0.95


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
