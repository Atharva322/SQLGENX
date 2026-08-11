from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from benchmarks.environment import configure_deterministic_provider
from benchmarks.reporters import classify_behavior, report_metadata, summarize_samples
from benchmarks.workload import load_cases


def _sample_from_response(case: dict[str, Any], latency_ms: float, response: Any) -> dict[str, Any]:
    meta = response.execution_meta
    unsafe_executed = 1 if case.get("safety_expectation") == "block" and meta.query_execution_count > 0 else 0
    observed_behavior = classify_behavior(case, response)
    return {
        "case_id": case["id"],
        "bucket": case["bucket"],
        "expected_behavior": case.get("expected_behavior"),
        "observed_behavior": observed_behavior,
        "behavior_match": observed_behavior == case.get("expected_behavior"),
        "latency_ms": round(latency_ms, 3),
        "error": False,
        "http_status": None,
        "tokens": int(meta.llm_token_usage.get("total_tokens", 0) or 0),
        "llm_calls": meta.llm_call_count,
        "provider_attempts": meta.provider_attempt_count,
        "sql_statements": meta.sql_statement_count,
        "query_execution_count": meta.query_execution_count,
        "unsafe_queries_executed": unsafe_executed,
        "trace_coverage_ratio": meta.trace_coverage_ratio,
        "stage_durations_ms": meta.stage_durations_ms,
    }


def run(repetitions: int, limit: int | None, concurrency: int = 1, cold: bool = False) -> dict[str, Any]:
    configure_deterministic_provider()
    from src.services.query_service import QueryService

    cases = load_cases(limit=limit)
    samples: list[dict[str, Any]] = []
    service = QueryService()
    if not cold and cases:
        service.process_question(cases[0]["question"], connection_id="default")
    for _ in range(repetitions):
        for case in cases:
            if cold:
                service = QueryService()
            started_at = perf_counter()
            try:
                response = service.process_question(case["question"], connection_id="default")
                sample = _sample_from_response(case, (perf_counter() - started_at) * 1000, response)
            except Exception as exc:
                sample = {
                    "case_id": case["id"],
                    "bucket": case["bucket"],
                    "latency_ms": round((perf_counter() - started_at) * 1000, 3),
                    "error": True,
                    "error_type": type(exc).__name__,
                    "unsafe_queries_executed": 0,
                }
            sample["concurrency"] = concurrency
            sample["process_state"] = "cold" if cold else "warm"
            samples.append(sample)
    return {
        "profile": "integration-service",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "case_count": len(cases),
        "matrix": {
            "concurrency": [concurrency],
            "process_state": ["cold" if cold else "warm"],
            "repetitions": repetitions,
        },
        **report_metadata(),
        **summarize_samples(samples),
        "samples": samples,
    }
