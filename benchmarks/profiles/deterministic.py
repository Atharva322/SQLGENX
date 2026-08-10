from __future__ import annotations

from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from benchmarks.reporters import summarize_samples
from benchmarks.workload import load_cases


def _fake_request(case: dict[str, Any], rag_enabled: bool, multi_query: bool) -> dict[str, Any]:
    started_at = perf_counter()
    behavior = case.get("expected_behavior")
    question = case["question"].lower()
    llm_calls = 0 if behavior == "block" else 2
    if multi_query and case["bucket"] in {"aggregation", "join"}:
        llm_calls += 1
    db_round_trips = 0 if behavior in {"block", "unanswerable"} else 3
    token_estimate = 120 + len(question.split()) * 8
    if rag_enabled:
        token_estimate += 80
    if multi_query:
        token_estimate += 60
    latency_ms = (perf_counter() - started_at) * 1000
    return {
        "case_id": case["id"],
        "bucket": case["bucket"],
        "latency_ms": round(latency_ms, 3),
        "error": False,
        "tokens": token_estimate,
        "llm_calls": llm_calls,
        "provider_attempts": llm_calls,
        "sql_statements": db_round_trips,
        "unsafe_queries_executed": 0,
    }


def run(repetitions: int, limit: int | None, concurrency: int = 1) -> dict[str, Any]:
    cases = load_cases(limit=limit)
    samples: list[dict[str, Any]] = []
    for rag_enabled in (False, True):
        for multi_query in (False, True):
            for process_state in ("cold", "warm"):
                for _ in range(repetitions):
                    for case in cases:
                        sample = _fake_request(case, rag_enabled, multi_query)
                        sample.update(
                            {
                                "rag_enabled": rag_enabled,
                                "multi_query_validation": multi_query,
                                "concurrency": concurrency,
                                "process_state": process_state,
                            }
                        )
                        samples.append(sample)
    summary = summarize_samples(samples)
    return {
        "profile": "deterministic-report-smoke",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "case_count": len(cases),
        "matrix": {
            "rag_enabled": [False, True],
            "multi_query_validation": [False, True],
            "concurrency": [concurrency],
            "process_state": ["cold", "warm"],
            "repetitions": repetitions,
        },
        **summary,
        "samples": samples,
        "warning": "Synthetic report-format smoke only; do not use as application latency.",
    }
