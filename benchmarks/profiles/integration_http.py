from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from time import perf_counter
from typing import Any

from fastapi.testclient import TestClient

from benchmarks.environment import configure_deterministic_provider
from benchmarks.reporters import classify_behavior, report_metadata, summarize_samples
from benchmarks.workload import load_cases


def _request(client: TestClient, case: dict[str, Any]) -> dict[str, Any]:
    started_at = perf_counter()
    response = client.post("/v1/query", json={"question": case["question"], "connection_id": "default"})
    latency_ms = (perf_counter() - started_at) * 1000
    sample: dict[str, Any] = {
        "case_id": case["id"],
        "bucket": case["bucket"],
        "latency_ms": round(latency_ms, 3),
        "http_status": response.status_code,
        "error": response.status_code >= 400,
        "unsafe_queries_executed": 0,
    }
    if response.status_code == 200:
        body = response.json()
        meta = body.get("execution_meta", {})
        observed_behavior = classify_behavior(case, None, http_body=body)
        sample.update(
            {
                "expected_behavior": case.get("expected_behavior"),
                "observed_behavior": observed_behavior,
                "behavior_match": observed_behavior == case.get("expected_behavior"),
                "tokens": int((meta.get("llm_token_usage") or {}).get("total_tokens", 0) or 0),
                "llm_calls": int(meta.get("llm_call_count", 0) or 0),
                "provider_attempts": int(meta.get("provider_attempt_count", 0) or 0),
                "sql_statements": int(meta.get("sql_statement_count", 0) or 0),
                "schema_introspection_count": int(meta.get("schema_introspection_count", 0) or 0),
                "query_execution_count": int(meta.get("query_execution_count", 0) or 0),
                "trace_coverage_ratio": float(meta.get("trace_coverage_ratio", 0.0) or 0.0),
            }
        )
        if case.get("safety_expectation") == "block" and int(meta.get("query_execution_count", 0) or 0) > 0:
            sample["unsafe_queries_executed"] = 1
    return sample


def run(repetitions: int, limit: int | None, concurrency: int = 1) -> dict[str, Any]:
    configure_deterministic_provider()
    from src.api.main import app

    cases = load_cases(limit=limit)
    client = TestClient(app)
    samples: list[dict[str, Any]] = []
    if cases:
        client.post("/v1/query", json={"question": cases[0]["question"], "connection_id": "default"})
    tasks = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
        for _ in range(repetitions):
            for case in cases:
                tasks.append(pool.submit(_request, client, case))
        for future in as_completed(tasks):
            sample = future.result()
            sample["concurrency"] = concurrency
            sample["process_state"] = "warm"
            samples.append(sample)
    return {
        "profile": "integration-http-inprocess",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "case_count": len(cases),
        "matrix": {
            "concurrency": [concurrency],
            "process_state": ["warm"],
            "repetitions": repetitions,
        },
        **report_metadata(),
        **summarize_samples(samples),
        "samples": samples,
    }
