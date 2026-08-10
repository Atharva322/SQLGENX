from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median, quantiles
from time import perf_counter
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "benchmarks" / "cases.jsonl"
RESULTS_DIR = ROOT / "benchmarks" / "results"


def _load_cases(limit: int | None = None) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with CASES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                cases.append(json.loads(line))
            if limit and len(cases) >= limit:
                break
    return cases


def _percentile(values: list[float], percentile: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return round(values[0], 3)
    if percentile == 50:
        return round(median(values), 3)
    buckets = quantiles(values, n=100, method="inclusive")
    return round(buckets[percentile - 1], 3)


def _fake_request(case: dict[str, Any], rag_enabled: bool, multi_query: bool) -> dict[str, Any]:
    started_at = perf_counter()
    question = case["question"].lower()
    llm_calls = 0 if case["expected_safety"] == "block" else 2
    if multi_query and case["bucket"] in {"aggregation", "join"}:
        llm_calls += 1
    db_round_trips = 0 if case["expected_safety"] in {"block", "unanswerable"} else 3
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
        "db_round_trips": db_round_trips,
    }


def run(profile: str, repetitions: int, limit: int | None) -> dict[str, Any]:
    if profile != "deterministic":
        raise ValueError("Only the deterministic profile is available in Phase 0.")
    cases = _load_cases(limit=limit)
    samples: list[dict[str, Any]] = []
    for rag_enabled in (False, True):
        for multi_query in (False, True):
            for concurrency in (1, 5):
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

    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        by_bucket[sample["bucket"]].append(sample)

    bucket_summary = {}
    for bucket, rows in by_bucket.items():
        latencies = [row["latency_ms"] for row in rows]
        bucket_summary[bucket] = {
            "samples": len(rows),
            "p50_ms": _percentile(latencies, 50),
            "p95_ms": _percentile(latencies, 95),
            "p99_ms": _percentile(latencies, 99),
            "error_rate": round(sum(1 for row in rows if row["error"]) / max(1, len(rows)), 4),
            "tokens": sum(row["tokens"] for row in rows),
            "llm_calls": sum(row["llm_calls"] for row in rows),
            "db_round_trips": sum(row["db_round_trips"] for row in rows),
        }

    report = {
        "profile": profile,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "case_count": len(cases),
        "sample_count": len(samples),
        "matrix": {
            "rag_enabled": [False, True],
            "multi_query_validation": [False, True],
            "concurrency": [1, 5],
            "process_state": ["cold", "warm"],
            "repetitions": repetitions,
        },
        "bucket_summary": bucket_summary,
        "samples": samples,
    }
    return report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="deterministic")
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    report = run(profile=args.profile, repetitions=args.repetitions, limit=args.limit)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    output = args.output or RESULTS_DIR / f"{args.profile}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({"output": str(output), "sample_count": report["sample_count"]}))


if __name__ == "__main__":
    main()
