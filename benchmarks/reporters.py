from __future__ import annotations

from collections import defaultdict
import hashlib
import os
import platform
import subprocess
from pathlib import Path
from statistics import median, quantiles
from typing import Any

ROOT = Path(__file__).resolve().parents[1]


def percentile(values: list[float], pct: int) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return round(values[0], 3)
    if pct == 50:
        return round(median(values), 3)
    buckets = quantiles(values, n=100, method="inclusive")
    return round(buckets[pct - 1], 3)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=ROOT,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        return "unknown"


def report_metadata() -> dict[str, Any]:
    manifest_path = ROOT / "benchmarks" / "db" / "manifest.json"
    cases_path = ROOT / "benchmarks" / "cases.jsonl"
    return {
        "commit": git_commit(),
        "dataset": {
            "path": "benchmarks/cases.jsonl",
            "sha256": file_sha256(cases_path),
        },
        "database_seed": {
            "manifest_path": "benchmarks/db/manifest.json",
            "manifest_sha256": file_sha256(manifest_path),
            "schema_fingerprint": "phase0-benchmark-v1",
        },
        "environment": {
            "python": platform.python_version(),
            "platform": platform.platform(),
            "llm_provider": os.environ.get("LLM_PROVIDER", "deterministic"),
            "llm_model": os.environ.get("LLM_MODEL", "sqlgenx-fixture-v1"),
        },
        "sanitization": {
            "raw_prompts_included": False,
            "raw_sql_included": False,
            "raw_rows_included": False,
            "credentials_or_database_urls_included": False,
        },
    }


def classify_behavior(case: dict[str, Any], response: Any | None, *, http_body: dict[str, Any] | None = None) -> str:
    if response is not None:
        meta = response.execution_meta
        sql = response.sql.strip().upper()
        if case.get("safety_expectation") == "block" and meta.query_execution_count == 0:
            return "block"
        if sql == "UNANSWERABLE" or meta.failure_classification == "unanswerable":
            return "unanswerable"
        if meta.query_execution_count > 0:
            return "execute"
        return "unanswerable"
    if http_body is not None:
        meta = http_body.get("execution_meta", {})
        query_execution_count = int(meta.get("query_execution_count", 0) or 0)
        failure_classification = meta.get("failure_classification")
        sql = str(http_body.get("sql", "")).strip().upper()
        if case.get("safety_expectation") == "block" and query_execution_count == 0:
            return "block"
        if sql == "UNANSWERABLE" or failure_classification == "unanswerable":
            return "unanswerable"
        if query_execution_count > 0:
            return "execute"
    return "error"


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    by_bucket: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_validation_level: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_proposed_validation_level: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for sample in samples:
        by_bucket[str(sample.get("bucket", "unknown"))].append(sample)
        if sample.get("validation_level"):
            by_validation_level[str(sample.get("validation_level"))].append(sample)
        if sample.get("proposed_validation_level"):
            by_proposed_validation_level[str(sample.get("proposed_validation_level"))].append(sample)

    bucket_summary: dict[str, Any] = {}
    for bucket, rows in by_bucket.items():
        latencies = [float(row.get("latency_ms", 0.0)) for row in rows]
        errors = [row for row in rows if row.get("error")]
        bucket_summary[bucket] = {
            "samples": len(rows),
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
            "error_rate": round(len(errors) / max(1, len(rows)), 4),
            "llm_calls": sum(int(row.get("llm_calls", 0) or 0) for row in rows),
            "provider_attempts": sum(int(row.get("provider_attempts", 0) or 0) for row in rows),
            "tokens": sum(int(row.get("tokens", 0) or 0) for row in rows),
            "sql_statements": sum(int(row.get("sql_statements", 0) or 0) for row in rows),
            "schema_introspections": sum(
                int(row.get("schema_introspection_count", 0) or 0) for row in rows
            ),
            "unsafe_queries_executed": sum(
                int(row.get("unsafe_queries_executed", 0) or 0) for row in rows
            ),
        }
    validation_summary: dict[str, Any] = {}
    for level, rows in by_validation_level.items():
        latencies = [float(row.get("latency_ms", 0.0)) for row in rows]
        validation_summary[level] = {
            "samples": len(rows),
            "share": round(len(rows) / max(1, len(samples)), 3),
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
        }
    proposed_validation_summary: dict[str, Any] = {}
    for level, rows in by_proposed_validation_level.items():
        latencies = [float(row.get("latency_ms", 0.0)) for row in rows]
        proposed_validation_summary[level] = {
            "samples": len(rows),
            "share": round(len(rows) / max(1, len(samples)), 3),
            "p50_ms": percentile(latencies, 50),
            "p95_ms": percentile(latencies, 95),
            "p99_ms": percentile(latencies, 99),
        }
    latencies = [float(row.get("latency_ms", 0.0)) for row in samples]
    wall_time_ms = sum(latencies)
    trace_values = [
        float(row.get("trace_coverage_ratio", 0.0) or 0.0)
        for row in samples
        if "trace_coverage_ratio" in row
    ]
    behavior_scored = [row for row in samples if "behavior_match" in row]
    behavior_hits = [row for row in behavior_scored if row.get("behavior_match")]
    behavior_success_rate = round(
        len(behavior_hits) / max(1, len(behavior_scored)), 3
    )
    return {
        "sample_count": len(samples),
        "p50_ms": percentile(latencies, 50),
        "p95_ms": percentile(latencies, 95),
        "p99_ms": percentile(latencies, 99),
        "error_rate": round(
            sum(1 for row in samples if row.get("error")) / max(1, len(samples)), 4
        ),
        "requests_per_second": round((len(samples) / wall_time_ms) * 1000, 3)
        if wall_time_ms > 0
        else 0.0,
        "unsafe_queries_executed": sum(
            int(row.get("unsafe_queries_executed", 0) or 0) for row in samples
        ),
        "schema_introspection_count": sum(
            int(row.get("schema_introspection_count", 0) or 0) for row in samples
        ),
        "minimum_trace_coverage": round(min(trace_values), 3) if trace_values else 0.0,
        "behavior_success_rate": behavior_success_rate,
        "success_rate": behavior_success_rate,
        "bucket_summary": bucket_summary,
        "validation_level_summary": validation_summary,
        "proposed_validation_level_summary": proposed_validation_summary,
    }
