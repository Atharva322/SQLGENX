from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CASES_PATH = ROOT / "benchmarks" / "cases.jsonl"


def load_cases(limit: int | None = None) -> list[dict[str, Any]]:
    cases: list[dict[str, Any]] = []
    with CASES_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                cases.append(json.loads(line))
            if limit and len(cases) >= limit:
                break
    return cases


def representative_subset(cases: list[dict[str, Any]], per_bucket: int = 1) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    counts: dict[str, int] = {}
    for case in cases:
        bucket = str(case.get("bucket", ""))
        if counts.get(bucket, 0) >= per_bucket:
            continue
        selected.append(case)
        counts[bucket] = counts.get(bucket, 0) + 1
    return selected
