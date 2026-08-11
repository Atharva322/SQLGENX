from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CASES = ROOT / "benchmarks" / "cases.jsonl"
DEFAULT_MANIFEST = ROOT / "benchmarks" / "db" / "manifest.json"
EXPECTED_DIR = ROOT / "benchmarks" / "expected_results"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_cases(path: Path) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def validate_cases(cases_path: Path = DEFAULT_CASES, manifest_path: Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    manifest = _load_json(manifest_path)
    cases = _load_cases(cases_path)
    tables: dict[str, list[str]] = manifest["tables"]
    required_buckets = set(manifest["required_buckets"])
    fingerprint = manifest["schema_fingerprint"]
    errors: list[str] = []
    seen_ids: set[str] = set()
    buckets: set[str] = set()

    for case in cases:
        case_id = str(case.get("id", ""))
        if not case_id:
            errors.append("case missing id")
        elif case_id in seen_ids:
            errors.append(f"duplicate case id: {case_id}")
        seen_ids.add(case_id)

        bucket = str(case.get("bucket", ""))
        buckets.add(bucket)
        behavior = case.get("expected_behavior")
        safety = case.get("safety_expectation")
        if behavior not in {"execute", "block", "unanswerable"}:
            errors.append(f"{case_id}: invalid expected_behavior {behavior}")
        if safety not in {"allow", "block"}:
            errors.append(f"{case_id}: invalid safety_expectation {safety}")
        if safety == "block" and behavior == "execute":
            errors.append(f"{case_id}: unsafe case cannot expect execution")

        for table in case.get("expected_tables", []):
            if table not in tables:
                errors.append(f"{case_id}: unknown expected table {table}")
        for column_ref in case.get("expected_columns", []):
            if "." not in column_ref:
                errors.append(f"{case_id}: invalid column reference {column_ref}")
                continue
            table, column = column_ref.split(".", 1)
            if table not in tables:
                errors.append(f"{case_id}: unknown column table {table}")
            elif column not in tables[table]:
                errors.append(f"{case_id}: unknown expected column {column_ref}")

        fixture_name = case.get("expected_result_fixture")
        if behavior == "execute" and not fixture_name:
            errors.append(f"{case_id}: execution case lacks expected_result_fixture")
        if behavior != "execute" and fixture_name:
            errors.append(f"{case_id}: non-execution case must not define expected_result_fixture")
        if fixture_name:
            fixture_path = EXPECTED_DIR / str(fixture_name)
            if not fixture_path.exists():
                errors.append(f"{case_id}: missing fixture {fixture_name}")
            else:
                payload = _load_json(fixture_path)
                if payload.get("schema_fingerprint") != fingerprint:
                    errors.append(f"{case_id}: fixture fingerprint mismatch {fixture_name}")

    missing_buckets = required_buckets - buckets
    for bucket in sorted(missing_buckets):
        errors.append(f"missing bucket: {bucket}")

    if errors:
        raise SystemExit("\n".join(errors))
    return {
        "case_count": len(cases),
        "bucket_count": len(buckets),
        "schema_fingerprint": fingerprint,
        "valid": True,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--cases", type=Path, default=DEFAULT_CASES)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--connection", default="")
    args = parser.parse_args()
    print(json.dumps(validate_cases(args.cases, args.manifest), indent=2))


if __name__ == "__main__":
    main()
