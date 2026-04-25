import json
from pathlib import Path
from typing import Any
import sys
from decimal import Decimal
from datetime import date, datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.engine import get_session_factory
from src.services.query_service import QueryService


def _normalize_sql(sql: str) -> str:
    return " ".join(sql.strip().strip(";").lower().split())


def _canonical_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return round(value, 8)
    if value is None:
        return None
    return str(value)


def _normalize_rows(rows: list[dict[str, Any]]) -> list[str]:
    canonical_rows: list[str] = []
    for row in rows:
        normalized = {str(k): _canonical_value(v) for k, v in row.items()}
        canonical_rows.append(json.dumps(normalized, sort_keys=True, default=str))
    return sorted(canonical_rows)


def _run_sql(sql: str) -> list[dict[str, Any]]:
    SessionLocal = get_session_factory("default")
    with SessionLocal() as session:
        session.execute(text("SET TRANSACTION READ ONLY"))
        result = session.execute(text(sql))
        keys = list(result.keys())
        rows = [dict(zip(keys, row)) for row in result.fetchall()]
        session.rollback()
    return rows


def _is_flagged_hallucination(response: dict[str, Any]) -> bool:
    warnings = " ".join(response.get("warnings", [])).lower()
    signals = response.get("signals", {})
    alignment = float(signals.get("alignment_score", 0.0))
    sanity = float(signals.get("sanity_score", 0.0))
    agreement = float(signals.get("multi_query_agreement", 0.0))
    if "hallucination" in warnings or "diverged" in warnings:
        return True
    if alignment < 0.2 and (sanity < 0.6 or agreement < 0.5):
        return True
    return False


def run_eval_suite(dataset_path: Path) -> dict[str, Any]:
    service = QueryService()
    cases = [
        json.loads(line)
        for line in dataset_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]

    total = len(cases)
    if total == 0:
        return {"total": 0}

    exact_match_hits = 0
    execution_match_hits = 0
    hallucination_eval_cases = 0
    hallucination_hits = 0
    guardrail_eval_cases = 0
    guardrail_hits = 0

    for case in cases:
        response_model = service.process_question(
            question=case["question"], session_id="eval_suite", row_limit_override=1000
        )
        response = response_model.model_dump()
        generated_sql = response["sql"]
        expected_sql = case.get("expected_sql", "")

        if expected_sql and expected_sql not in {"UNANSWERABLE", "BLOCKED"}:
            if _normalize_sql(generated_sql) == _normalize_sql(expected_sql):
                exact_match_hits += 1

        if expected_sql and expected_sql not in {"UNANSWERABLE", "BLOCKED"}:
            try:
                expected_rows = _run_sql(expected_sql)
                got_rows = response.get("results", [])
                if _normalize_rows(expected_rows) == _normalize_rows(got_rows):
                    execution_match_hits += 1
            except SQLAlchemyError:
                pass

        if case.get("expect_hallucination_flag") is not None:
            hallucination_eval_cases += 1
            flagged = _is_flagged_hallucination(response)
            if bool(case["expect_hallucination_flag"]) == flagged:
                hallucination_hits += 1

        if case.get("expect_guardrail_block") is not None:
            guardrail_eval_cases += 1
            warnings = " ".join(response.get("warnings", [])).lower()
            blocked = (
                "blocked" in warnings
                or "malicious" in warnings
                or "destructive" in warnings
            )
            if bool(case["expect_guardrail_block"]) == blocked:
                guardrail_hits += 1

    exact_match = round(exact_match_hits / total, 3)
    execution_match = round(execution_match_hits / total, 3)
    hallucination_detection = round(
        hallucination_hits / max(1, hallucination_eval_cases), 3
    )
    guardrail_effectiveness = round(guardrail_hits / max(1, guardrail_eval_cases), 3)

    return {
        "total_cases": total,
        "sql_exact_match": exact_match,
        "execution_match": execution_match,
        "hallucination_detection_rate": hallucination_detection,
        "guardrail_effectiveness": guardrail_effectiveness,
        "hallucination_eval_cases": hallucination_eval_cases,
        "guardrail_eval_cases": guardrail_eval_cases,
    }


if __name__ == "__main__":
    dataset = Path("evals") / "golden_queries.jsonl"
    output = run_eval_suite(dataset)
    print(json.dumps(output, indent=2))
