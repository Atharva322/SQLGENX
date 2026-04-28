import json
from pathlib import Path
from typing import Any
import sys
from decimal import Decimal
from datetime import date, datetime
import math
import re
import argparse

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db.engine import get_session_factory
from src.db.schema_introspector import get_schema_summary
from src.services.rag_retriever import rank_context_candidates
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


def _load_feedback_examples() -> list[dict[str, Any]]:
    path = Path("data") / "feedback_fewshots.jsonl"
    if not path.exists():
        return []
    examples: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if payload.get("verdict") != "correct":
            continue
        question = str(payload.get("question", "")).strip()
        sql = str(payload.get("sql", "")).strip()
        if not question or not sql:
            continue
        examples.append(payload)
    return examples


def _extract_tables_from_sql(sql: str) -> set[str]:
    matches = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][\w\.]*)", sql, flags=re.IGNORECASE)
    tables: set[str] = set()
    for raw in matches:
        tables.add(raw.split(".")[-1].lower())
    return tables


def _recall_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    if not relevant:
        return 0.0
    top_k = ranked[:k]
    hits = len({item.lower() for item in top_k}.intersection({item.lower() for item in relevant}))
    return hits / max(1, len(relevant))


def _dcg_at_k(binary_rels: list[int], k: int) -> float:
    score = 0.0
    for i, rel in enumerate(binary_rels[:k]):
        score += (2**rel - 1) / math.log2(i + 2)
    return score


def _ndcg_at_k(ranked: list[str], relevant: set[str], k: int) -> float:
    if not relevant:
        return 0.0
    binary = [1 if item.lower() in {r.lower() for r in relevant} else 0 for item in ranked[:k]]
    dcg = _dcg_at_k(binary, k)
    ideal_ones = [1] * min(k, len(relevant))
    idcg = _dcg_at_k(ideal_ones, k)
    if idcg == 0:
        return 0.0
    return dcg / idcg


def run_eval_suite(
    dataset_path: Path,
    limit: int | None = None,
    retrieval_only: bool = False,
) -> dict[str, Any]:
    service = QueryService()
    all_cases = [
        json.loads(line)
        for line in dataset_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    cases = all_cases[:limit] if limit and limit > 0 else all_cases

    total = len(cases)
    if total == 0:
        return {"total": 0}

    exact_match_hits = 0
    execution_match_hits = 0
    hallucination_eval_cases = 0
    hallucination_hits = 0
    guardrail_eval_cases = 0
    guardrail_hits = 0
    retrieval_eval_cases = 0
    schema_recall_sum = 0.0
    schema_ndcg_sum = 0.0

    schema = get_schema_summary(connection_id="default")
    schema_tables = schema.get("tables", [])
    feedback_examples = _load_feedback_examples()
    rag_k = max(1, int(service.settings.rag_top_k_schema))
    retrieval_context_available = bool(schema_tables)

    for case in cases:
        expected_sql = case.get("expected_sql", "")
        if retrieval_context_available and expected_sql and expected_sql not in {"UNANSWERABLE", "BLOCKED"}:
            relevant_tables = _extract_tables_from_sql(expected_sql)
            if relevant_tables:
                ranking = rank_context_candidates(
                    question=case["question"],
                    schema={"tables": schema_tables},
                    feedback_examples=feedback_examples,
                )
                ranked_table_names = [
                    str(schema_tables[idx].get("table", "")).lower()
                    for idx in ranking.schema_ranked_indices
                    if 0 <= idx < len(schema_tables)
                ]
                schema_recall_sum += _recall_at_k(ranked_table_names, relevant_tables, rag_k)
                schema_ndcg_sum += _ndcg_at_k(ranked_table_names, relevant_tables, rag_k)
                retrieval_eval_cases += 1

        if retrieval_only:
            continue

        response_model = service.process_question(
            question=case["question"], session_id="eval_suite", row_limit_override=1000
        )
        response = response_model.model_dump()
        generated_sql = response["sql"]

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

    if retrieval_only:
        exact_match = 0.0
        execution_match = 0.0
        hallucination_detection = 0.0
        guardrail_effectiveness = 0.0
    else:
        exact_match = round(exact_match_hits / total, 3)
        execution_match = round(execution_match_hits / total, 3)
        hallucination_detection = round(
            hallucination_hits / max(1, hallucination_eval_cases), 3
        )
        guardrail_effectiveness = round(guardrail_hits / max(1, guardrail_eval_cases), 3)
    schema_recall_at_k = round(schema_recall_sum / max(1, retrieval_eval_cases), 3)
    schema_ndcg_at_k = round(schema_ndcg_sum / max(1, retrieval_eval_cases), 3)

    return {
        "total_cases": total,
        "sql_exact_match": exact_match,
        "execution_match": execution_match,
        "hallucination_detection_rate": hallucination_detection,
        "guardrail_effectiveness": guardrail_effectiveness,
        "hallucination_eval_cases": hallucination_eval_cases,
        "guardrail_eval_cases": guardrail_eval_cases,
        f"schema_recall_at_{rag_k}": schema_recall_at_k,
        f"schema_ndcg_at_{rag_k}": schema_ndcg_at_k,
        "retrieval_eval_cases": retrieval_eval_cases,
        "retrieval_context_available": retrieval_context_available,
        "retrieval_only_mode": retrieval_only,
        "limited_cases": limit or total,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run text2sql evaluation suite.")
    parser.add_argument(
        "--dataset",
        default=str(Path("evals") / "golden_queries.jsonl"),
        help="Path to jsonl eval dataset.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional max number of cases to run.",
    )
    parser.add_argument(
        "--retrieval-only",
        action="store_true",
        help="Run only retrieval ranking metrics (Recall@K/nDCG@K).",
    )
    args = parser.parse_args()
    output = run_eval_suite(
        dataset_path=Path(args.dataset),
        limit=args.limit,
        retrieval_only=args.retrieval_only,
    )
    print(json.dumps(output, indent=2))
