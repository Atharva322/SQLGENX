from dataclasses import dataclass, field
import json
import re


@dataclass
class MultiQueryResult:
    score: float
    warnings: list[str] = field(default_factory=list)
    comparison_summary: str = ""


def should_run_multi_query_validation(question: str, sql: str, threshold: int = 2) -> bool:
    normalized_sql = sql.strip().lower()
    if normalized_sql == "unanswerable":
        return False

    question_text = question.lower()
    sql_text = sql.lower()
    text = f"{question_text} {sql_text}"

    question_markers = [
        "compare",
        "vs",
        "versus",
        "trend",
        "breakdown",
        "over time",
        "rank",
        "top",
        "share",
        "contribution",
        "percent",
        "percentage",
    ]
    sql_markers = [
        " join ",
        " group by ",
        " having ",
        " distinct ",
        " over (",
        " union ",
        " intersect ",
        " except ",
    ]

    question_hits = sum(1 for marker in question_markers if marker in question_text)
    sql_hits = sum(1 for marker in sql_markers if marker in f" {sql_text} ")
    nested_select = len(re.findall(r"\(\s*select\b", text, flags=re.IGNORECASE))
    complexity_score = question_hits + sql_hits + (2 if nested_select > 0 else 0)

    # Run only for clearly complex questions to reduce latency on simple cases.
    return complexity_score >= max(1, threshold)


def _normalize_rows(rows: list[dict]) -> list[str]:
    normalized: list[str] = []
    for row in rows:
        normalized.append(json.dumps(row, sort_keys=True, default=str))
    return sorted(normalized)


def evaluate_multi_query_agreement(
    primary_rows: list[dict],
    alternate_rows: list[dict],
) -> MultiQueryResult:
    if not primary_rows and not alternate_rows:
        return MultiQueryResult(
            score=0.5,
            warnings=["Both primary and alternative queries returned no rows."],
            comparison_summary="No comparable rows.",
        )

    left = _normalize_rows(primary_rows)
    right = _normalize_rows(alternate_rows)

    if left == right:
        return MultiQueryResult(
            score=1.0,
            warnings=[],
            comparison_summary="Primary and alternative query results match exactly.",
        )

    intersection = len(set(left).intersection(set(right)))
    union = len(set(left).union(set(right)))
    overlap = intersection / max(1, union)
    score = round(max(0.0, min(1.0, overlap)), 3)
    warnings = [
        "Primary and alternative SQL results diverged; manual review recommended for hallucination risk."
    ]
    return MultiQueryResult(
        score=score,
        warnings=warnings,
        comparison_summary=f"Result overlap ratio: {overlap:.2f}",
    )
