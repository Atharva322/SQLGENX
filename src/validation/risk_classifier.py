from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any, Literal

from src.guardrails.rules import detect_malicious_prompt_intent
from src.models.schemas import ConstraintValidationResult, LinkingContext
from src.validation.multi_query import compute_complexity_score


ValidationLevel = Literal["fast", "standard", "strict"]

RISK_CLASSIFIER_VERSION = "risk-adaptive-v1"


@dataclass(frozen=True)
class RiskThresholds:
    fast_max_score: float = 0.34
    standard_max_score: float = 0.67
    low_link_confidence: float = 0.55
    low_model_confidence: float = 0.55
    low_retrieval_margin: float = 0.08
    high_scan_rows: int = 100_000


@dataclass(frozen=True)
class RiskSignals:
    question: str
    sql: str
    join_count: int = 0
    subquery_depth: int = 0
    set_operation_count: int = 0
    aggregation_count: int = 0
    has_grouping: bool = False
    unknown_identifier_count: int = 0
    retrieval_score: float | None = None
    retrieval_margin: float | None = None
    model_confidence: float | None = None
    ambiguous: bool = False
    estimated_scan_rows: int = 0
    verified_example_similarity: float | None = None
    high_risk_metric: bool = False
    malicious_intent: bool = False
    destructive_sql: bool = False
    constraint_passed: bool = True
    timeout_or_failure: bool = False


@dataclass(frozen=True)
class RiskClassification:
    level: ValidationLevel
    score: float
    classifier_version: str
    reason_codes: list[str] = field(default_factory=list)
    signals: dict[str, Any] = field(default_factory=dict)


def sql_subquery_depth(sql: str) -> int:
    text = sql.lower()
    depth = 0
    max_depth = 0
    for token in re.findall(r"\(|\)|select\b", text):
        if token == "(":
            depth += 1
        elif token == ")":
            depth = max(0, depth - 1)
        elif depth > 0:
            max_depth = max(max_depth, depth)
    return max_depth


def extract_risk_signals(
    *,
    question: str,
    sql: str,
    linking: LinkingContext,
    constraint: ConstraintValidationResult,
    model_confidence: float | None,
    estimated_scan_rows: int = 0,
    timeout_or_failure: bool = False,
) -> RiskSignals:
    sql_text = f" {sql.lower()} "
    retrieval_meta = linking.retrieval_meta or {}
    schema_score = retrieval_meta.get("schema_avg_score")
    schema_margin = retrieval_meta.get("schema_margin")
    example_similarity = retrieval_meta.get("example_avg_score")
    metric_terms = ("ratio", "rate", "retention", "margin", "percent", "percentage", "conversion")
    return RiskSignals(
        question=question,
        sql=sql,
        join_count=len(re.findall(r"\bjoin\b", sql_text)),
        subquery_depth=sql_subquery_depth(sql),
        set_operation_count=len(re.findall(r"\b(union|intersect|except)\b", sql_text)),
        aggregation_count=len(re.findall(r"\b(count|sum|avg|min|max)\s*\(", sql_text)),
        has_grouping=" group by " in sql_text or " having " in sql_text,
        unknown_identifier_count=len(linking.unresolved_identifiers)
        + len(constraint.blocked_identifiers),
        retrieval_score=_optional_float(schema_score),
        retrieval_margin=_optional_float(schema_margin),
        model_confidence=_optional_float(model_confidence),
        ambiguous=bool(linking.ambiguous),
        estimated_scan_rows=max(0, int(estimated_scan_rows or 0)),
        verified_example_similarity=_optional_float(example_similarity),
        high_risk_metric=any(term in question.lower() for term in metric_terms),
        malicious_intent=bool(detect_malicious_prompt_intent(question)),
        destructive_sql=bool(re.search(r"\b(insert|update|delete|drop|alter|truncate|create)\b", sql_text)),
        constraint_passed=constraint.passed,
        timeout_or_failure=timeout_or_failure,
    )


class RiskClassifier:
    version = RISK_CLASSIFIER_VERSION

    def __init__(self, thresholds: RiskThresholds | None = None) -> None:
        self.thresholds = thresholds or RiskThresholds()

    def classify(self, signals: RiskSignals) -> RiskClassification:
        score = 0.0
        reasons: list[str] = []

        def add(points: float, code: str) -> None:
            nonlocal score
            score += points
            reasons.append(code)

        if signals.malicious_intent or signals.destructive_sql:
            add(1.0, "unsafe_intent_or_sql")
        if not signals.constraint_passed:
            add(0.75, "constraint_identifier_violation")
        elif signals.unknown_identifier_count > 0:
            add(0.35, "unknown_identifiers")
        if signals.timeout_or_failure:
            add(0.5, "validation_failure_escalation")
        if signals.ambiguous:
            add(0.28, "ambiguous_request")
        if signals.join_count >= 2:
            add(0.22, "multi_join")
        elif signals.join_count == 1:
            add(0.1, "single_join")
        if signals.subquery_depth > 0:
            add(min(0.4, 0.25 * signals.subquery_depth), "nested_query")
        if signals.set_operation_count:
            add(0.35, "set_operation")
        if signals.aggregation_count >= 2 or (signals.aggregation_count and signals.has_grouping):
            add(0.25, "aggregation_complexity")
        elif signals.aggregation_count:
            add(0.06, "single_aggregation")
        if (
            signals.model_confidence is not None
            and signals.model_confidence < self.thresholds.low_model_confidence
        ):
            add(0.2, "low_model_confidence")
        if (
            signals.retrieval_margin is not None
            and signals.retrieval_margin < self.thresholds.low_retrieval_margin
        ):
            add(0.16, "low_retrieval_margin")
        if (
            signals.retrieval_score is not None
            and signals.retrieval_score < self.thresholds.low_link_confidence
        ):
            add(0.12, "low_retrieval_score")
        if signals.estimated_scan_rows >= self.thresholds.high_scan_rows:
            add(0.18, "large_estimated_scan")
        if signals.high_risk_metric:
            add(0.12, "high_risk_metric")
        if signals.verified_example_similarity is not None and signals.verified_example_similarity >= 0.82:
            score -= 0.08
            reasons.append("verified_example_match")

        normalized = round(max(0.0, min(1.0, score)), 3)
        if (
            signals.malicious_intent
            or signals.destructive_sql
            or not signals.constraint_passed
            or (signals.subquery_depth > 0 and signals.set_operation_count > 0)
        ):
            level: ValidationLevel = "strict"
        elif normalized <= self.thresholds.fast_max_score:
            level = "fast"
        elif normalized <= self.thresholds.standard_max_score:
            level = "standard"
        else:
            level = "strict"
        if signals.unknown_identifier_count > 0 and level == "fast":
            level = "standard"

        if not reasons:
            reasons.append("low_risk_simple_select")
        return RiskClassification(
            level=level,
            score=normalized,
            classifier_version=self.version,
            reason_codes=reasons,
            signals={
                "join_count": signals.join_count,
                "subquery_depth": signals.subquery_depth,
                "set_operation_count": signals.set_operation_count,
                "aggregation_count": signals.aggregation_count,
                "unknown_identifier_count": signals.unknown_identifier_count,
                "ambiguous": signals.ambiguous,
                "estimated_scan_rows": signals.estimated_scan_rows,
                "model_confidence": signals.model_confidence,
                "retrieval_score": signals.retrieval_score,
                "retrieval_margin": signals.retrieval_margin,
                "high_risk_metric": signals.high_risk_metric,
            },
        )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
