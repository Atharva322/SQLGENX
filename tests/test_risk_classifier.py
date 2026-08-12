from src.models.schemas import ConstraintValidationResult, LinkingContext, ResolvedIdentifierSet
from src.validation.risk_classifier import (
    RiskClassifier,
    extract_risk_signals,
    sql_subquery_depth,
)


def _linking(**overrides) -> LinkingContext:
    values = {
        "normalized_question": "show sales",
        "schema_fingerprint": "fp",
        "resolved": ResolvedIdentifierSet(
            tables=["sales"],
            columns=["sales.amount"],
            join_hints=["sales"],
        ),
        "confidence": 0.9,
        "retrieval_meta": {"schema_avg_score": 0.9, "schema_margin": 0.4},
    }
    values.update(overrides)
    return LinkingContext(**values)


def _classify(question: str, sql: str, **kwargs):
    linking = kwargs.pop("linking", _linking())
    constraint = kwargs.pop("constraint", ConstraintValidationResult(passed=True))
    signals = extract_risk_signals(
        question=question,
        sql=sql,
        linking=linking,
        constraint=constraint,
        model_confidence=kwargs.pop("model_confidence", 0.9),
        estimated_scan_rows=kwargs.pop("estimated_scan_rows", 10),
        timeout_or_failure=kwargs.pop("timeout_or_failure", False),
    )
    return RiskClassifier().classify(signals)


def test_simple_grounded_select_is_fast() -> None:
    result = _classify("Show sales", "SELECT amount FROM sales")
    assert result.level == "fast"
    assert result.score <= 0.34
    assert result.classifier_version


def test_single_join_or_aggregation_stays_standard_boundary() -> None:
    result = _classify(
        "Show total sales by region",
        "SELECT r.name, SUM(s.amount) FROM sales s JOIN regions r ON s.region_id = r.id GROUP BY r.name",
    )
    assert result.level == "standard"
    assert "single_join" in result.reason_codes
    assert "aggregation_complexity" in result.reason_codes


def test_nested_set_operation_escalates_to_strict() -> None:
    result = _classify(
        "Compare active and inactive customers",
        "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders) UNION SELECT id FROM leads",
    )
    assert result.level == "strict"
    assert "nested_query" in result.reason_codes
    assert "set_operation" in result.reason_codes


def test_unknown_identifiers_cannot_reach_fast_execution() -> None:
    result = _classify(
        "Show margin",
        "SELECT margin FROM sales",
        linking=_linking(unresolved_identifiers=["margin"], confidence=0.4),
    )
    assert result.level in {"standard", "strict"}
    assert "unknown_identifiers" in result.reason_codes


def test_destructive_sql_is_never_downgraded() -> None:
    result = _classify("Drop sales", "DROP TABLE sales")
    assert result.level == "strict"
    assert "unsafe_intent_or_sql" in result.reason_codes


def test_failure_escalates_safely() -> None:
    result = _classify(
        "Show sales",
        "SELECT amount FROM sales",
        timeout_or_failure=True,
    )
    assert result.level in {"standard", "strict"}
    assert "validation_failure_escalation" in result.reason_codes


def test_subquery_depth_counts_nested_selects() -> None:
    assert sql_subquery_depth("SELECT * FROM a WHERE id IN (SELECT id FROM b)") == 1
