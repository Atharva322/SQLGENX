from src.validation.alignment import verify_sql_alignment
from src.validation.multi_query import evaluate_multi_query_agreement
from src.validation.sanity import analyze_result_sanity


def test_alignment_flags_low_overlap() -> None:
    result = verify_sql_alignment(
        original_question="What is total revenue by region this quarter?",
        back_translated_question="List employee names and departments.",
    )
    assert result.score < 0.55
    assert result.warnings


def test_sanity_detects_null_heavy_columns() -> None:
    rows = [
        {"department": None, "count": 10},
        {"department": None, "count": 12},
        {"department": "Sales", "count": 14},
        {"department": None, "count": 16},
        {"department": None, "count": 18},
    ]
    result = analyze_result_sanity(rows)
    assert result.score < 1.0
    assert any("NULL" in warning for warning in result.warnings)


def test_multi_query_agreement_scores_exact_match() -> None:
    rows = [{"region": "NA", "total": 100}]
    result = evaluate_multi_query_agreement(rows, rows)
    assert result.score == 1.0
    assert not result.warnings


def test_multi_query_agreement_flags_divergence() -> None:
    left = [{"region": "NA", "total": 100}]
    right = [{"region": "EMEA", "total": 500}]
    result = evaluate_multi_query_agreement(left, right)
    assert result.score < 1.0
    assert result.warnings
