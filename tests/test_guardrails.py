from src.guardrails.rules import apply_guardrails, enforce_limit


def test_blocks_dml_statement() -> None:
    result = apply_guardrails(
        sql='DELETE FROM employees',
        max_rows=1000,
        max_subquery_depth=3,
    )
    assert not result.allowed
    assert any('forbidden' in reason.lower() for reason in result.reasons)


def test_enforces_limit_when_missing() -> None:
    sql = 'SELECT * FROM employees'
    guarded = enforce_limit(sql, max_rows=1000)
    assert 'LIMIT 1000' in guarded.upper()


def test_blocks_high_scan_estimate() -> None:
    result = apply_guardrails(
        sql='SELECT * FROM sales',
        max_rows=1000,
        max_subquery_depth=3,
        explain_estimated_rows=2_000_000,
        explain_row_limit=1_000_000,
    )
    assert not result.allowed
    assert any('estimated scan rows' in reason.lower() for reason in result.reasons)
