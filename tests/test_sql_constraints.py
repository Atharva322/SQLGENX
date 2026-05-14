from src.models.schemas import LinkingContext, ResolvedIdentifierSet
from src.validation.sql_constraints import validate_sql_identifiers


def test_unknown_identifier_is_blocked() -> None:
    linking = LinkingContext(
        normalized_question="show revenue by region",
        schema_fingerprint="fp",
        resolved=ResolvedIdentifierSet(
            tables=["sales"],
            columns=["sales.amount", "sales.region"],
            join_hints=["sales"],
        ),
    )
    result = validate_sql_identifiers(
        "SELECT s.amount FROM sales s JOIN users u ON s.user_id = u.id",
        linking,
    )
    assert not result.passed
    assert "users" in " ".join(result.blocked_identifiers).lower()


def test_join_not_grounded_is_blocked() -> None:
    linking = LinkingContext(
        normalized_question="compare revenue and employee salary",
        schema_fingerprint="fp",
        resolved=ResolvedIdentifierSet(
            tables=["sales", "employees"],
            columns=["sales.amount", "employees.salary", "sales.emp_id", "employees.id"],
            join_hints=["sales"],
        ),
    )
    result = validate_sql_identifiers(
        "SELECT s.amount, e.salary FROM sales s JOIN employees e ON s.emp_id = e.id",
        linking,
        strict_join_grounding=True,
    )
    assert not result.passed
    assert result.violation_type == "join_not_grounded"


def test_grounded_join_passes() -> None:
    linking = LinkingContext(
        normalized_question="compare revenue and salary",
        schema_fingerprint="fp",
        resolved=ResolvedIdentifierSet(
            tables=["sales", "employees"],
            columns=["sales.amount", "employees.salary", "sales.emp_id", "employees.id"],
            join_hints=["sales", "employees"],
        ),
    )
    result = validate_sql_identifiers(
        "SELECT s.amount, e.salary FROM sales s JOIN employees e ON s.emp_id = e.id",
        linking,
        strict_join_grounding=True,
    )
    assert result.passed
