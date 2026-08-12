from src.validation.ast_sql import analyze_sql_ast, are_semantically_duplicate


def _schema() -> dict:
    return {
        "tables": [
            {
                "table": "employees",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "department_id", "type": "INTEGER"},
                    {"name": "salary", "type": "NUMERIC"},
                    {"name": "name", "type": "TEXT"},
                ],
            },
            {
                "table": "departments",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "TEXT"},
                ],
            },
            {
                "table": "sales",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "amount", "type": "NUMERIC"},
                    {"name": "region", "type": "TEXT"},
                ],
            },
        ]
    }


def test_extracts_aliases_joins_aggregations_and_groupings() -> None:
    result = analyze_sql_ast(
        "SELECT d.name, AVG(e.salary) AS avg_salary "
        "FROM employees e JOIN departments d ON e.department_id = d.id "
        "GROUP BY d.name ORDER BY avg_salary DESC",
        _schema(),
    )

    assert result.valid
    assert result.tables == ["departments", "employees"]
    assert result.aliases == {"d": "departments", "e": "employees"}
    assert "avg" in result.aggregations
    assert result.groupings
    assert result.orderings
    assert result.fingerprint


def test_cte_outputs_are_schema_validated() -> None:
    result = analyze_sql_ast(
        "WITH totals AS (SELECT region, SUM(amount) AS total FROM sales GROUP BY region) "
        "SELECT region, total FROM totals",
        _schema(),
    )

    assert result.valid
    assert result.ctes == ["totals"]
    assert "sales" in result.tables


def test_union_and_subquery_depth_are_extracted() -> None:
    result = analyze_sql_ast(
        "SELECT id FROM employees WHERE id IN (SELECT id FROM employees) "
        "UNION SELECT id FROM sales",
        _schema(),
    )

    assert result.valid
    assert result.set_operations == ["union"]
    assert result.subquery_depth >= 1


def test_malformed_sql_fails_closed() -> None:
    result = analyze_sql_ast("SELECT FROM", _schema())
    assert not result.valid
    assert "parse_failed" in result.reason_codes


def test_writable_statement_fails_closed() -> None:
    result = analyze_sql_ast("DELETE FROM employees", _schema())
    assert not result.valid
    assert "non_readonly_root" in result.reason_codes or "writable_ast_node" in result.reason_codes


def test_unknown_and_ambiguous_columns_are_blocked() -> None:
    unknown = analyze_sql_ast("SELECT bonus FROM employees", _schema())
    ambiguous = analyze_sql_ast(
        "SELECT id FROM employees JOIN departments ON employees.department_id = departments.id",
        _schema(),
    )

    assert not unknown.valid
    assert "unknown_column:bonus" in unknown.reason_codes
    assert not ambiguous.valid
    assert "ambiguous_unqualified_column:id" in ambiguous.reason_codes


def test_select_star_policy_can_block_star() -> None:
    result = analyze_sql_ast("SELECT * FROM employees", _schema(), allow_select_star=False)
    assert not result.valid
    assert "star_not_allowed:*" in result.reason_codes


def test_normalization_equivalence_and_non_equivalence() -> None:
    assert are_semantically_duplicate(
        "select amount from sales",
        "SELECT amount FROM sales",
    )
    assert not are_semantically_duplicate(
        "SELECT amount FROM sales",
        "SELECT region FROM sales",
    )
