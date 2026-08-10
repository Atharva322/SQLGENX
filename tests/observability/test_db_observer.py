from src.observability.db_observer import classify_statement, safe_sql_fingerprint


def test_statement_classifier_uses_safe_categories() -> None:
    assert classify_statement("EXPLAIN SELECT * FROM employees") == "explain"
    assert classify_statement("SET TRANSACTION READ ONLY") == "read_only_config"
    assert classify_statement("select * from information_schema.tables") == "schema"
    assert classify_statement("SELECT first_name FROM employees") == "query"


def test_sql_fingerprint_does_not_expose_statement_text() -> None:
    fingerprint = safe_sql_fingerprint("SELECT secret_column FROM customers")

    assert len(fingerprint) == 16
    assert "secret" not in fingerprint
    assert "customers" not in fingerprint
