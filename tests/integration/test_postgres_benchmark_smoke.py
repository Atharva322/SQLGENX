import pytest

from src.db.engine import check_connection
from src.db.schema_introspector import get_schema_summary
from src.services.query_service import QueryService


@pytest.mark.integration
def test_postgres_benchmark_connection_and_trace_smoke(monkeypatch) -> None:
    ok, error = check_connection("default")
    if not ok:
        pytest.skip(f"PostgreSQL unavailable: {error}")

    monkeypatch.setenv("LLM_PROVIDER", "deterministic")
    monkeypatch.setenv("LLM_MODEL", "sqlgenx-fixture-v1")
    from src.config.settings import get_settings

    get_settings.cache_clear()
    schema = get_schema_summary("default")
    assert schema.get("tables")

    service = QueryService()
    response = service.process_question("List departments", connection_id="default")

    assert response.execution_meta.trace_coverage_ratio >= 0.95
    assert response.execution_meta.sql_statement_count > 0
