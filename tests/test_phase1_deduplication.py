from types import SimpleNamespace

import pytest

from src.db import schema_introspector
from src.llm.client import GeneratedQueryPlan, GeneratedSQL
from src.models.schemas import LinkingContext, QueryPlanDraft, ResolvedIdentifierSet
from src.services import prompt_builder
from src.services.query_service import QueryService


@pytest.fixture(autouse=True)
def clear_phase1_caches():
    schema_introspector.clear_schema_summary_cache()
    prompt_builder.clear_prompt_asset_caches()
    yield
    schema_introspector.clear_schema_summary_cache()
    prompt_builder.clear_prompt_asset_caches()


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        max_result_rows=1000,
        rag_enabled=False,
        rag_top_k_schema=5,
        rag_top_k_examples=3,
        rag_min_feedback_confidence=0.65,
        identifier_resolution_fail_fast_enabled=True,
        fail_fast_min_link_confidence=0.25,
        fail_fast_max_unresolved=6,
        fail_fast_require_low_confidence=True,
        alternative_sql_adaptive_enabled=True,
        alternative_sql_complexity_threshold=0,
        constrained_sql_enabled=False,
        constrained_sql_strict_identifiers=False,
        join_grounding_strict_enabled=False,
        max_subquery_depth=3,
        max_explain_rows=1000000,
        enable_multi_query_validation=False,
        multi_query_complexity_threshold=2,
        multi_query_easy_skip_enabled=True,
        intermediate_trace_logging_enabled=False,
        phase1_cache_enabled=True,
        schema_cache_ttl_seconds=300,
        schema_cache_max_entries=32,
        prompt_asset_cache_enabled=True,
        prompt_asset_cache_max_entries=32,
        adaptive_validation_enabled=False,
        adaptive_validation_mode="shadow",
        risk_fast_max_score=0.34,
        risk_standard_max_score=0.67,
        risk_low_link_confidence=0.55,
        risk_low_model_confidence=0.55,
        risk_low_retrieval_margin=0.08,
        risk_high_scan_rows=100000,
    )


def _schema(table: str = "sales") -> dict:
    return {
        "schema_fingerprint": f"fp_{table}",
        "tables": [
            {
                "table": table,
                "columns": [
                    {"name": "amount", "type": "INTEGER", "nullable": False},
                    {"name": "region", "type": "TEXT", "nullable": True},
                ],
                "foreign_keys": [],
            }
        ],
    }


def _linking(schema: dict) -> SimpleNamespace:
    return SimpleNamespace(
        context=LinkingContext(
            normalized_question="show total sales by region",
            schema_fingerprint=schema["schema_fingerprint"],
            resolved=ResolvedIdentifierSet(
                tables=["sales"],
                columns=["sales.amount", "sales.region"],
                join_hints=["sales"],
            ),
            confidence=0.9,
        ),
        selected_schema_tables=schema["tables"],
        selected_examples=[],
    )


def test_query_request_loads_schema_once_and_reuses_backtranslation(monkeypatch) -> None:
    settings = _settings()
    monkeypatch.setattr("src.services.query_service.get_settings", lambda: settings)
    calls = {"schema": 0, "back_translate": 0, "alternative": 0}
    schema = _schema()

    def get_schema_summary(connection_id=None):
        calls["schema"] += 1
        return schema

    service = QueryService()
    service.settings = settings
    monkeypatch.setattr("src.services.query_service.get_schema_summary", get_schema_summary)
    monkeypatch.setattr("src.services.query_service.select_relevant_feedback_examples", lambda *a, **k: [])
    monkeypatch.setattr("src.services.query_service.run_schema_linking", lambda **kwargs: _linking(schema))
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *a, **k: GeneratedQueryPlan(
            plan=QueryPlanDraft(
                target_tables=["sales"],
                target_columns=["sales.amount", "sales.region"],
                grouping=["sales.region"],
                aggregations=["SUM"],
                join_path=["sales"],
            ),
            confidence=0.9,
            token_usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        ),
    )
    generated = GeneratedSQL(
        sql="SELECT region, SUM(amount) AS total_sales FROM sales GROUP BY region",
        explanation="ok",
        accessed_tables=["sales"],
        accessed_columns=["sales.amount", "sales.region"],
        model_confidence=0.9,
        token_usage={"prompt_tokens": 2, "completion_tokens": 2, "total_tokens": 4},
    )
    monkeypatch.setattr(service.llm, "generate_structured_sql", lambda *a, **k: generated)

    def alternative(*args, **kwargs):
        calls["alternative"] += 1
        return generated

    def back_translate_sql(*args, **kwargs):
        calls["back_translate"] += 1
        return "show total sales by region"

    monkeypatch.setattr(service.llm, "generate_alternative_sql", alternative)
    monkeypatch.setattr(service.llm, "back_translate_sql", back_translate_sql)
    monkeypatch.setattr(service, "_run_explain", lambda *a, **k: ["rows=1"])
    monkeypatch.setattr(
        service,
        "_execute_read_only",
        lambda *a, **k: ([{"region": "East", "total_sales": 10}], ["rows=1"], 3),
    )

    response = service.process_question("Show total sales by region", connection_id="default")

    assert response.sql.startswith("SELECT region")
    assert calls["schema"] == 1
    assert calls["alternative"] == 0
    assert calls["back_translate"] == 1


def test_schema_summary_cache_is_connection_scoped_and_warm_hits(monkeypatch) -> None:
    schema_introspector.clear_schema_summary_cache()
    settings = _settings()
    monkeypatch.setattr(schema_introspector, "get_settings", lambda: settings)
    calls: list[str] = []

    def introspect(connection_id=None):
        key = connection_id or "default"
        calls.append(key)
        return _schema(key)

    monkeypatch.setattr(schema_introspector, "_introspect_schema", introspect)

    first_default = schema_introspector.get_schema_summary("default")
    second_default = schema_introspector.get_schema_summary("default")
    analytics = schema_introspector.get_schema_summary("analytics")

    assert first_default == second_default
    assert first_default["schema_fingerprint"] == "fp_default"
    assert analytics["schema_fingerprint"] == "fp_analytics"
    assert calls == ["default", "analytics"]
