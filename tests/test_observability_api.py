from types import SimpleNamespace

from fastapi.testclient import TestClient

from src.api import main
from src.llm.client import GeneratedSQL
from src.models.schemas import LinkingContext, QueryPlanDraft, ResolvedIdentifierSet
from src.services.query_service import QueryService


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
        alternative_sql_complexity_threshold=2,
        constrained_sql_enabled=False,
        constrained_sql_strict_identifiers=False,
        join_grounding_strict_enabled=False,
        max_subquery_depth=3,
        max_explain_rows=1000000,
        enable_multi_query_validation=False,
        multi_query_complexity_threshold=2,
        multi_query_easy_skip_enabled=True,
        intermediate_trace_logging_enabled=False,
    )


def test_query_response_keeps_old_meta_and_adds_trace_fields(monkeypatch) -> None:
    settings = _settings()
    monkeypatch.setattr("src.services.query_service.get_settings", lambda: settings)
    service = QueryService()
    service.settings = settings
    main.service = service

    monkeypatch.setattr(
        "src.services.query_service.get_schema_summary",
        lambda connection_id=None: {
            "schema_fingerprint": "fp",
            "tables": [
                {
                    "table": "sales",
                    "columns": [
                        {"name": "amount", "type": "INTEGER", "nullable": False},
                        {"name": "region", "type": "TEXT", "nullable": True},
                    ],
                    "foreign_keys": [],
                }
            ],
        },
    )
    monkeypatch.setattr(
        "src.services.query_service.select_relevant_feedback_examples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "src.services.query_service.run_schema_linking",
        lambda **kwargs: SimpleNamespace(
            context=LinkingContext(
                normalized_question="show sales",
                schema_fingerprint="fp",
                resolved=ResolvedIdentifierSet(
                    tables=["sales"],
                    columns=["sales.amount", "sales.region"],
                    join_hints=["sales"],
                ),
                confidence=0.9,
            ),
            selected_schema_tables=kwargs["schema"]["tables"],
            selected_examples=[],
        ),
    )
    monkeypatch.setattr("src.services.query_service.build_prompt", lambda *args, **kwargs: "prompt")
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *args, **kwargs: SimpleNamespace(
            plan=QueryPlanDraft(
                target_tables=["sales"],
                target_columns=["sales.amount", "sales.region"],
                join_path=["sales"],
            ),
            confidence=0.8,
            token_usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
        ),
    )
    monkeypatch.setattr(
        service.llm,
        "generate_structured_sql",
        lambda *args, **kwargs: GeneratedSQL(
            sql="SELECT amount, region FROM sales",
            explanation="ok",
            accessed_tables=["sales"],
            accessed_columns=["sales.amount", "sales.region"],
            model_confidence=0.8,
            token_usage={"prompt_tokens": 2, "completion_tokens": 2, "total_tokens": 4},
        ),
    )
    monkeypatch.setattr(service.llm, "back_translate_sql", lambda *args, **kwargs: "show sales")
    monkeypatch.setattr(service, "_run_explain", lambda *args, **kwargs: ["rows=1"])
    monkeypatch.setattr(
        service,
        "_execute_read_only",
        lambda *args, **kwargs: ([{"amount": 10, "region": "East"}], ["rows=1"], 3),
    )

    body = TestClient(main.app).post("/v1/query", json={"question": "Show sales"}).json()
    meta = body["execution_meta"]

    assert "stage_latencies_ms" in meta
    assert "stage_durations_ms" in meta
    assert meta["total_duration_ms"] >= 0
    assert meta["trace_coverage_ratio"] >= 0
    assert meta["llm_call_count"] >= 3
    assert meta["db_round_trip_count"] >= 2
    assert meta["validation_level"] == "standard"
