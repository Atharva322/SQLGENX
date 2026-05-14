from types import SimpleNamespace

from src.llm.client import GeneratedSQL
from src.models.schemas import LinkingContext, QueryPlanDraft, ResolvedIdentifierSet
from src.services.query_service import QueryService


def _service_with_settings(monkeypatch, settings_overrides=None) -> QueryService:
    settings = SimpleNamespace(
        max_result_rows=1000,
        rag_top_k_schema=5,
        rag_top_k_examples=3,
        rag_min_feedback_confidence=0.65,
        identifier_resolution_fail_fast_enabled=True,
        fail_fast_min_link_confidence=0.25,
        fail_fast_max_unresolved=6,
        fail_fast_require_low_confidence=True,
        alternative_sql_adaptive_enabled=True,
        alternative_sql_complexity_threshold=2,
        constrained_sql_enabled=True,
        constrained_sql_strict_identifiers=True,
        join_grounding_strict_enabled=True,
        max_subquery_depth=3,
        max_explain_rows=1000000,
        enable_multi_query_validation=False,
        multi_query_complexity_threshold=2,
        multi_query_easy_skip_enabled=True,
        intermediate_trace_logging_enabled=False,
    )
    if settings_overrides:
        for key, value in settings_overrides.items():
            setattr(settings, key, value)
    monkeypatch.setattr("src.services.query_service.get_settings", lambda: settings)
    service = QueryService()
    service.settings = settings
    return service


def test_low_confidence_no_resolution_returns_pre_generation_unanswerable(monkeypatch) -> None:
    service = _service_with_settings(monkeypatch)
    monkeypatch.setattr(
        "src.services.query_service.get_schema_summary",
        lambda connection_id=None: {"tables": [], "schema_fingerprint": "fp"},
    )
    monkeypatch.setattr(
        "src.services.query_service.select_relevant_feedback_examples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "src.services.query_service.run_schema_linking",
        lambda **kwargs: SimpleNamespace(
            context=LinkingContext(
                normalized_question="show profit margin",
                schema_fingerprint="fp",
                resolved=ResolvedIdentifierSet(tables=[], columns=[], join_hints=[]),
                confidence=0.1,
                unresolved_identifiers=["profit", "margin"],
                ambiguous=False,
                resolution_status="resolved",
                join_grounding_status="pending",
            ),
            selected_schema_tables=[],
            selected_examples=[],
        ),
    )

    called = {"primary": 0}

    def _primary(*args, **kwargs):
        called["primary"] += 1
        return GeneratedSQL(
            sql="SELECT 1",
            explanation="x",
            accessed_tables=[],
            accessed_columns=[],
            model_confidence=0.5,
            token_usage={},
        )

    monkeypatch.setattr(service.llm, "generate_structured_sql", _primary)
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *args, **kwargs: SimpleNamespace(
            plan=QueryPlanDraft(),
            confidence=0.0,
            token_usage={},
        ),
    )
    response = service.process_question("Show profit margin")
    assert response.sql == "UNANSWERABLE"
    assert response.reasoning.strategy == "severe_fail_fast_unanswerable"
    assert called["primary"] == 0


def test_medium_confidence_partial_resolution_still_generates(monkeypatch) -> None:
    service = _service_with_settings(monkeypatch)
    monkeypatch.setattr(
        "src.services.query_service.get_schema_summary",
        lambda connection_id=None: {"tables": [], "schema_fingerprint": "fp"},
    )
    monkeypatch.setattr(
        "src.services.query_service.select_relevant_feedback_examples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "src.services.query_service.run_schema_linking",
        lambda **kwargs: SimpleNamespace(
            context=LinkingContext(
                normalized_question="show revenue by region",
                schema_fingerprint="fp",
                resolved=ResolvedIdentifierSet(
                    tables=["sales"],
                    columns=["sales.amount", "sales.region"],
                    join_hints=["sales"],
                ),
                confidence=0.5,
                unresolved_identifiers=["revenue"],
                ambiguous=False,
                resolution_status="resolved",
                join_grounding_status="pending",
            ),
            selected_schema_tables=[],
            selected_examples=[],
        ),
    )

    called = {"primary": 0}

    def _primary(*args, **kwargs):
        called["primary"] += 1
        return GeneratedSQL(
            sql="SELECT amount, region FROM sales",
            explanation="ok",
            accessed_tables=["sales"],
            accessed_columns=["sales.amount", "sales.region"],
            model_confidence=0.7,
            token_usage={},
        )

    monkeypatch.setattr(service.llm, "generate_structured_sql", _primary)
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *args, **kwargs: SimpleNamespace(
            plan=QueryPlanDraft(
                target_tables=["sales"],
                target_columns=["sales.amount", "sales.region"],
                grouping=["sales.region"],
                aggregations=["SUM"],
                join_path=["sales"],
            ),
            confidence=0.7,
            token_usage={},
        ),
    )
    monkeypatch.setattr(service, "_run_explain", lambda *args, **kwargs: ["rows=10"])
    monkeypatch.setattr(service, "_execute_read_only", lambda *args, **kwargs: ([{"amount": 1, "region": "E"}], ["rows=10"], 5))
    monkeypatch.setattr(
        "src.services.query_service.build_prompt",
        lambda *args, **kwargs: "prompt",
    )
    monkeypatch.setattr(service.llm, "back_translate_sql", lambda *args, **kwargs: "show revenue by region")
    response = service.process_question("Show revenue by region")
    assert called["primary"] == 1
    assert response.reasoning.strategy == "primary_only_easy_path"
    assert response.sql != "UNANSWERABLE"


def test_ambiguous_but_not_low_confidence_still_generates(monkeypatch) -> None:
    service = _service_with_settings(monkeypatch)
    monkeypatch.setattr(
        "src.services.query_service.get_schema_summary",
        lambda connection_id=None: {"tables": [], "schema_fingerprint": "fp"},
    )
    monkeypatch.setattr(
        "src.services.query_service.select_relevant_feedback_examples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "src.services.query_service.run_schema_linking",
        lambda **kwargs: SimpleNamespace(
            context=LinkingContext(
                normalized_question="show revenue by team",
                schema_fingerprint="fp",
                resolved=ResolvedIdentifierSet(
                    tables=["sales"],
                    columns=["sales.amount"],
                    join_hints=["sales"],
                ),
                confidence=0.6,
                unresolved_identifiers=["team"],
                ambiguous=True,
                ambiguity_reasons=["team may map to multiple concepts"],
                resolution_status="ambiguous",
                join_grounding_status="pending",
            ),
            selected_schema_tables=[],
            selected_examples=[],
        ),
    )
    called = {"primary": 0}

    def _primary(*args, **kwargs):
        called["primary"] += 1
        return GeneratedSQL(
            sql="SELECT amount FROM sales",
            explanation="ok",
            accessed_tables=["sales"],
            accessed_columns=["sales.amount"],
            model_confidence=0.6,
            token_usage={},
        )

    monkeypatch.setattr(service.llm, "generate_structured_sql", _primary)
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *args, **kwargs: SimpleNamespace(
            plan=QueryPlanDraft(
                target_tables=["sales"],
                target_columns=["sales.amount"],
                join_path=["sales"],
                notes=["team unresolved"],
            ),
            confidence=0.6,
            token_usage={},
        ),
    )
    monkeypatch.setattr(service, "_run_explain", lambda *args, **kwargs: ["rows=10"])
    monkeypatch.setattr(service, "_execute_read_only", lambda *args, **kwargs: ([{"amount": 1}], ["rows=10"], 5))
    monkeypatch.setattr("src.services.query_service.build_prompt", lambda *args, **kwargs: "prompt")
    monkeypatch.setattr(service.llm, "back_translate_sql", lambda *args, **kwargs: "show revenue")
    response = service.process_question("Show revenue by team")
    assert called["primary"] == 1
    assert response.sql != "UNANSWERABLE"


def test_query_plan_stage_populates_reasoning(monkeypatch) -> None:
    service = _service_with_settings(monkeypatch)
    monkeypatch.setattr(
        "src.services.query_service.get_schema_summary",
        lambda connection_id=None: {"tables": [], "schema_fingerprint": "fp"},
    )
    monkeypatch.setattr(
        "src.services.query_service.select_relevant_feedback_examples",
        lambda *args, **kwargs: [],
    )
    monkeypatch.setattr(
        "src.services.query_service.run_schema_linking",
        lambda **kwargs: SimpleNamespace(
            context=LinkingContext(
                normalized_question="show total sales by region",
                schema_fingerprint="fp",
                resolved=ResolvedIdentifierSet(
                    tables=["sales"],
                    columns=["sales.amount", "sales.region"],
                    join_hints=["sales"],
                ),
                confidence=0.8,
                unresolved_identifiers=[],
                ambiguous=False,
                resolution_status="resolved",
                join_grounding_status="pending",
            ),
            selected_schema_tables=[],
            selected_examples=[],
        ),
    )
    monkeypatch.setattr(
        service.llm,
        "generate_query_plan",
        lambda *args, **kwargs: SimpleNamespace(
            plan=QueryPlanDraft(
                target_tables=["sales"],
                target_columns=["sales.amount", "sales.region"],
                grouping=["sales.region"],
                aggregations=["SUM"],
                join_path=["sales"],
            ),
            confidence=0.8,
            token_usage={},
        ),
    )
    monkeypatch.setattr(
        service.llm,
        "generate_structured_sql",
        lambda *args, **kwargs: GeneratedSQL(
            sql="SELECT region, SUM(amount) FROM sales GROUP BY region",
            explanation="ok",
            accessed_tables=["sales"],
            accessed_columns=["sales.amount", "sales.region"],
            model_confidence=0.8,
            token_usage={},
        ),
    )
    monkeypatch.setattr(service, "_run_explain", lambda *args, **kwargs: ["rows=10"])
    monkeypatch.setattr(service, "_execute_read_only", lambda *args, **kwargs: ([{"region": "E", "sum": 1}], ["rows=10"], 5))
    monkeypatch.setattr("src.services.query_service.build_prompt", lambda *args, **kwargs: "prompt")
    monkeypatch.setattr(service.llm, "back_translate_sql", lambda *args, **kwargs: "show total sales by region")
    response = service.process_question("Show total sales by region")
    assert response.reasoning.query_plan["target_tables"] == ["sales"]
