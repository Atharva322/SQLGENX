from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from src.semantic.cli import lint, run_definition_tests
from src.semantic.layer import SemanticLayer
from src.semantic.loader import load_semantic_layer


def test_semantic_yaml_lints_and_definition_tests_pass() -> None:
    assert lint(Path("semantic/metrics.yaml"))["metric_count"] == 5
    assert run_definition_tests(Path("semantic/metrics.yaml"))["test_count"] == 5


def test_compiler_resolves_paraphrases_to_same_metric_sql() -> None:
    layer = SemanticLayer(load_semantic_layer())
    first = layer.compile_question("total revenue by region", schema=_schema())
    second = layer.compile_question("bookings by territory", schema=_schema())

    assert first.matched
    assert second.matched
    assert first.metric_id == second.metric_id == "total_revenue"
    assert first.sql == second.sql
    assert "SUM(sales.amount)" in first.sql
    assert first.disclosure["metric"]["version"] == "1.0.0"


def test_unsupported_semantic_combination_fails_clearly() -> None:
    layer = SemanticLayer(load_semantic_layer())
    result = layer.compile_question("total revenue by department", schema=_schema())

    assert result.matched
    assert result.unsupported
    assert result.metric_id == "total_revenue"
    assert "does not allow" in result.reason
    assert result.sql == ""


def test_general_text_to_sql_fallback_when_no_metric_matches() -> None:
    layer = SemanticLayer(load_semantic_layer())
    result = layer.compile_question("list employees in engineering", schema=_schema())

    assert not result.matched
    assert result.reason == "no_semantic_metric_match"


def test_semantic_dimension_matching_rejects_partial_synonym_overlap() -> None:
    layer = SemanticLayer(load_semantic_layer())
    result = layer.compile_question("rank employees by total sales", schema=_schema())

    assert not result.matched
    assert result.reason == "no_semantic_dimension_match"


def test_invalid_semantic_definitions_fail_closed() -> None:
    scratch = _scratch_dir()
    duplicate_metric = scratch / "duplicate.yaml"
    duplicate_metric.write_text(
        """
semantic_layer:
  id: sample
  version: "1"
  entities:
    - id: sales
      table: sales
      primary_key: id
  dimensions: []
  metrics:
    - id: total_revenue
      version: "1"
      description: A
      source: sales
      expression: SUM(sales.amount)
      owner: a
    - id: total_revenue
      version: "1"
      description: B
      source: sales
      expression: SUM(sales.amount)
      owner: b
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate metric"):
        load_semantic_layer(duplicate_metric)

    unsafe_expression = scratch / "unsafe.yaml"
    unsafe_expression.write_text(
        """
semantic_layer:
  id: sample
  version: "1"
  entities:
    - id: sales
      table: sales
      primary_key: id
  dimensions: []
  metrics:
    - id: bad_metric
      version: "1"
      description: Bad
      source: sales
      expression: SUM(sales.amount) + random()
      owner: a
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="arbitrary functions"):
        load_semantic_layer(unsafe_expression)


def test_join_graph_cycles_and_fanout_are_rejected() -> None:
    scratch = _scratch_dir()
    cycle = scratch / "cycle.yaml"
    cycle.write_text(
        """
semantic_layer:
  id: sample
  version: "1"
  entities:
    - id: a
      table: a
      primary_key: id
    - id: b
      table: b
      primary_key: id
  approved_joins:
    - id: a_b
      left_table: a
      left_column: b_id
      right_table: b
      right_column: id
      relationship: many_to_one
    - id: b_a
      left_table: b
      left_column: a_id
      right_table: a
      right_column: id
      relationship: many_to_one
  dimensions:
    - id: b_name
      table: b
      expression: b.name
      label: B Name
  metrics:
    - id: metric_a
      version: "1"
      description: A
      source: a
      expression: COUNT(a.id)
      allowed_dimensions: [b_name]
      owner: a
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="cycle"):
        load_semantic_layer(cycle)

    fanout = scratch / "fanout.yaml"
    fanout.write_text(
        """
semantic_layer:
  id: sample
  version: "1"
  entities:
    - id: orders
      table: orders
      primary_key: id
    - id: order_items
      table: order_items
      primary_key: id
  approved_joins:
    - id: order_items_order
      left_table: orders
      left_column: id
      right_table: order_items
      right_column: order_id
      relationship: one_to_many
  dimensions:
    - id: product_line
      table: order_items
      expression: order_items.product_id
      label: Product Line
  metrics:
    - id: order_value
      version: "1"
      description: Order value
      source: orders
      expression: SUM(orders.total)
      allowed_dimensions: [product_line]
      owner: ops
""",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="fanout"):
        load_semantic_layer(fanout)


def test_query_service_uses_semantic_compile_path(monkeypatch) -> None:
    from src.llm.client import GeneratedSQL
    import src.services.query_service as query_module
    from src.services.query_service import QueryService

    schema = _schema()
    monkeypatch.setattr(query_module, "available_connections", lambda: {"default": "Default"})
    monkeypatch.setattr(query_module, "get_schema_summary", lambda connection_id=None: schema)
    monkeypatch.setattr(query_module, "select_relevant_feedback_examples", lambda *a, **k: [])
    monkeypatch.setattr(query_module, "run_schema_linking", lambda *a, **k: _linking())

    service = QueryService()
    service.settings.semantic_layer_enabled = True
    service.settings.constrained_sql_enabled = True
    service.settings.join_grounding_strict_enabled = True
    service.settings.identifier_resolution_fail_fast_enabled = False
    service.llm.generate_structured_sql = lambda *a, **k: pytest.fail("semantic metric should bypass provider SQL")
    service.llm.generate_query_plan = lambda *a, **k: pytest.fail("semantic metric should bypass provider plan")
    service.llm.back_translate_sql = lambda *a, **k: "total revenue by region"
    monkeypatch.setattr(service, "_run_explain", lambda *a, **k: ["rows=3"])
    monkeypatch.setattr(
        service,
        "_execute_read_only",
        lambda *a, **k: ([{"region": "EMEA", "total_revenue": 32000}], ["rows=3"], 5),
    )

    response = service.process_question("Show total revenue by region")

    assert response.reasoning.strategy == "semantic_layer_compile"
    assert response.reasoning.selected_candidate == "total_revenue"
    assert response.execution_meta.semantic_layer["metric"]["id"] == "total_revenue"
    assert response.execution_meta.llm_call_count == 0
    assert response.sql.startswith("SELECT sales.region AS region")
    assert response.results == [{"region": "EMEA", "total_revenue": 32000}]


def test_query_service_semantic_unsupported_does_not_execute(monkeypatch) -> None:
    import src.services.query_service as query_module
    from src.services.query_service import QueryService

    monkeypatch.setattr(query_module, "available_connections", lambda: {"default": "Default"})
    monkeypatch.setattr(query_module, "get_schema_summary", lambda connection_id=None: _schema())
    monkeypatch.setattr(query_module, "select_relevant_feedback_examples", lambda *a, **k: [])
    monkeypatch.setattr(query_module, "run_schema_linking", lambda *a, **k: _linking())

    service = QueryService()
    service.settings.identifier_resolution_fail_fast_enabled = False
    monkeypatch.setattr(service, "_execute_read_only", lambda *a, **k: pytest.fail("unsupported semantic combo must not execute"))

    response = service.process_question("Show total revenue by department")

    assert response.sql == "UNANSWERABLE"
    assert response.reasoning.strategy == "semantic_layer_unsupported"
    assert "does not allow" in response.warnings[0]
    assert response.execution_meta.query_execution_count == 0


def test_query_service_falls_back_when_no_metric_matches(monkeypatch) -> None:
    import src.services.query_service as query_module
    from src.llm.client import GeneratedQueryPlan, GeneratedSQL
    from src.models.schemas import QueryPlanDraft
    from src.services.query_service import QueryService

    monkeypatch.setattr(query_module, "available_connections", lambda: {"default": "Default"})
    monkeypatch.setattr(query_module, "get_schema_summary", lambda connection_id=None: _schema())
    monkeypatch.setattr(query_module, "select_relevant_feedback_examples", lambda *a, **k: [])
    monkeypatch.setattr(query_module, "run_schema_linking", lambda *a, **k: _linking())

    service = QueryService()
    service.settings.identifier_resolution_fail_fast_enabled = False
    service.llm.generate_query_plan = lambda *a, **k: GeneratedQueryPlan(
        plan=QueryPlanDraft(target_tables=["employees"], target_columns=["employees.first_name"]),
        confidence=0.9,
        token_usage={"provider": "deterministic", "model": "test", "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    )
    service.llm.generate_structured_sql = lambda *a, **k: GeneratedSQL(
        sql="SELECT first_name FROM employees",
        explanation="Fallback provider SQL.",
        accessed_tables=["employees"],
        accessed_columns=["employees.first_name"],
        model_confidence=0.9,
        token_usage={"provider": "deterministic", "model": "test", "prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    )
    service.llm.back_translate_sql = lambda *a, **k: "list employees"
    monkeypatch.setattr(service, "_run_explain", lambda *a, **k: ["rows=3"])
    monkeypatch.setattr(service, "_execute_read_only", lambda *a, **k: ([{"first_name": "Ava"}], ["rows=3"], 5))

    response = service.process_question("List employees in Engineering")

    assert response.reasoning.strategy == "primary_then_adaptive_validation"
    assert response.execution_meta.llm_token_usage["calls"] == 2
    assert response.sql.startswith("SELECT first_name")


def _schema() -> dict:
    return {
        "schema_fingerprint": "phase0-benchmark-v1",
        "tables": [
            {
                "table": "sales",
                "columns": [
                    {"name": "id"},
                    {"name": "employee_id"},
                    {"name": "amount"},
                    {"name": "region"},
                    {"name": "channel"},
                    {"name": "sale_date"},
                ],
            },
            {
                "table": "employees",
                "columns": [
                    {"name": "id"},
                    {"name": "department_id"},
                    {"name": "first_name"},
                    {"name": "salary"},
                ],
            },
            {"table": "departments", "columns": [{"name": "id"}, {"name": "name"}]},
            {"table": "customers", "columns": [{"name": "id"}, {"name": "state"}]},
            {"table": "orders", "columns": [{"name": "id"}, {"name": "customer_id"}, {"name": "status"}, {"name": "total"}]},
            {
                "table": "invoices",
                "columns": [
                    {"name": "id"},
                    {"name": "customer_id"},
                    {"name": "order_id"},
                    {"name": "status"},
                    {"name": "total"},
                ],
            },
        ],
    }


def _linking():
    from src.models.schemas import LinkingContext, ResolvedIdentifierSet

    return SimpleNamespace(
        context=LinkingContext(
            normalized_question="",
            schema_fingerprint="phase0-benchmark-v1",
            resolved=ResolvedIdentifierSet(
                tables=["sales", "employees"],
                columns=["sales.region", "sales.amount", "employees.first_name"],
                join_hints=["sales", "employees"],
            ),
            confidence=0.9,
        ),
        selected_schema_tables=[],
        selected_examples=[],
    )


def _scratch_dir() -> Path:
    path = Path(".tmp") / "semantic-tests" / uuid4().hex
    path.mkdir(parents=True, exist_ok=True)
    return path
