from types import SimpleNamespace

from src.services import schema_linker


def test_synonym_resolution_maps_to_canonical_identifiers(monkeypatch) -> None:
    monkeypatch.setattr(
        schema_linker,
        "get_settings",
        lambda: SimpleNamespace(
            rag_enabled=False,
            schema_linking_enabled=True,
            schema_link_top_k=8,
            schema_synonym_source="",
        ),
    )
    schema = {
        "tables": [
            {
                "table": "sales",
                "columns": [
                    {"name": "amount", "type": "NUMERIC"},
                    {"name": "region", "type": "TEXT"},
                ],
            }
        ],
        "schema_fingerprint": "fp1",
    }
    artifacts = schema_linker.run_schema_linking(
        question="show revenue by territory",
        schema=schema,
        feedback_examples=[],
        top_k_schema=5,
        top_k_examples=3,
    )
    assert "sales" in artifacts.context.resolved.tables
    assert "sales.amount" in artifacts.context.resolved.columns
    assert "sales.region" in artifacts.context.resolved.columns


def test_link_resolver_marks_ambiguity(monkeypatch) -> None:
    monkeypatch.setattr(
        schema_linker,
        "get_settings",
        lambda: SimpleNamespace(
            rag_enabled=False,
            schema_linking_enabled=True,
            schema_link_top_k=8,
            schema_synonym_source="",
        ),
    )
    schema = {
        "tables": [
            {"table": "orders", "columns": [{"name": "amount", "type": "NUMERIC"}]},
            {"table": "sales", "columns": [{"name": "amount", "type": "NUMERIC"}]},
        ],
        "schema_fingerprint": "fp2",
    }
    artifacts = schema_linker.run_schema_linking(
        question="show total amount",
        schema=schema,
        feedback_examples=[],
        top_k_schema=5,
        top_k_examples=3,
    )
    assert artifacts.context.ambiguous is True
    assert artifacts.context.ambiguity_reasons


def test_grounding_scores_full_schema_and_expands_resolved_table_columns(monkeypatch) -> None:
    monkeypatch.setattr(
        schema_linker,
        "get_settings",
        lambda: SimpleNamespace(
            rag_enabled=True,
            schema_linking_enabled=True,
            schema_link_top_k=8,
            schema_synonym_source="",
        ),
    )
    monkeypatch.setattr(
        schema_linker,
        "retrieve_context",
        lambda **kwargs: SimpleNamespace(
            selected_schema_tables=[
                {
                    "table": "order_items",
                    "columns": [
                        {"name": "product_id", "type": "INTEGER"},
                        {"name": "quantity", "type": "INTEGER"},
                    ],
                }
            ],
            selected_examples=[],
            retrieval_meta={"mode": "test"},
        ),
    )
    schema = {
        "tables": [
            {
                "table": "order_items",
                "columns": [
                    {"name": "product_id", "type": "INTEGER"},
                    {"name": "quantity", "type": "INTEGER"},
                ],
            },
            {
                "table": "products",
                "columns": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "TEXT"},
                ],
            },
        ],
        "schema_fingerprint": "fp3",
    }
    artifacts = schema_linker.run_schema_linking(
        question="show order items with product names",
        schema=schema,
        feedback_examples=[],
        top_k_schema=1,
        top_k_examples=0,
    )
    assert "products" in artifacts.context.resolved.tables
    assert "products.id" in artifacts.context.resolved.columns
    assert "products.name" in artifacts.context.resolved.columns
    assert "order_items.product_id" in artifacts.context.resolved.columns
