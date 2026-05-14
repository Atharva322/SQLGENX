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
