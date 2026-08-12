import sqlite3
from pathlib import Path
from types import SimpleNamespace
import uuid

import pytest

from src.context.documents import combined_source_fingerprint, schema_source_fingerprint
from src.context.fusion import reciprocal_rank_fusion
from src.context.index import ContextIndex
from src.services import rag_retriever


def _schema(extra_column: bool = False) -> dict:
    order_columns = [
        {"name": "id", "type": "INTEGER", "nullable": False},
        {"name": "customer_id", "type": "INTEGER", "nullable": False},
        {"name": "amount", "type": "NUMERIC", "nullable": False},
    ]
    if extra_column:
        order_columns.append({"name": "discount", "type": "NUMERIC", "nullable": True})
    return {
        "schema_fingerprint": "fp-orders",
        "tables": [
            {
                "table": "customers",
                "columns": [
                    {"name": "id", "type": "INTEGER", "nullable": False},
                    {"name": "name", "type": "TEXT", "nullable": False},
                ],
                "foreign_keys": [],
            },
            {
                "table": "orders",
                "columns": order_columns,
                "foreign_keys": [
                    {
                        "constrained_columns": ["customer_id"],
                        "referred_table": "customers",
                        "referred_columns": ["id"],
                    }
                ],
            },
        ],
    }


def _examples() -> list[dict]:
    return [
        {
            "question": "Show order totals",
            "sql": "SELECT SUM(amount) FROM orders",
            "confidence": 0.9,
            "connection_id": "default",
        }
    ]


def _db_path(name: str) -> Path:
    root = Path(".tmp") / "context-index-tests"
    root.mkdir(parents=True, exist_ok=True)
    return root / f"{name}-{uuid.uuid4().hex}.sqlite3"


def test_source_fingerprint_includes_fk_edges_and_columns() -> None:
    base = _schema()
    changed = _schema(extra_column=True)
    assert schema_source_fingerprint(base) != schema_source_fingerprint(changed)
    assert combined_source_fingerprint(base, _examples()) == combined_source_fingerprint(base, _examples())


def test_rrf_is_deterministic_for_ties() -> None:
    first = reciprocal_rank_fusion([[("b", 1.0), ("a", 1.0)]], k=60)
    second = reciprocal_rank_fusion([[("b", 1.0), ("a", 1.0)]], k=60)
    assert first == second
    assert [doc_id for doc_id, _ in first] == ["b", "a"]


def test_sqlite_build_reopen_retrieve_and_one_hop() -> None:
    path = _db_path("reopen")
    index = ContextIndex(path)
    status = index.build("default", "fp-orders", _schema(), _examples())
    assert status.active
    assert status.schema_document_count == 2
    assert status.semantic_document_count >= 5

    reopened = ContextIndex(path)
    hits, meta = reopened.retrieve(
        "total order amount",
        connection_id="default",
        schema_fingerprint="fp-orders",
        top_k_schema=2,
        top_k_examples=1,
    )

    schema_tables = [hit.table_name for hit in hits if hit.kind == "schema"]
    assert meta["mode"] == "context_index"
    assert "orders" in schema_tables
    assert "customers" in schema_tables
    assert any("one_hop" in reason for reason in meta["schema_reasons"])
    assert [hit.payload for hit in hits if hit.kind == "example"][0]["sql"].startswith("SELECT")


def test_atomic_rebuild_failure_leaves_previous_index(monkeypatch) -> None:
    path = _db_path("atomic")
    index = ContextIndex(path)
    index.build("default", "fp-orders", _schema(), _examples())
    before = index.status("default", "fp-orders")

    def explode(_schema: dict) -> list:
        raise RuntimeError("boom")

    monkeypatch.setattr("src.context.index.schema_documents", explode)
    with pytest.raises(RuntimeError):
        index.build("default", "fp-orders", _schema(extra_column=True), _examples())

    after = index.status("default", "fp-orders")
    assert after.active
    assert after.source_fingerprint == before.source_fingerprint


def test_connection_isolation_and_model_version_rebuild() -> None:
    path = _db_path("isolation")
    ContextIndex(path, embedding_model="v1").build("default", "fp-orders", _schema(), _examples())
    other = ContextIndex(path, embedding_model="v1").status("tenant-b", "fp-orders", _schema(), _examples())
    newer = ContextIndex(path, embedding_model="v2").status("default", "fp-orders", _schema(), _examples())

    assert other.exists and not other.active
    assert newer.exists and not newer.active


def test_retriever_uses_index_when_enabled(monkeypatch) -> None:
    path = _db_path("retriever")
    settings = SimpleNamespace(
        context_index_enabled=True,
        context_index_path=str(path),
        context_index_embedding_model="deterministic-hash-v1",
        context_index_chunking_version="schema-doc-v1",
        context_index_rrf_k=60,
    )
    monkeypatch.setattr(rag_retriever, "get_settings", lambda: settings)

    result = rag_retriever.retrieve_context(
        "total order amount",
        _schema(),
        _examples(),
        top_k_schema=2,
        top_k_examples=1,
        connection_id="default",
    )

    assert result.retrieval_meta["mode"] == "context_index"
    assert result.retrieval_meta["schema_method"] == "hybrid_rrf"
    assert result.retrieval_meta["index_semantic_document_count"] >= 5
    assert [table["table"] for table in result.selected_schema_tables] == ["orders", "customers"]


def test_corrupted_index_falls_back_without_credentials(monkeypatch) -> None:
    path = _db_path("corrupt")
    path.write_text("not sqlite", encoding="utf-8")
    settings = SimpleNamespace(
        context_index_enabled=True,
        context_index_path=str(path),
        context_index_embedding_model="deterministic-hash-v1",
        context_index_chunking_version="schema-doc-v1",
        context_index_rrf_k=60,
    )
    monkeypatch.setattr(rag_retriever, "get_settings", lambda: settings)

    result = rag_retriever.retrieve_context(
        "orders",
        _schema(),
        _examples(),
        top_k_schema=1,
        top_k_examples=1,
        connection_id="postgresql://user:secret@host/db",
    )

    assert result.retrieval_meta["mode"] == "context_index_fallback"
    assert "secret" not in str(result.retrieval_meta)
    with pytest.raises(sqlite3.DatabaseError):
        sqlite3.connect(path).execute("SELECT * FROM active_indexes").fetchall()
