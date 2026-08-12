from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any

from src.context.documents import (
    ContextDocument,
    combined_source_fingerprint,
    example_documents,
    foreign_key_edges,
    hashed_embedding,
    schema_documents,
    semantic_documents,
    semantic_source_fingerprint,
    tokenize,
)
from src.semantic.loader import load_semantic_layer_cached
from src.context.fusion import reciprocal_rank_fusion


INDEX_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class ContextIndexStatus:
    exists: bool
    active: bool
    connection_id: str
    schema_fingerprint: str
    source_fingerprint: str | None = None
    document_count: int = 0
    schema_document_count: int = 0
    example_document_count: int = 0
    semantic_document_count: int = 0
    created_at: str | None = None
    stale: bool = True


@dataclass(frozen=True)
class RetrievalHit:
    doc_id: str
    kind: str
    score: float
    lane_scores: dict[str, float]
    reason: str
    table_name: str | None
    payload: dict[str, Any]


class ContextIndex:
    _active_version_cache: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}
    _document_cache: dict[tuple[str, str], tuple[list[ContextDocument], set[tuple[str, str]]]] = {}

    def __init__(
        self,
        path: str | Path,
        embedding_model: str = "deterministic-hash-v1",
        chunking_version: str = "schema-doc-v1",
        retrieval_config: dict[str, Any] | None = None,
    ) -> None:
        self.path = Path(path)
        self.embedding_model = embedding_model
        self.chunking_version = chunking_version
        self.retrieval_config = retrieval_config or {"rrf_k": 60}

    def build(
        self,
        connection_id: str,
        schema_fingerprint: str,
        schema: dict[str, Any],
        feedback_examples: list[dict[str, Any]],
    ) -> ContextIndexStatus:
        semantic_docs, semantic_fingerprint = self._semantic_documents()
        docs = schema_documents(schema) + example_documents(feedback_examples) + semantic_docs
        edges = foreign_key_edges(schema)
        source_fingerprint = self._source_fingerprint(schema, feedback_examples, semantic_fingerprint)
        version_id = uuid.uuid4().hex
        created_at = datetime.now(UTC).isoformat()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            self._ensure_schema(conn)
            with conn:
                conn.execute(
                    """
                    INSERT INTO index_versions (
                        version_id, connection_id, schema_fingerprint, source_fingerprint,
                        embedding_model, chunking_version, retrieval_config_json,
                        index_schema_version, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        version_id,
                        connection_id,
                        schema_fingerprint,
                        source_fingerprint,
                        self.embedding_model,
                        self.chunking_version,
                        json.dumps(self.retrieval_config, sort_keys=True),
                        INDEX_SCHEMA_VERSION,
                        created_at,
                    ),
                )
                for doc in docs:
                    conn.execute(
                        """
                        INSERT INTO context_documents (
                            version_id, doc_id, kind, source_id, text, tokens_json,
                            embedding_json, table_name, column_name, payload_json
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            version_id,
                            doc.doc_id,
                            doc.kind,
                            doc.source_id,
                            doc.text,
                            json.dumps(doc.tokens),
                            json.dumps(doc.embedding),
                            doc.table_name,
                            doc.column_name,
                            json.dumps(doc.payload or {}, sort_keys=True),
                        ),
                    )
                for from_table, to_table in edges:
                    conn.execute(
                        "INSERT INTO fk_edges (version_id, from_table, to_table) VALUES (?, ?, ?)",
                        (version_id, from_table, to_table),
                    )
                conn.execute(
                    """
                    INSERT INTO active_indexes (
                        connection_id, schema_fingerprint, embedding_model,
                        chunking_version, retrieval_config_json, version_id
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT (
                        connection_id, schema_fingerprint, embedding_model,
                        chunking_version, retrieval_config_json
                    ) DO UPDATE SET version_id=excluded.version_id
                    """,
                    (
                        connection_id,
                        schema_fingerprint,
                        self.embedding_model,
                        self.chunking_version,
                        json.dumps(self.retrieval_config, sort_keys=True),
                        version_id,
                    ),
                )
        finally:
            conn.close()
        self._active_version_cache.clear()
        self._document_cache.clear()
        return self.status(connection_id, schema_fingerprint, schema, feedback_examples)

    def refresh_if_stale(
        self,
        connection_id: str,
        schema_fingerprint: str,
        schema: dict[str, Any],
        feedback_examples: list[dict[str, Any]],
    ) -> ContextIndexStatus:
        current = self.status(connection_id, schema_fingerprint, schema, feedback_examples)
        if current.active and not current.stale:
            return current
        return self.build(connection_id, schema_fingerprint, schema, feedback_examples)

    def retrieve(
        self,
        question: str,
        connection_id: str,
        schema_fingerprint: str,
        top_k_schema: int,
        top_k_examples: int,
    ) -> tuple[list[RetrievalHit], dict[str, Any]]:
        conn = self._connect()
        try:
            self._ensure_schema(conn)
            version = self._active_version(conn, connection_id, schema_fingerprint)
            if not version:
                return [], {"mode": "context_index_miss", "index_active": False}
            cache_key = (str(self.path), str(version["version_id"]))
            cached_documents = self._document_cache.get(cache_key)
            if cached_documents is None:
                docs = self._load_documents(conn, version["version_id"])
                edges = self._load_edges(conn, version["version_id"])
                self._document_cache[cache_key] = (docs, edges)
            else:
                docs, edges = cached_documents
        finally:
            conn.close()

        q_tokens = tokenize(question)
        q_embedding = hashed_embedding(q_tokens)
        lexical = self._lexical_rank(q_tokens, docs)
        semantic = self._semantic_rank(q_embedding, docs)
        overlap = self._overlap_rank(q_tokens, docs)
        fused = reciprocal_rank_fusion(
            [
                [(doc.doc_id, score) for doc, score in lexical],
                [(doc.doc_id, score) for doc, score in semantic],
                [(doc.doc_id, score) for doc, score in overlap],
            ],
            k=int(self.retrieval_config.get("rrf_k", 60)),
        )
        lane_scores = self._lane_score_map(lexical, semantic, overlap)
        docs_by_id = {doc.doc_id: doc for doc in docs}
        schema_hits: list[RetrievalHit] = []
        example_hits: list[RetrievalHit] = []
        for doc_id, score in fused:
            doc = docs_by_id[doc_id]
            doc_lane_scores = lane_scores.get(doc_id, {})
            if max(doc_lane_scores.values(), default=0.0) <= 0:
                continue
            hit = self._hit(doc, score, doc_lane_scores, "rrf")
            if hit.kind == "schema" and len(schema_hits) < top_k_schema:
                schema_hits.append(hit)
            elif hit.kind == "example" and len(example_hits) < top_k_examples:
                example_hits.append(hit)
            if len(schema_hits) >= top_k_schema and len(example_hits) >= top_k_examples:
                break

        schema_hits = self._expand_one_hop(schema_hits, docs_by_id, edges, top_k_schema)
        meta = {
            "mode": "context_index",
            "index_active": True,
            "version_id": version["version_id"],
            "source_fingerprint": version["source_fingerprint"],
            "schema_method": "hybrid_rrf",
            "example_method": "hybrid_rrf",
            "schema_candidates": sum(1 for doc in docs if doc.kind == "schema"),
            "example_candidates": sum(1 for doc in docs if doc.kind == "example"),
            "lanes": ["lexical", "semantic_hash", "token_overlap"],
            "schema_reasons": [hit.reason for hit in schema_hits],
            "example_reasons": [hit.reason for hit in example_hits],
        }
        return schema_hits + example_hits, meta

    def status(
        self,
        connection_id: str,
        schema_fingerprint: str,
        schema: dict[str, Any] | None = None,
        feedback_examples: list[dict[str, Any]] | None = None,
    ) -> ContextIndexStatus:
        source_fingerprint = None
        if schema is not None and feedback_examples is not None:
            _, semantic_fingerprint = self._semantic_documents()
            source_fingerprint = self._source_fingerprint(schema, feedback_examples, semantic_fingerprint)
        if not self.path.exists():
            return ContextIndexStatus(False, False, connection_id, schema_fingerprint, source_fingerprint)
        conn = self._connect()
        try:
            self._ensure_schema(conn)
            version = self._active_version(conn, connection_id, schema_fingerprint)
            if not version:
                return ContextIndexStatus(True, False, connection_id, schema_fingerprint, source_fingerprint)
            counts = self._document_counts(conn, version["version_id"])
            stale = bool(source_fingerprint and version["source_fingerprint"] != source_fingerprint)
            return ContextIndexStatus(
                exists=True,
                active=True,
                connection_id=connection_id,
                schema_fingerprint=schema_fingerprint,
                source_fingerprint=version["source_fingerprint"],
                document_count=counts["total"],
                schema_document_count=counts["schema"],
                example_document_count=counts["example"],
                semantic_document_count=counts["semantic"],
                created_at=version["created_at"],
                stale=stale,
            )
        finally:
            conn.close()

    def clear(self, connection_id: str | None = None) -> None:
        if not self.path.exists():
            return
        conn = self._connect()
        try:
            self._ensure_schema(conn)
            with conn:
                if connection_id:
                    versions = [
                        row[0]
                        for row in conn.execute(
                            "SELECT version_id FROM index_versions WHERE connection_id=?",
                            (connection_id,),
                        ).fetchall()
                    ]
                    conn.execute("DELETE FROM active_indexes WHERE connection_id=?", (connection_id,))
                    conn.execute("DELETE FROM index_versions WHERE connection_id=?", (connection_id,))
                    for version_id in versions:
                        conn.execute("DELETE FROM context_documents WHERE version_id=?", (version_id,))
                        conn.execute("DELETE FROM fk_edges WHERE version_id=?", (version_id,))
                else:
                    conn.execute("DELETE FROM active_indexes")
                    conn.execute("DELETE FROM index_versions")
                    conn.execute("DELETE FROM context_documents")
                    conn.execute("DELETE FROM fk_edges")
        finally:
            conn.close()
            self._active_version_cache.clear()
            self._document_cache.clear()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self, conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS index_versions (
                version_id TEXT PRIMARY KEY,
                connection_id TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                source_fingerprint TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                chunking_version TEXT NOT NULL,
                retrieval_config_json TEXT NOT NULL,
                index_schema_version INTEGER NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS active_indexes (
                connection_id TEXT NOT NULL,
                schema_fingerprint TEXT NOT NULL,
                embedding_model TEXT NOT NULL,
                chunking_version TEXT NOT NULL,
                retrieval_config_json TEXT NOT NULL,
                version_id TEXT NOT NULL,
                PRIMARY KEY (
                    connection_id, schema_fingerprint, embedding_model,
                    chunking_version, retrieval_config_json
                )
            );
            CREATE TABLE IF NOT EXISTS context_documents (
                version_id TEXT NOT NULL,
                doc_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                source_id TEXT NOT NULL,
                text TEXT NOT NULL,
                tokens_json TEXT NOT NULL,
                embedding_json TEXT NOT NULL,
                table_name TEXT,
                column_name TEXT,
                payload_json TEXT NOT NULL,
                PRIMARY KEY (version_id, doc_id)
            );
            CREATE TABLE IF NOT EXISTS fk_edges (
                version_id TEXT NOT NULL,
                from_table TEXT NOT NULL,
                to_table TEXT NOT NULL,
                PRIMARY KEY (version_id, from_table, to_table)
            );
            """
        )

    def _active_version(
        self, conn: sqlite3.Connection, connection_id: str, schema_fingerprint: str
    ) -> dict[str, Any] | None:
        config_json = json.dumps(self.retrieval_config, sort_keys=True)
        cache_key = (
            str(self.path),
            connection_id,
            schema_fingerprint,
            self.embedding_model,
            self.chunking_version,
            config_json,
        )
        cached = self._active_version_cache.get(cache_key)
        if cached is not None:
            return cached
        row = conn.execute(
            """
            SELECT v.* FROM active_indexes a
            JOIN index_versions v ON v.version_id = a.version_id
            WHERE a.connection_id=? AND a.schema_fingerprint=?
              AND a.embedding_model=? AND a.chunking_version=?
              AND a.retrieval_config_json=?
            """,
            (
                connection_id,
                schema_fingerprint,
                self.embedding_model,
                self.chunking_version,
                config_json,
            ),
        ).fetchone()
        if row is None:
            return None
        version = dict(row)
        self._active_version_cache[cache_key] = version
        return version

    def _document_counts(self, conn: sqlite3.Connection, version_id: str) -> dict[str, int]:
        rows = conn.execute(
            "SELECT kind, COUNT(*) AS count FROM context_documents WHERE version_id=? GROUP BY kind",
            (version_id,),
        ).fetchall()
        counts = {"schema": 0, "example": 0, "semantic": 0}
        for row in rows:
            counts[str(row["kind"])] = int(row["count"])
        counts["total"] = counts["schema"] + counts["example"] + counts["semantic"]
        return counts

    def _semantic_documents(self) -> tuple[list[ContextDocument], str]:
        try:
            definition = load_semantic_layer_cached()
        except Exception:
            return [], "none"
        payload = definition.model_dump(mode="json")
        return semantic_documents(definition), semantic_source_fingerprint(payload)

    def _source_fingerprint(
        self, schema: dict[str, Any], feedback_examples: list[dict[str, Any]], semantic_fingerprint: str
    ) -> str:
        return semantic_source_fingerprint(
            {
                "schema_examples": combined_source_fingerprint(schema, feedback_examples),
                "semantic": semantic_fingerprint,
            }
        )

    def _load_documents(self, conn: sqlite3.Connection, version_id: str) -> list[ContextDocument]:
        rows = conn.execute(
            "SELECT * FROM context_documents WHERE version_id=? ORDER BY kind, doc_id",
            (version_id,),
        ).fetchall()
        return [
            ContextDocument(
                doc_id=str(row["doc_id"]),
                kind=str(row["kind"]),
                source_id=str(row["source_id"]),
                text=str(row["text"]),
                tokens=tuple(json.loads(row["tokens_json"])),
                embedding=tuple(float(v) for v in json.loads(row["embedding_json"])),
                table_name=row["table_name"],
                column_name=row["column_name"],
                payload=json.loads(row["payload_json"]),
            )
            for row in rows
        ]

    def _load_edges(self, conn: sqlite3.Connection, version_id: str) -> set[tuple[str, str]]:
        rows = conn.execute(
            "SELECT from_table, to_table FROM fk_edges WHERE version_id=?",
            (version_id,),
        ).fetchall()
        return {(str(row["from_table"]), str(row["to_table"])) for row in rows}

    def _lexical_rank(
        self, q_tokens: tuple[str, ...], docs: list[ContextDocument]
    ) -> list[tuple[ContextDocument, float]]:
        q_set = set(q_tokens)
        ranked: list[tuple[ContextDocument, float]] = []
        for doc in docs:
            d_set = set(doc.tokens)
            overlap = len(q_set.intersection(d_set))
            union = len(q_set.union(d_set))
            ranked.append((doc, overlap / max(1, union)))
        ranked.sort(key=lambda item: (-item[1], item[0].doc_id))
        return ranked

    def _overlap_rank(
        self, q_tokens: tuple[str, ...], docs: list[ContextDocument]
    ) -> list[tuple[ContextDocument, float]]:
        q_set = set(q_tokens)
        ranked = [(doc, float(len(q_set.intersection(set(doc.tokens))))) for doc in docs]
        ranked.sort(key=lambda item: (-item[1], item[0].doc_id))
        return ranked

    def _semantic_rank(
        self, q_embedding: tuple[float, ...], docs: list[ContextDocument]
    ) -> list[tuple[ContextDocument, float]]:
        ranked = [(doc, sum(a * b for a, b in zip(q_embedding, doc.embedding))) for doc in docs]
        ranked.sort(key=lambda item: (-item[1], item[0].doc_id))
        return ranked

    def _lane_score_map(
        self,
        lexical: list[tuple[ContextDocument, float]],
        semantic: list[tuple[ContextDocument, float]],
        overlap: list[tuple[ContextDocument, float]],
    ) -> dict[str, dict[str, float]]:
        scores: dict[str, dict[str, float]] = {}
        for lane, ranking in (
            ("lexical", lexical),
            ("semantic_hash", semantic),
            ("token_overlap", overlap),
        ):
            for doc, score in ranking:
                scores.setdefault(doc.doc_id, {})[lane] = round(float(score), 6)
        return scores

    def _hit(
        self, doc: ContextDocument, score: float, lane_scores: dict[str, float], method: str
    ) -> RetrievalHit:
        top_lane = max(lane_scores.items(), key=lambda item: item[1])[0] if lane_scores else method
        reason = f"{doc.doc_id} selected by {method}; strongest_lane={top_lane}"
        return RetrievalHit(
            doc_id=doc.doc_id,
            kind=doc.kind,
            score=round(score, 6),
            lane_scores=lane_scores,
            reason=reason,
            table_name=doc.table_name,
            payload=doc.payload or {},
        )

    def _expand_one_hop(
        self,
        schema_hits: list[RetrievalHit],
        docs_by_id: dict[str, ContextDocument],
        edges: set[tuple[str, str]],
        top_k_schema: int,
    ) -> list[RetrievalHit]:
        if len(schema_hits) >= top_k_schema:
            return schema_hits
        selected = list(schema_hits)
        selected_tables = {hit.table_name for hit in selected if hit.table_name}
        for from_table, to_table in sorted(edges):
            if from_table not in selected_tables or to_table in selected_tables:
                continue
            doc = docs_by_id.get(f"schema:{to_table}")
            if doc is None:
                continue
            selected.append(
                self._hit(doc, 0.0, {"relationship": 1.0}, f"one_hop:{from_table}->{to_table}")
            )
            selected_tables.add(to_table)
            if len(selected) >= top_k_schema:
                break
        return selected
