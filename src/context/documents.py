from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
import math
import re
from typing import Any


STOPWORDS = {
    "the",
    "a",
    "an",
    "what",
    "show",
    "list",
    "for",
    "from",
    "with",
    "and",
    "or",
    "by",
    "to",
    "of",
    "in",
    "on",
}
VECTOR_DIMENSIONS = 64


@dataclass(frozen=True)
class ContextDocument:
    doc_id: str
    kind: str
    source_id: str
    text: str
    tokens: tuple[str, ...]
    embedding: tuple[float, ...]
    table_name: str | None = None
    column_name: str | None = None
    payload: dict[str, Any] | None = None


def tokenize(text: str) -> tuple[str, ...]:
    return tuple(
        token
        for token in re.findall(r"[a-zA-Z0-9_]+", text.lower())
        if token not in STOPWORDS and len(token) > 2
    )


def stable_hash(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def schema_source_fingerprint(schema: dict[str, Any]) -> str:
    tables: list[dict[str, Any]] = []
    for table in schema.get("tables", []):
        table_name = str(table.get("table", ""))
        columns = [
            {
                "name": str(col.get("name", "")),
                "type": str(col.get("type", "")),
                "nullable": bool(col.get("nullable", True)),
            }
            for col in table.get("columns", [])
        ]
        columns.sort(key=lambda col: col["name"])
        fks = [
            {
                "constrained_columns": [str(v) for v in fk.get("constrained_columns", [])],
                "referred_table": str(fk.get("referred_table", "")),
                "referred_columns": [str(v) for v in fk.get("referred_columns", [])],
            }
            for fk in table.get("foreign_keys", [])
        ]
        fks.sort(
            key=lambda fk: (
                ",".join(fk["constrained_columns"]),
                fk["referred_table"],
                ",".join(fk["referred_columns"]),
            )
        )
        tables.append({"table": table_name, "columns": columns, "foreign_keys": fks})
    tables.sort(key=lambda item: item["table"])
    return stable_hash({"tables": tables})[:16]


def example_source_fingerprint(examples: list[dict[str, Any]]) -> str:
    normalized = [
        {
            "question": str(example.get("question", "")).strip(),
            "sql": str(example.get("sql", "")).strip(),
            "confidence": round(float(example.get("confidence", 0.0)), 6),
            "connection_id": example.get("connection_id"),
            "schema_fingerprint": example.get("schema_fingerprint"),
            "source": example.get("source"),
        }
        for example in examples
    ]
    normalized.sort(key=lambda item: (item["question"], item["sql"], str(item["source"])))
    return stable_hash(normalized)[:16]


def combined_source_fingerprint(schema: dict[str, Any], examples: list[dict[str, Any]]) -> str:
    return stable_hash(
        {
            "schema": schema_source_fingerprint(schema),
            "examples": example_source_fingerprint(examples),
        }
    )[:16]


def semantic_source_fingerprint(payload: Any) -> str:
    return stable_hash(payload)[:16]


def hashed_embedding(tokens: tuple[str, ...], dimensions: int = VECTOR_DIMENSIONS) -> tuple[float, ...]:
    values = [0.0] * dimensions
    if not tokens:
        return tuple(values)
    for token in tokens:
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        bucket = int.from_bytes(digest[:4], "big") % dimensions
        sign = 1.0 if digest[4] % 2 == 0 else -1.0
        values[bucket] += sign
    norm = math.sqrt(sum(value * value for value in values))
    if norm == 0:
        return tuple(values)
    return tuple(value / norm for value in values)


def schema_documents(schema: dict[str, Any]) -> list[ContextDocument]:
    docs: list[ContextDocument] = []
    for table in sorted(schema.get("tables", []), key=lambda item: str(item.get("table", ""))):
        table_name = str(table.get("table", "")).strip()
        if not table_name:
            continue
        columns = sorted(table.get("columns", []), key=lambda col: str(col.get("name", "")))
        col_text = "; ".join(
            f"{col.get('name', '')} {col.get('type', '')} nullable={bool(col.get('nullable', True))}"
            for col in columns
        )
        fk_text = "; ".join(
            f"{','.join(str(v) for v in fk.get('constrained_columns', []))}"
            f"->{fk.get('referred_table', '')}.{','.join(str(v) for v in fk.get('referred_columns', []))}"
            for fk in table.get("foreign_keys", [])
        )
        text = f"Table {table_name}. Columns: {col_text}. Foreign keys: {fk_text or 'none'}."
        tokens = tokenize(text)
        docs.append(
            ContextDocument(
                doc_id=f"schema:{table_name}",
                kind="schema",
                source_id=table_name,
                text=text,
                tokens=tokens,
                embedding=hashed_embedding(tokens),
                table_name=table_name,
                payload=table,
            )
        )
    return docs


def example_documents(examples: list[dict[str, Any]]) -> list[ContextDocument]:
    docs: list[ContextDocument] = []
    for idx, example in enumerate(examples):
        question = str(example.get("question", "")).strip()
        sql = str(example.get("sql", "")).strip()
        if not question or not sql:
            continue
        text = f"Question: {question}\nSQL: {sql}"
        tokens = tokenize(text)
        source_id = stable_hash({"question": question, "sql": sql})[:16]
        docs.append(
            ContextDocument(
                doc_id=f"example:{source_id}:{idx}",
                kind="example",
                source_id=source_id,
                text=text,
                tokens=tokens,
                embedding=hashed_embedding(tokens),
                payload=example,
            )
        )
    return docs


def semantic_documents(definition: Any) -> list[ContextDocument]:
    docs: list[ContextDocument] = []
    for metric in getattr(definition, "metrics", []):
        dimensions = ", ".join(metric.allowed_dimensions)
        filters = ", ".join(metric.allowed_filters)
        text = (
            f"Metric {metric.id} version {metric.version}. {metric.description}. "
            f"Synonyms: {', '.join(metric.synonyms)}. "
            f"Source: {metric.source}. Expression: {metric.expression}. "
            f"Allowed dimensions: {dimensions or 'none'}. Filters: {filters or 'none'}."
        )
        tokens = tokenize(text)
        docs.append(
            ContextDocument(
                doc_id=f"semantic:metric:{metric.id}",
                kind="semantic",
                source_id=metric.id,
                text=text,
                tokens=tokens,
                embedding=hashed_embedding(tokens),
                table_name=metric.source,
                payload={
                    "kind": "metric",
                    "id": metric.id,
                    "version": metric.version,
                    "owner": metric.owner,
                    "sensitivity": metric.sensitivity,
                    "allowed_dimensions": list(metric.allowed_dimensions),
                    "allowed_filters": list(metric.allowed_filters),
                },
            )
        )
    return docs


def foreign_key_edges(schema: dict[str, Any]) -> set[tuple[str, str]]:
    edges: set[tuple[str, str]] = set()
    table_names = {str(table.get("table", "")) for table in schema.get("tables", [])}
    for table in schema.get("tables", []):
        table_name = str(table.get("table", ""))
        for fk in table.get("foreign_keys", []):
            referred = str(fk.get("referred_table", ""))
            if table_name and referred and referred in table_names:
                edges.add((table_name, referred))
                edges.add((referred, table_name))
    return edges
