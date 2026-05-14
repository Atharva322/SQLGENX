from dataclasses import dataclass
from pathlib import Path
import json
import re
from typing import Any

from src.config.settings import get_settings
from src.db.schema_introspector import compute_schema_fingerprint
from src.models.schemas import LinkingContext, ResolvedIdentifierSet, SchemaCandidate
from src.services.rag_retriever import _tokenize, retrieve_context


DEFAULT_SYNONYMS = {
    "table_synonyms": {
        "sales": ["revenue", "orders", "transactions"],
        "employees": ["staff", "workers", "team"],
        "customers": ["clients", "buyers"],
    },
    "column_synonyms": {
        "amount": ["revenue", "sales_amount", "value", "total"],
        "region": ["territory", "area", "zone"],
        "created_at": ["date", "timestamp", "created_on"],
    },
}


@dataclass
class SchemaLinkingArtifacts:
    context: LinkingContext
    selected_schema_tables: list[dict[str, Any]]
    selected_examples: list[dict[str, Any]]


def _normalize_question(question: str) -> str:
    compact = " ".join(question.strip().split())
    compact = re.sub(r"\b(last|past)\s+(\d+)\s+(days|months|years)\b", r"last_\2_\3", compact, flags=re.I)
    compact = re.sub(r"\b(this)\s+(quarter|month|year)\b", r"current_\2", compact, flags=re.I)
    return compact


def _load_synonyms() -> dict[str, dict[str, list[str]]]:
    settings = get_settings()
    source = (settings.schema_synonym_source or "").strip()
    if not source:
        return DEFAULT_SYNONYMS
    try:
        path = Path(source)
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return {
                    "table_synonyms": dict(payload.get("table_synonyms", {})),
                    "column_synonyms": dict(payload.get("column_synonyms", {})),
                }
    except Exception:
        pass
    return DEFAULT_SYNONYMS


def _build_schema_docs_with_synonyms(
    tables: list[dict[str, Any]], synonyms: dict[str, dict[str, list[str]]]
) -> list[str]:
    table_syn = synonyms.get("table_synonyms", {})
    col_syn = synonyms.get("column_synonyms", {})
    docs: list[str] = []
    for table in tables:
        table_name = str(table.get("table", ""))
        t_syn = ", ".join(table_syn.get(table_name, []))
        cols = []
        for col in table.get("columns", []):
            col_name = str(col.get("name", ""))
            c_syn = ", ".join(col_syn.get(col_name, []))
            cols.append(f"{col_name} ({col.get('type', '')}) synonyms: {c_syn}")
        docs.append(f"Table {table_name}. Synonyms: {t_syn}. Columns: {'; '.join(cols)}")
    return docs


def _score_candidates(
    question: str, tables: list[dict[str, Any]], synonyms: dict[str, dict[str, list[str]]], top_k: int
) -> tuple[list[SchemaCandidate], list[str]]:
    q_tokens = _tokenize(question)
    table_syn = synonyms.get("table_synonyms", {})
    col_syn = synonyms.get("column_synonyms", {})
    candidates: list[SchemaCandidate] = []
    synonym_hits: list[str] = []

    for table in tables:
        table_name = str(table.get("table", ""))
        table_tokens = _tokenize(table_name)
        table_synonyms = [s.lower() for s in table_syn.get(table_name, [])]
        t_overlap = len(q_tokens.intersection(table_tokens))
        syn_overlap = len(q_tokens.intersection(set(table_synonyms)))
        t_score = min(1.0, 0.5 * t_overlap + 0.3 * syn_overlap)
        if syn_overlap > 0:
            synonym_hits.extend(table_syn.get(table_name, []))
        if t_score > 0:
            candidates.append(
                SchemaCandidate(
                    identifier=table_name,
                    kind="table",
                    canonical_table=table_name,
                    score=round(t_score, 3),
                    evidence=[f"table_overlap={t_overlap}", f"table_synonym_overlap={syn_overlap}"],
                    matched_synonyms=table_syn.get(table_name, []) if syn_overlap else [],
                )
            )

        for col in table.get("columns", []):
            col_name = str(col.get("name", ""))
            fq = f"{table_name}.{col_name}"
            col_tokens = _tokenize(col_name)
            col_synonyms = [s.lower() for s in col_syn.get(col_name, [])]
            c_overlap = len(q_tokens.intersection(col_tokens))
            c_syn_overlap = len(q_tokens.intersection(set(col_synonyms)))
            c_score = min(1.0, 0.45 * c_overlap + 0.35 * c_syn_overlap)
            if c_syn_overlap > 0:
                synonym_hits.extend(col_syn.get(col_name, []))
            if c_score > 0:
                candidates.append(
                    SchemaCandidate(
                        identifier=fq,
                        kind="column",
                        canonical_table=table_name,
                        canonical_column=col_name,
                        score=round(c_score, 3),
                        evidence=[f"column_overlap={c_overlap}", f"column_synonym_overlap={c_syn_overlap}"],
                        matched_synonyms=col_syn.get(col_name, []) if c_syn_overlap else [],
                    )
                )

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[: max(1, top_k)], sorted(set(synonym_hits))


def _resolve_candidates(candidates: list[SchemaCandidate]) -> tuple[ResolvedIdentifierSet, bool, list[str], float]:
    tables: list[str] = []
    columns: list[str] = []
    for candidate in candidates:
        if candidate.kind == "table" and candidate.canonical_table:
            tables.append(candidate.canonical_table)
        if candidate.kind == "column" and candidate.canonical_table and candidate.canonical_column:
            tables.append(candidate.canonical_table)
            columns.append(f"{candidate.canonical_table}.{candidate.canonical_column}")

    tables = sorted(set(tables))
    columns = sorted(set(columns))
    join_hints = sorted({col.split(".")[0] for col in columns if "." in col})
    top_scores = sorted([c.score for c in candidates], reverse=True)[:2]
    ambiguous = len(top_scores) >= 2 and abs(top_scores[0] - top_scores[1]) < 0.08
    ambiguity_reasons: list[str] = []
    if ambiguous:
        ambiguity_reasons.append("Top schema-link candidates have near-equal confidence.")

    confidence = round(sum(c.score for c in candidates[:5]) / max(1, min(5, len(candidates))), 3)
    return (
        ResolvedIdentifierSet(tables=tables, columns=columns, join_hints=join_hints),
        ambiguous,
        ambiguity_reasons,
        confidence,
    )


def _infer_unresolved_identifiers(
    question: str, resolved: ResolvedIdentifierSet, synonyms: dict[str, dict[str, list[str]]]
) -> list[str]:
    question_tokens = _tokenize(question)
    canonical_tokens = set()
    canonical_tokens.update(t.lower() for t in resolved.tables)
    canonical_tokens.update(c.split(".")[-1].lower() for c in resolved.columns)
    for values in synonyms.get("table_synonyms", {}).values():
        canonical_tokens.update(str(v).lower() for v in values)
    for values in synonyms.get("column_synonyms", {}).values():
        canonical_tokens.update(str(v).lower() for v in values)

    unresolved = [
        token
        for token in sorted(question_tokens)
        if token not in canonical_tokens and token not in {"count", "sum", "avg", "min", "max", "top", "last"}
    ]
    return unresolved[:8]


def run_schema_linking(
    question: str,
    schema: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
    top_k_schema: int,
    top_k_examples: int,
) -> SchemaLinkingArtifacts:
    settings = get_settings()
    normalized = _normalize_question(question)
    tables = list(schema.get("tables", []))
    schema_fingerprint = schema.get("schema_fingerprint") or compute_schema_fingerprint(schema)
    synonyms = _load_synonyms()

    selected_schema = tables[:top_k_schema]
    selected_examples = feedback_examples[:top_k_examples]
    retrieval_meta: dict[str, Any] = {"mode": "disabled"}
    if settings.rag_enabled:
        # Use existing retriever for coarse selection before scoring/linking.
        rag = retrieve_context(
            question=normalized,
            schema={"tables": tables},
            feedback_examples=feedback_examples,
            top_k_schema=top_k_schema,
            top_k_examples=top_k_examples,
        )
        selected_schema = rag.selected_schema_tables
        selected_examples = rag.selected_examples
        retrieval_meta = rag.retrieval_meta

    if settings.schema_linking_enabled:
        _ = _build_schema_docs_with_synonyms(selected_schema, synonyms)
        candidates, synonym_hits = _score_candidates(
            normalized, selected_schema, synonyms, top_k=max(1, settings.schema_link_top_k)
        )
        resolved, ambiguous, ambiguity_reasons, confidence = _resolve_candidates(candidates)
    else:
        candidates = []
        synonym_hits = []
        resolved = ResolvedIdentifierSet(
            tables=[str(t.get("table", "")) for t in selected_schema if t.get("table")],
            columns=[
                f"{t.get('table')}.{c.get('name')}"
                for t in selected_schema
                for c in t.get("columns", [])
                if t.get("table") and c.get("name")
            ],
            join_hints=[],
        )
        ambiguous = False
        ambiguity_reasons = []
        confidence = 0.7

    context = LinkingContext(
        normalized_question=normalized,
        schema_fingerprint=schema_fingerprint,
        candidates=candidates,
        resolved=resolved,
        ambiguous=ambiguous,
        ambiguity_reasons=ambiguity_reasons,
        confidence=confidence,
        synonym_hits=synonym_hits,
        unresolved_identifiers=_infer_unresolved_identifiers(normalized, resolved, synonyms),
        resolution_status="ambiguous" if ambiguous else "resolved",
        join_grounding_status="pending",
        retrieval_meta=retrieval_meta,
    )
    return SchemaLinkingArtifacts(
        context=context,
        selected_schema_tables=selected_schema,
        selected_examples=selected_examples,
    )
