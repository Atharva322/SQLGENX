from dataclasses import dataclass
import re
from typing import Any

import numpy as np

from src.config.settings import get_settings


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


@dataclass
class RetrievalResult:
    selected_schema_tables: list[dict[str, Any]]
    selected_examples: list[dict[str, Any]]
    retrieval_meta: dict[str, Any]


@dataclass
class RetrievalRanking:
    schema_ranked_indices: list[int]
    example_ranked_indices: list[int]
    schema_method: str
    example_method: str
    schema_avg_score: float
    example_avg_score: float


_model_cache: dict[str, Any | None] = {}


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9_]+", text.lower())
        if token not in STOPWORDS and len(token) > 2
    }


def _lexical_similarity(a: str, b: str) -> float:
    left = _tokenize(a)
    right = _tokenize(b)
    if not left and not right:
        return 0.0
    overlap = len(left.intersection(right))
    union = len(left.union(right))
    return overlap / max(1, union)


def _cosine_similarity(vec_a: np.ndarray, vec_b: np.ndarray) -> float:
    denom = float(np.linalg.norm(vec_a) * np.linalg.norm(vec_b))
    if denom == 0:
        return 0.0
    return float(np.dot(vec_a, vec_b) / denom)


def _load_embedding_model() -> Any | None:
    settings = get_settings()
    model_name = settings.rag_embedding_model.strip()
    if not model_name:
        return None
    if model_name in _model_cache:
        return _model_cache[model_name]
    try:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(
            model_name,
            local_files_only=bool(settings.rag_embedding_local_only),
        )
        _model_cache[model_name] = model
        return model
    except Exception:
        _model_cache[model_name] = None
        return None


def _embed_texts(texts: list[str]) -> np.ndarray | None:
    model = _load_embedding_model()
    if model is None or not texts:
        return None
    try:
        vectors = model.encode(texts, normalize_embeddings=True)
        return np.array(vectors, dtype=float)
    except Exception:
        return None


def _schema_doc(table: dict[str, Any]) -> str:
    table_name = str(table.get("table", ""))
    columns = table.get("columns", [])
    col_blob = ", ".join(
        f"{str(col.get('name', ''))} ({str(col.get('type', ''))})" for col in columns
    )
    return f"Table {table_name}. Columns: {col_blob}."


def _example_doc(example: dict[str, Any]) -> str:
    return f"Question: {example.get('question', '')}\nSQL: {example.get('sql', '')}"


def _rank_by_lexical(question: str, docs: list[str]) -> list[tuple[int, float]]:
    ranked = [(idx, _lexical_similarity(question, doc)) for idx, doc in enumerate(docs)]
    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked


def _rank_by_embedding(question: str, docs: list[str]) -> list[tuple[int, float]] | None:
    vectors = _embed_texts([question] + docs)
    if vectors is None or len(vectors) != len(docs) + 1:
        return None
    q_vec = vectors[0]
    ranked: list[tuple[int, float]] = []
    for idx, doc_vec in enumerate(vectors[1:]):
        ranked.append((idx, _cosine_similarity(q_vec, doc_vec)))
    ranked.sort(key=lambda item: item[1], reverse=True)
    return ranked


def _top_k_indexes(
    question: str, docs: list[str], top_k: int
) -> tuple[list[int], str, float]:
    if not docs or top_k <= 0:
        return [], "none", 0.0
    ranked = _rank_by_embedding(question, docs)
    method = "embedding"
    if ranked is None:
        ranked = _rank_by_lexical(question, docs)
        method = "lexical_fallback"
    selected = ranked[: min(top_k, len(ranked))]
    avg_score = float(sum(score for _, score in selected) / max(1, len(selected)))
    return [idx for idx, _ in selected], method, round(avg_score, 3)


def rank_context_candidates(
    question: str,
    schema: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
) -> RetrievalRanking:
    schema_tables = list(schema.get("tables", []))
    schema_docs = [_schema_doc(table) for table in schema_tables]
    schema_ranked, schema_method, schema_score = _top_k_indexes(
        question, schema_docs, max(1, len(schema_docs))
    )

    example_docs = [_example_doc(example) for example in feedback_examples]
    example_ranked, example_method, example_score = _top_k_indexes(
        question, example_docs, max(1, len(example_docs))
    )

    return RetrievalRanking(
        schema_ranked_indices=schema_ranked,
        example_ranked_indices=example_ranked,
        schema_method=schema_method,
        example_method=example_method,
        schema_avg_score=schema_score,
        example_avg_score=example_score,
    )


def retrieve_context(
    question: str,
    schema: dict[str, Any],
    feedback_examples: list[dict[str, Any]],
    top_k_schema: int,
    top_k_examples: int,
) -> RetrievalResult:
    schema_tables = list(schema.get("tables", []))
    ranking = rank_context_candidates(question, schema, feedback_examples)
    schema_idxs = ranking.schema_ranked_indices[: min(top_k_schema, len(schema_tables))]
    schema_method = ranking.schema_method
    schema_score = ranking.schema_avg_score
    selected_schema = [schema_tables[idx] for idx in schema_idxs] if schema_idxs else schema_tables[:top_k_schema]

    ex_idxs = ranking.example_ranked_indices[: min(top_k_examples, len(feedback_examples))]
    ex_method = ranking.example_method
    ex_score = ranking.example_avg_score
    selected_examples = [feedback_examples[idx] for idx in ex_idxs] if ex_idxs else []

    retrieval_mode = "embedding"
    if "lexical_fallback" in {schema_method, ex_method}:
        retrieval_mode = "lexical_fallback"
    if schema_method == "none" and ex_method == "none":
        retrieval_mode = "none"

    return RetrievalResult(
        selected_schema_tables=selected_schema,
        selected_examples=selected_examples,
        retrieval_meta={
            "mode": retrieval_mode,
            "schema_method": schema_method,
            "schema_avg_score": schema_score,
            "example_method": ex_method,
            "example_avg_score": ex_score,
            "schema_candidates": len(schema_tables),
            "example_candidates": len(feedback_examples),
        },
    )
