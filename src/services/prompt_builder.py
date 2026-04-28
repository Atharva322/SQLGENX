from pathlib import Path
import json
import re

from src.config.settings import get_settings
from src.db.schema_introspector import compute_schema_fingerprint, get_schema_summary
from src.services.rag_retriever import retrieve_context


FEEDBACK_FEWSHOTS_PATH = Path("data") / "feedback_fewshots.jsonl"
PROMPT_ASSETS_PATH = Path("data") / "prompt_assets.json"
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


def _tokenize(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-zA-Z0-9_]+", text.lower())
        if token not in STOPWORDS and len(token) > 2
    }


def _load_feedback_examples() -> list[dict]:
    if not FEEDBACK_FEWSHOTS_PATH.exists():
        return []
    examples: list[dict] = []
    with FEEDBACK_FEWSHOTS_PATH.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if payload.get("verdict") != "correct":
                continue
            sql = str(payload.get("sql", "")).strip()
            question = str(payload.get("question", "")).strip()
            if not sql or not question:
                continue
            confidence = float(payload.get("confidence", 0.0))
            examples.append(
                {
                    "question": question,
                    "sql": sql,
                    "confidence": confidence,
                    "connection_id": payload.get("connection_id"),
                    "schema_fingerprint": payload.get("schema_fingerprint"),
                }
            )
    return examples


def _load_prompt_assets_examples() -> list[dict]:
    if not PROMPT_ASSETS_PATH.exists():
        return []
    try:
        payload = json.loads(PROMPT_ASSETS_PATH.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return []
    examples = payload.get("few_shot_examples", [])
    normalized: list[dict] = []
    for example in examples:
        question = str(example.get("question", "")).strip()
        sql = str(example.get("sql", "")).strip()
        if question and sql:
            normalized.append(
                {
                    "question": question,
                    "sql": sql,
                    "confidence": 0.95,
                    "connection_id": "default",
                    "schema_fingerprint": None,
                    "source": "prompt_assets",
                }
            )
    return normalized


def select_relevant_feedback_examples(
    question: str,
    connection_id: str | None = None,
    schema_fingerprint: str | None = None,
    max_examples: int = 3,
    min_confidence: float = 0.65,
) -> list[dict]:
    target_tokens = _tokenize(question)
    candidates: list[tuple[int, float, float, dict]] = []
    resolved_connection_id = connection_id or "default"

    all_examples = _load_prompt_assets_examples() + _load_feedback_examples()
    for example in all_examples:
        conf = float(example.get("confidence", 0.0))
        if conf < min_confidence:
            continue

        example_connection_id = example.get("connection_id")
        if example_connection_id and example_connection_id != resolved_connection_id:
            continue

        example_fingerprint = example.get("schema_fingerprint")
        if schema_fingerprint and example_fingerprint and example_fingerprint != schema_fingerprint:
            continue

        source_tokens = _tokenize(example["question"])
        overlap = len(target_tokens.intersection(source_tokens))
        union = len(target_tokens.union(source_tokens))
        relevance = overlap / max(1, union)
        if relevance <= 0:
            continue

        scope_rank = 0
        if example_connection_id == resolved_connection_id:
            scope_rank = 1
        if (
            scope_rank == 1
            and schema_fingerprint
            and example_fingerprint
            and example_fingerprint == schema_fingerprint
        ):
            scope_rank = 2

        candidates.append((scope_rank, relevance, conf, example))

    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return [entry[3] for entry in candidates[:max_examples]]


def build_prompt(question: str, connection_id: str | None = None) -> str:
    settings = get_settings()
    schema = get_schema_summary(connection_id=connection_id)
    all_tables = schema.get("tables", [])
    schema_lines: list[str] = []
    schema_fingerprint = schema.get("schema_fingerprint")
    if not schema_fingerprint:
        schema_fingerprint = compute_schema_fingerprint(schema)

    scoped_feedback = select_relevant_feedback_examples(
        question,
        connection_id=connection_id,
        schema_fingerprint=schema_fingerprint,
        max_examples=max(5, settings.rag_top_k_examples),
        min_confidence=settings.rag_min_feedback_confidence,
    )

    retrieval_meta: dict[str, str | int | float] = {
        "mode": "disabled",
        "schema_method": "none",
        "example_method": "none",
    }
    selected_tables = all_tables[: settings.rag_top_k_schema]
    selected_examples = scoped_feedback[: settings.rag_top_k_examples]
    if settings.rag_enabled:
        rag = retrieve_context(
            question=question,
            schema=schema,
            feedback_examples=scoped_feedback,
            top_k_schema=settings.rag_top_k_schema,
            top_k_examples=settings.rag_top_k_examples,
        )
        selected_tables = rag.selected_schema_tables
        selected_examples = rag.selected_examples
        retrieval_meta = rag.retrieval_meta

    for table in selected_tables:
        table_name = table.get("table", "")
        columns = table.get("columns", [])
        col_blob = ", ".join(
            f"{column.get('name')} ({column.get('type')})" for column in columns
        )
        schema_lines.append(f"- {table_name}: {col_blob}")

    schema_text = "\n".join(schema_lines) if schema_lines else "- no schema available"
    fewshots = selected_examples
    fewshot_text = ""
    if fewshots:
        fewshot_lines = []
        for shot in fewshots:
            fewshot_lines.append(f"Q: {shot['question']}")
            fewshot_lines.append(f"SQL: {shot['sql']}")
        fewshot_text = (
            "\nFew-shot examples from verified user feedback:\n"
            + "\n".join(fewshot_lines)
            + "\n"
        )

    return (
        "You are a SQL assistant. Generate safe read-only SQL only.\n"
        "Rules:\n"
        "1) Use ONLY the table and column names listed in the schema section.\n"
        "2) If required table/column is not present, return SQL exactly as UNANSWERABLE.\n"
        "3) If the user question has multiple valid business interpretations, return SQL exactly as UNANSWERABLE.\n"
        "4) Never fabricate columns (for example sales_amount if only amount exists).\n"
        "5) Exception to rule 3: if a requested grouping label is unavailable but an unambiguous surrogate"
        " key exists (such as *_id), group by that key instead of returning UNANSWERABLE.\n"
        "6) Only output SELECT/WITH SQL when answerable.\n\n"
        "RAG retrieval metadata:\n"
        f"- mode: {retrieval_meta.get('mode', 'none')}\n"
        f"- schema retrieval: {retrieval_meta.get('schema_method', 'none')}\n"
        f"- example retrieval: {retrieval_meta.get('example_method', 'none')}\n\n"
        f"Question: {question}\n\n"
        "Relevant schema:\n"
        f"{schema_text}\n"
        f"{fewshot_text}"
    )
