from pathlib import Path
import json
import re

from src.db.schema_introspector import compute_schema_fingerprint, get_schema_summary


FEEDBACK_FEWSHOTS_PATH = Path("data") / "feedback_fewshots.jsonl"
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


def filter_relevant_schema(question: str, schema: dict, threshold: float = 0.1) -> dict:
    """Simple lexical relevance filter stub for schema narrowing."""
    q_terms = {t.lower() for t in question.split()}
    relevant_tables = []
    for table in schema.get("tables", []):
        table_terms = {table.get("table", "").lower()}
        table_terms.update(col.get("name", "").lower() for col in table.get("columns", []))
        overlap = len(q_terms.intersection(table_terms))
        score = overlap / max(1, len(q_terms))
        if score >= threshold:
            relevant_tables.append(table)

    if not relevant_tables:
        relevant_tables = schema.get("tables", [])[:5]

    return {"tables": relevant_tables}


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

    for example in _load_feedback_examples():
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
    schema = get_schema_summary(connection_id=connection_id)
    filtered = filter_relevant_schema(question, schema)
    schema_lines: list[str] = []
    for table in filtered.get("tables", []):
        table_name = table.get("table", "")
        columns = table.get("columns", [])
        col_blob = ", ".join(
            f"{column.get('name')} ({column.get('type')})" for column in columns
        )
        schema_lines.append(f"- {table_name}: {col_blob}")

    schema_text = "\n".join(schema_lines) if schema_lines else "- no schema available"
    schema_fingerprint = schema.get("schema_fingerprint")
    if not schema_fingerprint:
        schema_fingerprint = compute_schema_fingerprint(schema)

    fewshots = select_relevant_feedback_examples(
        question,
        connection_id=connection_id,
        schema_fingerprint=schema_fingerprint,
    )
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
        f"Question: {question}\n\n"
        "Relevant schema:\n"
        f"{schema_text}\n"
        f"{fewshot_text}"
    )
