from pathlib import Path
import json
import re

from src.config.settings import get_settings
from src.db.schema_introspector import compute_schema_fingerprint, get_schema_summary
from src.models.schemas import LinkingContext, QueryPlanDraft
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


def _build_query_plan_draft(
    question: str,
    linking_context: LinkingContext | None,
    selected_tables: list[dict],
) -> QueryPlanDraft:
    lowered = question.lower()
    target_tables = list(linking_context.resolved.tables) if linking_context else []
    target_columns = list(linking_context.resolved.columns) if linking_context else []
    grouping: list[str] = []
    aggregations: list[str] = []
    filters: list[str] = []
    join_path = list(linking_context.resolved.join_hints) if linking_context else []
    notes: list[str] = []

    if " by " in lowered:
        for column in target_columns:
            col_name = column.split(".")[-1].lower()
            if col_name in lowered:
                grouping.append(column)
    if any(token in lowered for token in {"total", "sum", "revenue"}):
        aggregations.append("SUM")
    if any(token in lowered for token in {"average", "avg", "mean"}):
        aggregations.append("AVG")
    if any(token in lowered for token in {"count", "number of", "how many"}):
        aggregations.append("COUNT")
    if any(token in lowered for token in {"top", "rank", "highest", "lowest"}):
        notes.append("Use ORDER BY with LIMIT for ranking intent.")
    if any(token in lowered for token in {"last_", "current_", "today", "month", "quarter", "year"}):
        filters.append("Apply time filter if matching date/timestamp column exists.")
    if len(target_tables) > 1 and not join_path:
        join_path = [table.get("table", "") for table in selected_tables if table.get("table")]
        notes.append("Prefer FK-grounded joins across selected tables only.")
    if linking_context and linking_context.unresolved_identifiers:
        notes.append(
            f"Unresolved business terms remain: {', '.join(linking_context.unresolved_identifiers[:4])}."
        )

    return QueryPlanDraft(
        intent="select",
        target_tables=target_tables,
        target_columns=target_columns,
        grouping=sorted(set(grouping)),
        aggregations=sorted(set(aggregations)),
        filters=filters,
        join_path=join_path,
        notes=notes,
    )


def build_query_plan_draft(
    question: str,
    linking_context: LinkingContext | None,
    selected_tables: list[dict],
) -> QueryPlanDraft:
    return _build_query_plan_draft(question, linking_context, selected_tables)


def build_prompt(
    question: str,
    connection_id: str | None = None,
    linking_context: LinkingContext | None = None,
    selected_tables_override: list[dict] | None = None,
    selected_examples_override: list[dict] | None = None,
    query_plan_override: QueryPlanDraft | None = None,
    include_query_plan_draft: bool = True,
) -> str:
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
    selected_tables = selected_tables_override or all_tables[: settings.rag_top_k_schema]
    selected_examples = selected_examples_override or scoped_feedback[: settings.rag_top_k_examples]
    if settings.rag_enabled and not selected_tables_override and not selected_examples_override:
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

    if linking_context:
        selected_table_names = set(linking_context.resolved.tables)
        selected_tables = [t for t in selected_tables if t.get("table") in selected_table_names] or selected_tables

    for table in selected_tables:
        table_name = table.get("table", "")
        columns = table.get("columns", [])
        col_blob = ", ".join(f"{column.get('name')} ({column.get('type')})" for column in columns)
        fk_blob = ", ".join(
            f"{fk.get('constrained_columns', [])}->{fk.get('referred_table')}.{fk.get('referred_columns', [])}"
            for fk in table.get("foreign_keys", [])
        )
        schema_lines.append(
            f"- table={table_name} | columns=[{col_blob}] | fk_hints=[{fk_blob or 'none'}] | business_desc=core domain entity"
        )

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

    linking_text = ""
    query_plan_text = ""
    if linking_context:
        plan = query_plan_override or _build_query_plan_draft(question, linking_context, selected_tables)
        linking_text = (
            "\nSchema linking context:\n"
            f"- normalized_question: {linking_context.normalized_question}\n"
            f"- linker_confidence: {linking_context.confidence}\n"
            f"- ambiguous: {linking_context.ambiguous}\n"
            f"- ambiguity_reasons: {', '.join(linking_context.ambiguity_reasons) or 'none'}\n"
            f"- allowed_tables: {', '.join(linking_context.resolved.tables) or 'none'}\n"
            f"- allowed_columns: {', '.join(linking_context.resolved.columns) or 'none'}\n"
            f"- synonym_hits: {', '.join(linking_context.synonym_hits) or 'none'}\n"
            f"- join_hints: {', '.join(linking_context.resolved.join_hints) or 'none'}\n"
        )
        if include_query_plan_draft:
            query_plan_text = (
                "\nQuery plan draft:\n"
                f"- intent: {plan.intent}\n"
                f"- target_tables: {', '.join(plan.target_tables) or 'none'}\n"
                f"- target_columns: {', '.join(plan.target_columns) or 'none'}\n"
                f"- grouping: {', '.join(plan.grouping) or 'none'}\n"
                f"- aggregations: {', '.join(plan.aggregations) or 'none'}\n"
                f"- filters: {', '.join(plan.filters) or 'none'}\n"
                f"- join_path: {', '.join(plan.join_path) or 'none'}\n"
                f"- notes: {', '.join(plan.notes) or 'none'}\n"
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
        "7) Treat schema linking allowed_tables/allowed_columns as hard constraints.\n\n"
        "8) Follow the query plan draft unless it conflicts with schema reality; never invent missing joins or metrics.\n\n"
        "RAG retrieval metadata:\n"
        f"- mode: {retrieval_meta.get('mode', 'none')}\n"
        f"- schema retrieval: {retrieval_meta.get('schema_method', 'none')}\n"
        f"- example retrieval: {retrieval_meta.get('example_method', 'none')}\n\n"
        f"{linking_text}\n"
        f"{query_plan_text}\n"
        f"Question: {question}\n\n"
        "Relevant schema:\n"
        f"{schema_text}\n"
        f"{fewshot_text}"
    )
