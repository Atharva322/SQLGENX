from src.db.schema_introspector import get_schema_summary


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

    return (
        "You are a SQL assistant. Generate safe read-only SQL only.\n"
        "Rules:\n"
        "1) Use ONLY the table and column names listed in the schema section.\n"
        "2) If required table/column is not present, return SQL exactly as UNANSWERABLE.\n"
        "3) If the user question is ambiguous (multiple meanings), return SQL exactly as UNANSWERABLE.\n"
        "4) Never fabricate columns (for example sales_amount if only amount exists).\n"
        "5) Only output SELECT/WITH SQL when answerable.\n\n"
        f"Question: {question}\n\n"
        "Relevant schema:\n"
        f"{schema_text}\n"
    )
