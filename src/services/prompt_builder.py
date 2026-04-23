from src.db.schema_introspector import get_schema_summary


def filter_relevant_schema(question: str, schema: dict, threshold: float = 0.1) -> dict:
    """Simple lexical relevance filter stub for schema narrowing."""
    q_terms = {t.lower() for t in question.split()}
    relevant_tables = []
    for table in schema.get('tables', []):
        table_terms = {table.get('table', '').lower()}
        table_terms.update(col.get('name', '').lower() for col in table.get('columns', []))
        overlap = len(q_terms.intersection(table_terms))
        score = overlap / max(1, len(q_terms))
        if score >= threshold:
            relevant_tables.append(table)

    if not relevant_tables:
        relevant_tables = schema.get('tables', [])[:5]

    return {'tables': relevant_tables}


def build_prompt(question: str) -> str:
    schema = get_schema_summary()
    filtered = filter_relevant_schema(question, schema)
    return (
        'You are a SQL assistant. Generate safe read-only SQL.\\n'
        f'Question: {question}\\n'
        f'Relevant schema: {filtered}\\n'
        'Return SQL and explanation.'
    )
