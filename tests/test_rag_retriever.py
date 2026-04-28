from src.services import rag_retriever


def test_retrieve_context_lexical_fallback(monkeypatch) -> None:
    monkeypatch.setattr(rag_retriever, "_embed_texts", lambda texts: None)
    schema = {
        "tables": [
            {"table": "sales", "columns": [{"name": "amount", "type": "NUMERIC"}]},
            {"table": "employees", "columns": [{"name": "salary", "type": "NUMERIC"}]},
        ]
    }
    feedback = [
        {
            "question": "Show total sales by region",
            "sql": "SELECT region, SUM(amount) FROM sales GROUP BY region",
            "confidence": 0.9,
        },
        {
            "question": "List employees with salaries",
            "sql": "SELECT salary FROM employees",
            "confidence": 0.9,
        },
    ]

    result = rag_retriever.retrieve_context(
        question="Total sales by region",
        schema=schema,
        feedback_examples=feedback,
        top_k_schema=1,
        top_k_examples=1,
    )

    assert result.retrieval_meta["mode"] == "lexical_fallback"
    assert result.selected_schema_tables[0]["table"] == "sales"
    assert "sales" in result.selected_examples[0]["sql"].lower()
