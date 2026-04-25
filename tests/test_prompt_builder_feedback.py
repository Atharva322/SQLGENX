import json
from pathlib import Path

from src.services import prompt_builder


def test_prompt_includes_relevant_feedback_fewshot(monkeypatch) -> None:
    feedback_file = Path("tests") / "feedback_fewshots_test.jsonl"
    rows = [
        {
            "verdict": "correct",
            "question": "Show total sales by region",
            "sql": "SELECT region, SUM(amount) AS total_sales FROM sales GROUP BY region",
            "confidence": 0.9,
        },
        {
            "verdict": "correct",
            "question": "List employee titles",
            "sql": "SELECT title FROM employees",
            "confidence": 0.95,
        },
    ]
    feedback_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    monkeypatch.setattr(prompt_builder, "FEEDBACK_FEWSHOTS_PATH", feedback_file)
    monkeypatch.setattr(
        prompt_builder,
        "get_schema_summary",
        lambda connection_id=None: {
            "tables": [
                {
                    "table": "sales",
                    "columns": [{"name": "amount", "type": "NUMERIC"}],
                }
            ]
        },
    )

    try:
        prompt = prompt_builder.build_prompt("Show total sales by region")
        assert "Few-shot examples from verified user feedback" in prompt
        assert "Q: Show total sales by region" in prompt
        assert "SQL: SELECT region, SUM(amount) AS total_sales FROM sales GROUP BY region" in prompt
    finally:
        if feedback_file.exists():
            feedback_file.unlink()


def test_feedback_fewshots_are_connection_scoped(monkeypatch) -> None:
    feedback_file = Path("tests") / "feedback_fewshots_scope_test.jsonl"
    rows = [
        {
            "verdict": "correct",
            "connection_id": "default",
            "schema_fingerprint": "fp_default",
            "question": "Show total sales by region",
            "sql": "SELECT region, SUM(amount) AS total_sales FROM sales GROUP BY region",
            "confidence": 0.92,
        },
        {
            "verdict": "correct",
            "connection_id": "analytics",
            "schema_fingerprint": "fp_analytics",
            "question": "Show total sales by region",
            "sql": "SELECT region_name, SUM(gross_amount) AS total_sales FROM fact_sales GROUP BY region_name",
            "confidence": 0.94,
        },
    ]
    feedback_file.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    monkeypatch.setattr(prompt_builder, "FEEDBACK_FEWSHOTS_PATH", feedback_file)
    monkeypatch.setattr(
        prompt_builder,
        "get_schema_summary",
        lambda connection_id=None: {
            "tables": [
                {
                    "table": "sales",
                    "columns": [{"name": "amount", "type": "NUMERIC"}],
                }
            ],
            "schema_fingerprint": "fp_default" if connection_id == "default" else "fp_analytics",
        },
    )

    try:
        prompt = prompt_builder.build_prompt("Show total sales by region", connection_id="default")
        assert "SQL: SELECT region, SUM(amount) AS total_sales FROM sales GROUP BY region" in prompt
        assert "fact_sales" not in prompt
    finally:
        if feedback_file.exists():
            feedback_file.unlink()
