from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


DEFAULT_QUESTIONS = [
    "Show total revenue by region",
    "Bookings by territory",
    "Average salary by department",
    "Show total revenue by department",
    "List employees in Engineering",
]


def _configure_defaults() -> None:
    os.environ.setdefault(
        "DATABASE_URL",
        "postgresql://text2sql_user:text2sql_pass@localhost:55432/sample_company",
    )
    os.environ.setdefault("CONNECTION_URLS_JSON", "{}")
    os.environ.setdefault("LLM_PROVIDER", "deterministic")
    os.environ.setdefault("LLM_MODEL", "sqlgenx-fixture-v1")
    os.environ.setdefault("RAG_EMBEDDING_LOCAL_ONLY", "true")
    os.environ.setdefault("CONTEXT_INDEX_ENABLED", "true")
    os.environ.setdefault("ADAPTIVE_VALIDATION_ENABLED", "true")
    os.environ.setdefault("ADAPTIVE_VALIDATION_MODE", "adaptive")
    os.environ.setdefault("AST_VALIDATION_ENABLED", "true")
    os.environ.setdefault("ASYNC_RUNTIME_ENABLED", "true")
    os.environ.setdefault("SEMANTIC_LAYER_ENABLED", "true")


def _summarize_response(question: str, response: Any) -> dict[str, Any]:
    semantic_layer = response.execution_meta.semantic_layer or {}
    metric = semantic_layer.get("metric") or {}
    return {
        "question": question,
        "strategy": response.reasoning.strategy,
        "selected_candidate": response.reasoning.selected_candidate,
        "semantic_metric": metric.get("id"),
        "semantic_version": metric.get("version"),
        "sql": response.sql,
        "row_count": len(response.results),
        "llm_call_count": response.execution_meta.llm_call_count,
        "warnings": response.warnings,
        "sample_rows": response.results[:3],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run the Phase 6 semantic-layer demo.")
    parser.add_argument("questions", nargs="*", help="Optional questions to run instead of the demo script.")
    args = parser.parse_args()

    _configure_defaults()
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

    from loguru import logger
    from src.config.settings import get_settings
    from src.services.query_service import QueryService

    logger.remove()
    get_settings.cache_clear()
    service = QueryService()
    questions = args.questions or DEFAULT_QUESTIONS

    for question in questions:
        response = service.process_question(
            question,
            connection_id="default",
            session_id="phase6_semantic_demo",
        )
        print(json.dumps(_summarize_response(question, response), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
