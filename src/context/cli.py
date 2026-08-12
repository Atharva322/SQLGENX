from __future__ import annotations

import argparse
import json
from typing import Any

from src.config.settings import get_settings
from src.db.schema_introspector import compute_schema_fingerprint, get_schema_summary
from src.services.prompt_builder import select_relevant_feedback_examples
from src.context.index import ContextIndex


def _index() -> ContextIndex:
    settings = get_settings()
    return ContextIndex(
        path=settings.context_index_path,
        embedding_model=settings.context_index_embedding_model,
        chunking_version=settings.context_index_chunking_version,
        retrieval_config={"rrf_k": settings.context_index_rrf_k},
    )


def _sources(connection_id: str) -> tuple[str, dict[str, Any], list[dict[str, Any]]]:
    schema = get_schema_summary(connection_id=connection_id)
    schema_fingerprint = schema.get("schema_fingerprint") or compute_schema_fingerprint(schema)
    examples = select_relevant_feedback_examples(
        "",
        connection_id=connection_id,
        schema_fingerprint=schema_fingerprint,
        max_examples=1000,
        min_confidence=get_settings().rag_min_feedback_confidence,
    )
    return schema_fingerprint, schema, examples


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Manage the SQLGenX persistent context index.")
    parser.add_argument("command", choices=["build", "status", "refresh", "clear"])
    parser.add_argument("--connection", default="default", help="Connection id to index.")
    args = parser.parse_args(argv)

    index = _index()
    if args.command == "clear":
        index.clear(args.connection)
        print(json.dumps({"connection_id": args.connection, "cleared": True}, sort_keys=True))
        return 0

    schema_fingerprint, schema, examples = _sources(args.connection)
    if args.command == "build":
        status = index.build(args.connection, schema_fingerprint, schema, examples)
    elif args.command == "refresh":
        status = index.refresh_if_stale(args.connection, schema_fingerprint, schema, examples)
    else:
        status = index.status(args.connection, schema_fingerprint, schema, examples)
    print(json.dumps(status.__dict__, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
