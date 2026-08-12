from __future__ import annotations

import argparse
import json
from pathlib import Path

from src.semantic.layer import SemanticLayer
from src.semantic.loader import DEFAULT_SEMANTIC_LAYER_PATH, load_semantic_layer


def lint(path: Path) -> dict:
    definition = load_semantic_layer(path)
    return {
        "valid": True,
        "semantic_layer_id": definition.id,
        "version": definition.version,
        "metric_count": len(definition.metrics),
        "dimension_count": len(definition.dimensions),
        "approved_join_count": len(definition.approved_joins),
    }


def run_definition_tests(path: Path) -> dict:
    layer = SemanticLayer(load_semantic_layer(path))
    schema = _definition_schema(layer)
    failures = []
    total = 0
    for metric in layer.definition.metrics:
        for case in metric.tests:
            total += 1
            result = layer.compile_question(case.question, schema=schema)
            if not result.matched or result.unsupported or result.sql != case.expected_sql:
                failures.append(
                    {
                        "metric_id": metric.id,
                        "question": case.question,
                        "expected_sql": case.expected_sql,
                        "actual_sql": result.sql,
                        "reason": result.reason,
                    }
                )
    if failures:
        raise SystemExit(json.dumps({"valid": False, "failures": failures}, indent=2))
    return {"valid": True, "test_count": total}


def _definition_schema(layer: SemanticLayer) -> dict:
    tables = [
        {"table": entity.table, "columns": [{"name": entity.primary_key}]}
        for entity in layer.definition.entities
    ]
    return {"tables": tables}


def main() -> None:
    parser = argparse.ArgumentParser(description="Lint and test SQLGenX semantic layer YAML.")
    parser.add_argument("command", choices=["lint", "test"])
    parser.add_argument("--path", type=Path, default=DEFAULT_SEMANTIC_LAYER_PATH)
    args = parser.parse_args()
    if args.command == "lint":
        result = lint(args.path)
    else:
        result = run_definition_tests(args.path)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
