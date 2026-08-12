from __future__ import annotations

import re

import sqlglot
from sqlglot import exp

from src.semantic.models import SemanticLayerDefinition


POSTGRES_DIALECT = "postgres"
ALLOWED_AGGREGATES = {"SUM", "AVG", "COUNT", "MIN", "MAX"}


def validate_semantic_layer(definition: SemanticLayerDefinition) -> None:
    _validate_expressions(definition)
    _validate_join_graph(definition)


def _validate_expressions(definition: SemanticLayerDefinition) -> None:
    for metric in definition.metrics:
        _validate_metric_expression(metric.id, metric.expression)
    for dimension in definition.dimensions:
        _validate_column_expression("dimension", dimension.id, dimension.expression)
    for filter_def in definition.filters:
        _validate_filter_expression(filter_def.id, filter_def.expression)


def _validate_metric_expression(metric_id: str, expression: str) -> None:
    try:
        parsed = sqlglot.parse_one(f"SELECT {expression} AS metric_value", read=POSTGRES_DIALECT)
    except Exception as exc:
        raise ValueError(f"metric {metric_id} expression does not parse") from exc
    selected = list(parsed.expressions)
    if len(selected) != 1:
        raise ValueError(f"metric {metric_id} must contain exactly one expression")
    functions = list(selected[0].find_all(exp.AggFunc))
    if len(functions) != 1 or functions[0].key.upper() not in ALLOWED_AGGREGATES:
        raise ValueError(f"metric {metric_id} must use one approved aggregate")
    extra_functions = [
        node for node in selected[0].find_all(exp.Func) if not isinstance(node, exp.AggFunc)
    ]
    if extra_functions:
        raise ValueError(f"metric {metric_id} may not use arbitrary functions")
    _reject_arbitrary_sql(metric_id, selected[0])


def _validate_column_expression(kind: str, item_id: str, expression: str) -> None:
    if not re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", expression):
        raise ValueError(f"{kind} {item_id} must be a qualified column expression")


def _validate_filter_expression(filter_id: str, expression: str) -> None:
    try:
        parsed = sqlglot.parse_one(f"SELECT 1 WHERE {expression}", read=POSTGRES_DIALECT)
    except Exception as exc:
        raise ValueError(f"filter {filter_id} expression does not parse") from exc
    where = parsed.args.get("where")
    if where is None:
        raise ValueError(f"filter {filter_id} must contain a predicate")
    _reject_arbitrary_sql(filter_id, where)
    if list(where.find_all(exp.Or, exp.Subquery, exp.Select, exp.Union)):
        raise ValueError(f"filter {filter_id} may not contain ORs or nested SQL")


def _reject_arbitrary_sql(item_id: str, expression: exp.Expression) -> None:
    blocked = (exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Command, exp.Subquery)
    if list(expression.find_all(*blocked)):
        raise ValueError(f"semantic expression {item_id} contains blocked SQL nodes")


def _validate_join_graph(definition: SemanticLayerDefinition) -> None:
    directed = {join.left_table: join.right_table for join in definition.approved_joins}
    for start in directed:
        visited: set[str] = set()
        current = start
        while current in directed:
            if current in visited:
                raise ValueError("approved join graph contains a directed cycle")
            visited.add(current)
            current = directed[current]

    for metric in definition.metrics:
        for dimension_id in metric.allowed_dimensions:
            dimension = next(item for item in definition.dimensions if item.id == dimension_id)
            _assert_safe_path(definition, metric.source, dimension.table, metric.id, dimension.id)
        for filter_id in metric.allowed_filters:
            filter_def = next(item for item in definition.filters if item.id == filter_id)
            _assert_safe_path(definition, metric.source, filter_def.table, metric.id, filter_id)


def _assert_safe_path(
    definition: SemanticLayerDefinition,
    source_table: str,
    target_table: str,
    metric_id: str,
    target_id: str,
) -> None:
    if source_table == target_table:
        return
    path = shortest_join_path(definition, source_table, target_table)
    if not path:
        raise ValueError(f"metric {metric_id} cannot reach {target_id} through approved joins")
    for join in path:
        if join.relationship == "one_to_many":
            raise ValueError(f"metric {metric_id} path to {target_id} crosses fanout join {join.id}")


def shortest_join_path(definition: SemanticLayerDefinition, source: str, target: str):
    queue: list[tuple[str, list]] = [(source, [])]
    visited = {source}
    while queue:
        table, path = queue.pop(0)
        if table == target:
            return path
        for join in definition.approved_joins:
            next_table = None
            if join.left_table == table:
                next_table = join.right_table
                next_join = join
            elif join.right_table == table:
                next_table = join.left_table
                next_join = join
            else:
                continue
            if next_table in visited:
                continue
            visited.add(next_table)
            queue.append((next_table, path + [next_join]))
    return []
