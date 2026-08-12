from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
from typing import Any

import sqlglot
from sqlglot import exp
from sqlglot.optimizer.scope import Scope, traverse_scope


POSTGRES_DIALECT = "postgres"


@dataclass(frozen=True)
class AstSqlAnalysis:
    valid: bool
    normalized_sql: str = ""
    fingerprint: str = ""
    tables: list[str] = field(default_factory=list)
    aliases: dict[str, str] = field(default_factory=dict)
    columns: list[str] = field(default_factory=list)
    joins: list[str] = field(default_factory=list)
    predicates: list[str] = field(default_factory=list)
    groupings: list[str] = field(default_factory=list)
    aggregations: list[str] = field(default_factory=list)
    orderings: list[str] = field(default_factory=list)
    set_operations: list[str] = field(default_factory=list)
    ctes: list[str] = field(default_factory=list)
    subquery_depth: int = 0
    reason_codes: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)


def analyze_sql_ast(
    sql: str,
    schema: dict[str, Any],
    *,
    allow_select_star: bool = True,
) -> AstSqlAnalysis:
    try:
        statements = [stmt for stmt in sqlglot.parse(sql, read=POSTGRES_DIALECT) if stmt]
    except sqlglot.errors.ParseError as exc:
        return _invalid("parse_failed", f"AST parser failed closed: {exc}")
    except Exception as exc:
        return _invalid("parse_failed", f"AST parser failed closed: {type(exc).__name__}: {exc}")

    if len(statements) != 1:
        return _invalid("statement_count", "AST validation requires exactly one SQL statement.")

    root = statements[0]
    if not isinstance(root, (exp.Select, exp.Union, exp.Intersect, exp.Except)):
        return _invalid("non_readonly_root", "AST validation allows only read-only SELECT/WITH roots.")
    if list(root.find_all(exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create)):
        return _invalid("writable_ast_node", "AST validation found a writable SQL node.")

    normalized = root.sql(dialect=POSTGRES_DIALECT, pretty=False, normalize=True)
    schema_index = _schema_index(schema)
    cte_outputs = _cte_outputs(root)
    table_refs = _table_references(root, cte_outputs)
    aliases = {alias: table for table, alias in table_refs if alias}
    table_names = sorted({table for table, _ in table_refs if table not in cte_outputs})
    warnings: list[str] = []
    reasons: list[str] = []

    for table in table_names:
        if table not in schema_index:
            reasons.append(f"unknown_table:{table}")

    columns = _column_references(root)
    reasons.extend(_schema_validation_reasons(root, schema_index, cte_outputs, allow_select_star))

    warnings.extend(f"AST validation blocked {reason}." for reason in sorted(set(reasons)))
    set_ops = _set_operations(root)
    aggregations = _aggregations(root)
    predicates = [node.sql(dialect=POSTGRES_DIALECT, pretty=False) for node in root.find_all(exp.Where)]
    predicates.extend(node.sql(dialect=POSTGRES_DIALECT, pretty=False) for node in root.find_all(exp.Having))
    orderings = [
        node.sql(dialect=POSTGRES_DIALECT, pretty=False)
        for order in root.find_all(exp.Order)
        for node in order.expressions
    ]
    groupings = [
        node.sql(dialect=POSTGRES_DIALECT, pretty=False)
        for group in root.find_all(exp.Group)
        for node in group.expressions
    ]

    return AstSqlAnalysis(
        valid=not reasons,
        normalized_sql=normalized,
        fingerprint=sql_fingerprint(normalized),
        tables=table_names,
        aliases=aliases,
        columns=columns,
        joins=[
            join.this.sql(dialect=POSTGRES_DIALECT, pretty=False)
            for join in root.find_all(exp.Join)
            if join.this is not None
        ],
        predicates=predicates,
        groupings=groupings,
        aggregations=aggregations,
        orderings=orderings,
        set_operations=set_ops,
        ctes=sorted(cte_outputs),
        subquery_depth=_subquery_depth(root),
        reason_codes=sorted(set(reasons)),
        warnings=warnings,
    )


def sql_fingerprint(normalized_sql: str) -> str:
    return hashlib.sha256(normalized_sql.encode("utf-8")).hexdigest()[:16]


def are_semantically_duplicate(sql_a: str, sql_b: str) -> bool:
    try:
        a = sqlglot.parse_one(sql_a, read=POSTGRES_DIALECT).sql(
            dialect=POSTGRES_DIALECT, pretty=False, normalize=True
        )
        b = sqlglot.parse_one(sql_b, read=POSTGRES_DIALECT).sql(
            dialect=POSTGRES_DIALECT, pretty=False, normalize=True
        )
    except Exception:
        return False
    return sql_fingerprint(a) == sql_fingerprint(b)


def _invalid(code: str, warning: str) -> AstSqlAnalysis:
    return AstSqlAnalysis(valid=False, reason_codes=[code], warnings=[warning])


def _schema_index(schema: dict[str, Any]) -> dict[str, set[str]]:
    index: dict[str, set[str]] = {}
    for table in schema.get("tables", []):
        table_name = _normalize_name(str(table.get("table", "")))
        if not table_name:
            continue
        columns = {
            _normalize_name(str(column.get("name", "")))
            for column in table.get("columns", [])
            if column.get("name")
        }
        index[table_name] = columns
    return index


def _normalize_name(value: str) -> str:
    return value.strip().strip('"').strip("`").split(".")[-1].lower()


def _table_references(root: exp.Expression, cte_outputs: dict[str, set[str]]) -> list[tuple[str, str]]:
    refs: list[tuple[str, str]] = []
    for table in root.find_all(exp.Table):
        name = _normalize_name(table.name)
        if not name:
            continue
        # CTE aliases are not physical tables and should not be schema-validated.
        alias = _normalize_name(table.alias_or_name) if table.alias else ""
        refs.append((name, alias))
    return refs


def _column_references(root: exp.Expression) -> list[str]:
    columns: set[str] = set()
    for column in root.find_all(exp.Column):
        parts = [_normalize_name(part.name if hasattr(part, "name") else str(part)) for part in column.parts]
        parts = [part for part in parts if part]
        if len(parts) >= 2:
            columns.add(f"{parts[-2]}.{parts[-1]}")
        elif parts:
            columns.add(parts[-1])
    for star in root.find_all(exp.Star):
        parent = star.parent
        if isinstance(parent, exp.Column) and parent.table:
            columns.add(f"{_normalize_name(parent.table)}.*")
        else:
            columns.add("*")
    return sorted(columns)


def _split_column(column: str) -> tuple[str | None, str]:
    if "." in column:
        table, col = column.rsplit(".", 1)
        return table, col
    return None, column


def _schema_validation_reasons(
    root: exp.Expression,
    schema_index: dict[str, set[str]],
    cte_outputs: dict[str, set[str]],
    allow_select_star: bool,
) -> list[str]:
    reasons: list[str] = []
    for scope in traverse_scope(root):
        source_columns = _scope_source_columns(scope, schema_index, cte_outputs)
        source_names = set(source_columns)

        for column in scope.columns:
            table_part = _normalize_name(str(column.table or ""))
            column_part = _normalize_name(column.name)
            if not column_part:
                continue
            if column_part == "*":
                if not allow_select_star:
                    star_name = f"{table_part}.*" if table_part else "*"
                    reasons.append(f"star_not_allowed:{star_name}")
                continue
            if table_part:
                if table_part not in source_names:
                    reasons.append(f"unknown_table_alias:{table_part}")
                elif column_part not in source_columns[table_part]:
                    reasons.append(f"unknown_column:{table_part}.{column_part}")
                continue

            containing = [
                source
                for source, available_columns in source_columns.items()
                if column_part in available_columns
            ]
            if not containing:
                reasons.append(f"unknown_column:{column_part}")
            elif len(set(containing)) > 1:
                reasons.append(f"ambiguous_unqualified_column:{column_part}")

    if not allow_select_star:
        for star in root.find_all(exp.Star):
            table_part = ""
            if isinstance(star.parent, exp.Column):
                table_part = _normalize_name(str(star.parent.table or ""))
            star_name = f"{table_part}.*" if table_part else "*"
            reasons.append(f"star_not_allowed:{star_name}")
    return reasons


def _scope_source_columns(
    scope: Scope,
    schema_index: dict[str, set[str]],
    cte_outputs: dict[str, set[str]],
) -> dict[str, set[str]]:
    columns: dict[str, set[str]] = {}
    for source_name, (_, source) in scope.selected_sources.items():
        normalized_source = _normalize_name(source_name)
        if isinstance(source, exp.Table):
            table_name = _normalize_name(source.name)
            if table_name in cte_outputs:
                columns[normalized_source] = set(cte_outputs[table_name])
            else:
                columns[normalized_source] = set(schema_index.get(table_name, set()))
        elif isinstance(source, Scope):
            columns[normalized_source] = _select_output_aliases(source.expression)
    return columns


def _select_output_aliases(expression: exp.Expression) -> set[str]:
    if not isinstance(expression, exp.Select):
        return set()
    aliases: set[str] = set()
    for selected in expression.expressions:
        alias = selected.alias_or_name
        if alias:
            aliases.add(_normalize_name(alias))
    return aliases


def _cte_outputs(root: exp.Expression) -> dict[str, set[str]]:
    outputs: dict[str, set[str]] = {}
    for cte in root.find_all(exp.CTE):
        name = _normalize_name(cte.alias)
        if not name:
            continue
        table_alias = cte.args.get("alias")
        explicit = set()
        if isinstance(table_alias, exp.TableAlias):
            explicit = {
                _normalize_name(column.name)
                for column in table_alias.args.get("columns") or []
                if getattr(column, "name", "")
            }
        selected = set()
        if isinstance(cte.this, exp.Select):
            for expression in cte.this.expressions:
                alias = expression.alias_or_name
                if alias:
                    selected.add(_normalize_name(alias))
        outputs[name] = explicit or selected
    return outputs


def _subquery_depth(root: exp.Expression) -> int:
    def walk(node: exp.Expression, depth: int) -> int:
        next_depth = depth + 1 if isinstance(node, exp.Subquery) else depth
        child_depth = next_depth
        for child in node.iter_expressions():
            child_depth = max(child_depth, walk(child, next_depth))
        return child_depth

    return walk(root, 0)


def _set_operations(root: exp.Expression) -> list[str]:
    ops: list[str] = []
    for klass, name in ((exp.Union, "union"), (exp.Intersect, "intersect"), (exp.Except, "except")):
        if list(root.find_all(klass)):
            ops.append(name)
    return sorted(set(ops))


def _aggregations(root: exp.Expression) -> list[str]:
    aggregations: set[str] = set()
    aggregate_types = (exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max)
    for node in root.find_all(*aggregate_types):
        aggregations.add(node.key.lower())
    for window in root.find_all(exp.Window):
        if window.this is not None:
            aggregations.add(f"window:{window.this.key.lower()}")
    return sorted(aggregations)
