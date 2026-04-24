from dataclasses import dataclass, field
import re

import sqlparse


FORBIDDEN_PREFIXES = {
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "GRANT",
    "REVOKE",
}

SYNTAX_BLOCK_TYPES = {"UNKNOWN"}


@dataclass
class GuardrailResult:
    allowed: bool
    sql: str
    reasons: list[str] = field(default_factory=list)
    syntax_valid: bool = True


def validate_sql_syntax(sql: str) -> tuple[bool, str | None]:
    try:
        statements = [s for s in sqlparse.parse(sql) if s.tokens]
    except Exception as exc:
        return False, f"SQL parsing failed: {exc}"

    if not statements:
        return False, "SQL is empty."
    if len(statements) > 1:
        return False, "Only one statement is allowed."

    statement_type = statements[0].get_type().upper()
    if statement_type in SYNTAX_BLOCK_TYPES:
        normalized = sql.strip().upper()
        if not (normalized.startswith("SELECT") or normalized.startswith("WITH")):
            return False, f"Only SELECT/WITH statements are allowed, got {statement_type}."
    return True, None


def _starts_with_forbidden_statement(sql: str) -> str | None:
    statements = sqlparse.parse(sql)
    if not statements:
        return "EMPTY_SQL"
    first = statements[0]
    tokens = [t for t in first.tokens if not t.is_whitespace]
    if not tokens:
        return "EMPTY_SQL"

    first_token = tokens[0].value.upper()
    if first_token in FORBIDDEN_PREFIXES:
        return first_token

    for keyword in FORBIDDEN_PREFIXES:
        if re.search(rf"\b{keyword}\b", sql, flags=re.IGNORECASE):
            return keyword
    return None


def _subquery_depth(sql: str) -> int:
    return len(re.findall(r"\(\s*SELECT\b", sql, flags=re.IGNORECASE))


def enforce_limit(sql: str, max_rows: int) -> str:
    if re.search(r"\bLIMIT\b\s+\d+", sql, flags=re.IGNORECASE):
        return sql
    cleaned = sql.strip().rstrip(";")
    return f"{cleaned} LIMIT {max_rows};"


def parse_explain_total_rows(explain_lines: list[str]) -> int:
    joined = " ".join(explain_lines)
    matches = re.findall(r"rows=(\d+)", joined)
    if not matches:
        return 0
    return max(int(m) for m in matches)


def apply_guardrails(
    sql: str,
    max_rows: int,
    max_subquery_depth: int,
    explain_estimated_rows: int | None = None,
    explain_row_limit: int | None = None,
) -> GuardrailResult:
    reasons: list[str] = []
    syntax_valid, syntax_reason = validate_sql_syntax(sql)
    if not syntax_valid and syntax_reason:
        reasons.append(f"Blocked invalid syntax: {syntax_reason}")

    forbidden = _starts_with_forbidden_statement(sql)
    if forbidden:
        reasons.append(f"Blocked forbidden statement: {forbidden}")

    depth = _subquery_depth(sql)
    if depth > max_subquery_depth:
        reasons.append(f"Blocked subquery depth {depth} > {max_subquery_depth}")

    if explain_estimated_rows is not None and explain_row_limit is not None:
        if explain_estimated_rows > explain_row_limit:
            reasons.append(
                f"Blocked estimated scan rows {explain_estimated_rows} > {explain_row_limit}"
            )

    guarded_sql = enforce_limit(sql, max_rows=max_rows)
    return GuardrailResult(
        allowed=not reasons,
        sql=guarded_sql,
        reasons=reasons,
        syntax_valid=syntax_valid,
    )
