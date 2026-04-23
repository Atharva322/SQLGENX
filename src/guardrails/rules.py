from dataclasses import dataclass, field
import re

import sqlparse


FORBIDDEN_PREFIXES = {
    'CREATE',
    'ALTER',
    'DROP',
    'TRUNCATE',
    'INSERT',
    'UPDATE',
    'DELETE',
    'MERGE',
    'GRANT',
    'REVOKE',
}


@dataclass
class GuardrailResult:
    allowed: bool
    sql: str
    reasons: list[str] = field(default_factory=list)


def _starts_with_forbidden_statement(sql: str) -> str | None:
    statements = sqlparse.parse(sql)
    if not statements:
        return 'EMPTY_SQL'
    first = statements[0]
    tokens = [t for t in first.tokens if not t.is_whitespace]
    if not tokens:
        return 'EMPTY_SQL'

    first_token = tokens[0].value.upper()
    if first_token in FORBIDDEN_PREFIXES:
        return first_token

    for keyword in FORBIDDEN_PREFIXES:
        if re.search(rf'\b{keyword}\b', sql, flags=re.IGNORECASE):
            return keyword
    return None


def _subquery_depth(sql: str) -> int:
    depth = 0
    max_depth = 0
    for idx, char in enumerate(sql):
        if char == '(':
            segment = sql[idx : idx + 30].upper()
            if 'SELECT' in segment:
                depth += 1
                max_depth = max(max_depth, depth)
        elif char == ')' and depth > 0:
            depth -= 1
    return max_depth


def enforce_limit(sql: str, max_rows: int) -> str:
    if re.search(r'\bLIMIT\b\s+\d+', sql, flags=re.IGNORECASE):
        return sql
    cleaned = sql.strip().rstrip(';')
    return f"{cleaned} LIMIT {max_rows};"


def parse_explain_total_rows(explain_lines: list[str]) -> int:
    joined = ' '.join(explain_lines)
    matches = re.findall(r'rows=(\d+)', joined)
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

    forbidden = _starts_with_forbidden_statement(sql)
    if forbidden:
        reasons.append(f'Blocked forbidden statement: {forbidden}')

    depth = _subquery_depth(sql)
    if depth > max_subquery_depth:
        reasons.append(f'Blocked subquery depth {depth} > {max_subquery_depth}')

    if explain_estimated_rows is not None and explain_row_limit is not None:
        if explain_estimated_rows > explain_row_limit:
            reasons.append(
                f'Blocked estimated scan rows {explain_estimated_rows} > {explain_row_limit}'
            )

    guarded_sql = enforce_limit(sql, max_rows=max_rows)
    return GuardrailResult(allowed=not reasons, sql=guarded_sql, reasons=reasons)
