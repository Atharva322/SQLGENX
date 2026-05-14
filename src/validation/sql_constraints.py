import re

from src.models.schemas import ConstraintValidationResult, LinkingContext


TABLE_PATTERN = re.compile(
    r"\b(?:FROM|JOIN)\s+([a-zA-Z_][\w\.]*)(?:\s+(?:AS\s+)?([a-zA-Z_][\w]*))?",
    flags=re.IGNORECASE,
)
COLUMN_PATTERN = re.compile(r"([a-zA-Z_][\w]*)\.([a-zA-Z_][\w]*)")


def _normalize_identifier(identifier: str) -> str:
    return identifier.strip().strip("`").strip('"').lower()


def validate_sql_identifiers(
    sql: str,
    linking: LinkingContext,
    strict_join_grounding: bool = True,
) -> ConstraintValidationResult:
    allowed_tables = {_normalize_identifier(t) for t in linking.resolved.tables}
    allowed_columns = {_normalize_identifier(c) for c in linking.resolved.columns}
    alias_to_table: dict[str, str] = {}
    referenced_tables: set[str] = set()
    blocked: list[str] = []
    reasons: list[str] = []

    for table, alias in TABLE_PATTERN.findall(sql):
        norm = _normalize_identifier(table)
        # Allow aliased schema.table by checking terminal table segment.
        terminal = norm.split(".")[-1]
        resolved_table = terminal if terminal in allowed_tables else norm
        if resolved_table not in allowed_tables:
            blocked.append(table)
            continue
        referenced_tables.add(resolved_table)
        if alias:
            alias_to_table[_normalize_identifier(alias)] = resolved_table

    for table, col in COLUMN_PATTERN.findall(sql):
        left = _normalize_identifier(table)
        canonical_left = alias_to_table.get(left, left)
        fq = _normalize_identifier(f"{canonical_left}.{col}")
        if fq not in allowed_columns:
            blocked.append(f"{table}.{col}")

    if blocked:
        reasons.append("SQL references identifiers outside schema-link whitelist.")
        reasons.append("Always enrich from known schema and synonyms; never invent identifiers.")
        return ConstraintValidationResult(
            passed=False,
            blocked_identifiers=sorted(set(blocked)),
            reasons=reasons,
            violation_type="unknown_identifier",
            enforced_policy="strict_identifiers",
        )
    if strict_join_grounding:
        join_hints = {_normalize_identifier(t) for t in linking.resolved.join_hints}
        if " join " in f" {sql.lower()} " and join_hints:
            ungrounded = sorted(t for t in referenced_tables if t not in join_hints)
            if ungrounded:
                return ConstraintValidationResult(
                    passed=False,
                    blocked_identifiers=ungrounded,
                    reasons=["JOIN tables are not grounded in schema-link join hints."],
                    violation_type="join_not_grounded",
                    enforced_policy="strict_join_grounding",
                )
    return ConstraintValidationResult(passed=True, blocked_identifiers=[], reasons=[])
