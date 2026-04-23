def score_alignment(question: str, sql: str) -> float:
    """Stub: compare question keywords with SQL tokens for rough alignment."""
    q = {t.lower() for t in question.split() if len(t) > 2}
    s = {t.lower().strip(',()') for t in sql.split()}
    if not q:
        return 0.0
    return min(1.0, len(q.intersection(s)) / max(1, len(q)))
