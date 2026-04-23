def score_result_sanity(rows: list[dict]) -> float:
    """Stub sanity check: penalize error rows and empty outputs."""
    if not rows:
        return 0.4
    if any('error' in row for row in rows):
        return 0.2
    return 0.9
