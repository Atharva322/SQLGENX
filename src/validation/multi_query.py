def score_multi_query_agreement(question: str, primary_rows: list[dict]) -> float:
    """Stub for dual-query agreement until secondary SQL path is implemented."""
    if not primary_rows:
        return 0.5
    return 0.8
