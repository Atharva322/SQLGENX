from __future__ import annotations


def reciprocal_rank_fusion(
    rankings: list[list[tuple[str, float]]], k: int = 60
) -> list[tuple[str, float]]:
    scores: dict[str, float] = {}
    best_lane_score: dict[str, float] = {}
    for ranking in rankings:
        for rank, (doc_id, lane_score) in enumerate(ranking, start=1):
            scores[doc_id] = scores.get(doc_id, 0.0) + 1.0 / (k + rank)
            best_lane_score[doc_id] = max(best_lane_score.get(doc_id, float("-inf")), lane_score)
    fused = [(doc_id, score) for doc_id, score in scores.items()]
    fused.sort(key=lambda item: (-item[1], -best_lane_score.get(item[0], 0.0), item[0]))
    return fused
