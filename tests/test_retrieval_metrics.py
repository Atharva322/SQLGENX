from evals.run_evals import _ndcg_at_k, _recall_at_k


def test_recall_at_k() -> None:
    ranked = ["sales", "employees", "departments"]
    relevant = {"sales", "departments"}
    score = _recall_at_k(ranked, relevant, 2)
    assert round(score, 3) == 0.5


def test_ndcg_at_k_perfect_ranking() -> None:
    ranked = ["sales", "departments", "employees"]
    relevant = {"sales", "departments"}
    score = _ndcg_at_k(ranked, relevant, 2)
    assert round(score, 3) == 1.0

