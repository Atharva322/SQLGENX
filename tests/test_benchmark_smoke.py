from benchmarks.run_latency import run


def test_deterministic_benchmark_returns_machine_readable_report() -> None:
    report = run(profile="deterministic", repetitions=1, limit=2)

    assert report["profile"] == "deterministic"
    assert report["case_count"] == 2
    assert report["sample_count"] == 32
    assert "bucket_summary" in report
    assert report["samples"][0]["llm_calls"] >= 0
