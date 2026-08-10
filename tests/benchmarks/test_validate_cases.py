from benchmarks.validate_cases import validate_cases


def test_benchmark_workload_contract_is_valid() -> None:
    result = validate_cases()

    assert result["valid"] is True
    assert result["case_count"] == 50
    assert result["schema_fingerprint"] == "phase0-benchmark-v1"
