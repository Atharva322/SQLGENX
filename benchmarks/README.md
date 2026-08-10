# Latency Benchmarks

Phase 0 benchmark runs write machine-readable JSON reports to `benchmarks/results/`, which is git-ignored.

Deterministic smoke profile:

```bash
python benchmarks/run_latency.py --profile deterministic --repetitions 1
```

The deterministic profile uses a fake local request runner and does not call live LLM providers. Live-provider benchmark runs must be labeled separately and should document provider, model, database seed, flags, and sample size.
