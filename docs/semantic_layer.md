# Versioned Semantic Layer

Phase 6 adds a small, version-controlled semantic layer for high-value sample-company metrics. The goal is not a universal modeling system; it is a deterministic path for business questions that should not require the provider to rediscover metric SQL.

## Authoring

Semantic definitions live in `semantic/metrics.yaml`.

Each metric must define:

- `id`: stable machine-readable metric ID.
- `version`: semantic definition version.
- `description`: plain-English meaning.
- `source`: base table.
- `expression`: one approved aggregate over qualified columns.
- `allowed_dimensions`: dimensions that may group the metric.
- `allowed_filters`: canonical filters that may be applied.
- `owner`: accountable team.
- `sensitivity`: disclosure classification.
- `tests`: golden compile cases.

Dimensions and filters must use qualified expressions. Metric expressions are validated with SQLGlot and may not contain arbitrary functions, nested SQL, DML, DDL, or provider-generated expressions.

## Review

Run these before accepting definition changes:

```powershell
.venv\Scripts\python.exe -m src.semantic.cli lint
.venv\Scripts\python.exe -m src.semantic.cli test
.venv\Scripts\python.exe -m pytest tests\test_semantic_layer.py
```

Reviewers should verify:

- the metric owner is correct;
- the expression matches the business definition;
- allowed dimensions do not cross fanout joins;
- canonical filters are explicit;
- golden SQL tests cover expected demo/user phrasing;
- sensitivity is accurate.

## Runtime

`QueryService` attempts semantic compilation after schema linking and before provider SQL generation. When a semantic metric qualifies, SQL is compiled from YAML and then goes through the existing safety path:

- malicious intent guardrail;
- SQL guardrail;
- AST validation;
- strict identifier/join validation;
- EXPLAIN;
- read-only execution;
- trace finalization.

If no semantic definition qualifies, the request falls back to the existing general Text-to-SQL path. If a metric qualifies but the requested dimension/filter is unsupported, the service returns `UNANSWERABLE` with a clear semantic-layer warning rather than guessing.

## Migration

For a business definition change:

1. Add or update the metric with a new `version`.
2. Keep the old ID stable unless the meaning truly changes.
3. Update tests to show the intended compiled SQL.
4. Run lint, semantic tests, full Python tests, and benchmark smoke.
5. Include owner approval in the PR description.

## Rollback

Rollback is file-based:

1. Revert `semantic/metrics.yaml` to the last approved version.
2. Run `python -m src.semantic.cli lint` and `python -m src.semantic.cli test`.
3. Restart the API process so the semantic-layer cache reloads.
4. Rebuild the context index if `CONTEXT_INDEX_ENABLED=true`.

## Current Starter Metrics

- `total_revenue` v1.0.0
- `average_salary` v1.0.0
- `paid_invoice_value` v1.0.0
- `order_value` v1.0.0
- `employee_count` v1.0.0
