# Phase 6 Demo Script

Use this to show that the semantic layer is working.

## Start

```powershell
cd C:\Users\athar\Desktop\GENXSQL\text2sql
docker compose up -d benchmark-postgres
$env:DATABASE_URL='postgresql://text2sql_user:text2sql_pass@localhost:55432/sample_company'
$env:CONNECTION_URLS_JSON='{}'
$env:LLM_PROVIDER='deterministic'
$env:LLM_MODEL='sqlgenx-fixture-v1'
$env:RAG_EMBEDDING_LOCAL_ONLY='true'
$env:SEMANTIC_LAYER_ENABLED='true'
.venv\Scripts\uvicorn.exe src.api.main:app --reload --port 8000
```

For a fast terminal-only demo that uses the same `QueryService` and database path:

```powershell
.venv\Scripts\python.exe scripts\demo_semantic_layer.py
```

## Show Definition

Open `semantic/metrics.yaml` and point to:

- metric ID and version;
- expression;
- allowed dimensions;
- owner and sensitivity;
- approved joins.

## Call A Defined Metric

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/v1/query `
  -ContentType 'application/json' `
  -Body '{"question":"Show total revenue by region","connection_id":"default"}'
```

Expected talking points:

- `reasoning.strategy` is `semantic_layer_compile`.
- `reasoning.selected_candidate` is `total_revenue`.
- SQL is deterministic: `SUM(sales.amount)` grouped by `sales.region`.
- `execution_meta.semantic_layer.metric.version` is `1.0.0`.
- Provider SQL generation is bypassed; safety and execution still run.

## Show A Paraphrase

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/v1/query `
  -ContentType 'application/json' `
  -Body '{"question":"Bookings by territory","connection_id":"default"}'
```

Expected talking point: different wording resolves to the same `total_revenue` metric and the same SQL.

## Show Multi-Table Compile

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/v1/query `
  -ContentType 'application/json' `
  -Body '{"question":"Average salary by department","connection_id":"default"}'
```

Expected talking point: the SQL uses the approved `employees -> departments` join and discloses that join in `execution_meta.semantic_layer.joins`.

## Show Unsupported Combination

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/v1/query `
  -ContentType 'application/json' `
  -Body '{"question":"Show total revenue by department","connection_id":"default"}'
```

Expected talking point: this returns `UNANSWERABLE` because `department` is not an allowed dimension for `total_revenue`; the system refuses to guess.

## Show Fallback

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/v1/query `
  -ContentType 'application/json' `
  -Body '{"question":"List employees in Engineering","connection_id":"default"}'
```

Expected talking point: this is not a defined semantic metric, so it falls back to the general Text-to-SQL path.
