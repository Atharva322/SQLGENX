# Text-to-SQL Interface with Guardrails and Hallucination Detection

Latest Eval Snapshot (run pending local dependency setup):
- Execution Accuracy: `0.000` (pending)
- Hallucination Detection Rate: `0.000` (pending)
- Unsafe Queries Executed: `0` across `55` safety-focused eval cases

## What Is Implemented

- API endpoints:
  - `POST /v1/query`: accepts natural language question, returns SQL, execution results, confidence, warnings.
  - `GET /v1/schema`: returns introspected database schema.
- `GET /v1/history?session_id=...`: returns session query history with results and feedback.
- `POST /v1/feedback`: stores correct/incorrect user feedback.
- `GET /v1/connections`: lists available `connection_id -> database_url` mappings.
- Frontend (Streamlit):
  - natural language input
  - generated SQL with syntax highlighting
  - editable SQL for power users
  - sortable result table
  - confidence score with signal breakdown
  - session history panel with query details and results
  - feedback buttons (correct/incorrect)
- Guardrails and hallucination signals:
  - DDL/DML blocking, subquery depth limit, enforced `LIMIT`, EXPLAIN scan threshold
  - SQL back-translation alignment check
  - result sanity anomaly checks
  - multi-query agreement scoring
  - combined confidence score with signal breakdown

## Quickstart

1. Create and activate a Python 3.11 virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy env template:

```bash
cp .env.example .env.local
```

4. Run API:

```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

5. Run frontend:

```bash
streamlit run frontend/app.py --server.port=8501
```

## Docker Compose

```bash
docker compose up --build
```

Services:
- API: `http://localhost:8000`
- Frontend: `http://localhost:8501`
- Postgres: `localhost:5432`

## Multi-Database Connection Selector

Configure one default DB and optional named connections in `.env.local`:

```env
DATABASE_URL=postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company
CONNECTION_URLS_JSON={"default":"postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company","docker_internal":"postgresql://text2sql_user:text2sql_pass@postgres:5432/sample_company"}
```

The UI reads `/v1/connections` and lets you switch by `connection_id` without editing env per query.

## RAG Retrieval for SQL Generation

The prompt engine now supports retrieval-augmented generation:
- embeds/retrieves relevant schema chunks (tables + columns)
- embeds/retrieves relevant successful past queries from feedback few-shots
- injects top-k retrieved schema + examples into the SQL-generation prompt

Config via env:

```env
RAG_ENABLED=true
RAG_EMBEDDING_MODEL=sentence-transformers/all-MiniLM-L6-v2
RAG_EMBEDDING_LOCAL_ONLY=true
RAG_TOP_K_SCHEMA=5
RAG_TOP_K_EXAMPLES=3
RAG_MIN_FEEDBACK_CONFIDENCE=0.65
```

Set `RAG_EMBEDDING_LOCAL_ONLY=false` only when you want automatic model downloads.

## Evaluation Suite

- Golden dataset: `evals/golden_queries.jsonl` (55 cases)
- Automated evaluator: `evals/run_evals.py`

Run:

```bash
python evals/run_evals.py
```

Fast retrieval-metric only run:

```bash
python evals/run_evals.py --retrieval-only --limit 20
```

Metrics reported:
- SQL exact match
- execution result match
- hallucination detection rate
- guardrail effectiveness
- schema Recall@K
- schema nDCG@K

## Feedback Flywheel Files

- Correct feedback appends to: `data/feedback_fewshots.jsonl`
- Incorrect feedback appends to: `evals/feedback_incorrect_cases.jsonl`

## Test Commands

```bash
pytest -q
```

If `pytest` is not installed in your environment:

```bash
pip install pytest pytest-asyncio
```
