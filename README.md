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

## Evaluation Suite

- Golden dataset: `evals/golden_queries.jsonl` (55 cases)
- Automated evaluator: `evals/run_evals.py`

Run:

```bash
python evals/run_evals.py
```

Metrics reported:
- SQL exact match
- execution result match
- hallucination detection rate
- guardrail effectiveness

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
