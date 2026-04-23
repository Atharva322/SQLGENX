# Text-to-SQL Interface with Guardrails and Hallucination Detection

Production-style scaffold for a natural-language-to-SQL system with safety controls, hallucination detection signals, and a query UI.

## Quickstart

1. Create and activate a Python 3.11 virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Copy env template and set keys:

```bash
cp .env.example .env.local
```

4. Run API locally:

```bash
uvicorn src.api.main:app --reload --host 0.0.0.0 --port 8000
```

5. Run Streamlit frontend locally:

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

## API Endpoints

- `GET /health`
- `POST /v1/query`
- `GET /v1/schema`
- `GET /v1/history`

## Architecture (Scaffold)

- `src/services/prompt_builder.py`: schema-aware prompt assembly and lightweight filtering stub.
- `src/guardrails/rules.py`: DDL/DML blocking, LIMIT enforcement, subquery-depth rule, explain threshold hook.
- `src/validation/*`: alignment, sanity, and multi-query agreement score stubs.
- `src/services/query_service.py`: orchestration pipeline for generate -> guardrail -> execute -> score.

## Testing

```bash
pytest -q
```

## Next-Phase Checklist

- Wire provider-specific structured output with Instructor for OpenAI/Anthropic.
- Add embedding-based schema filtering and ambiguity clarification flow.
- Add robust SQL parsing and explain-plan row-estimation extraction.
- Implement dual-query generation and result agreement analysis.
- Expand eval suite to 50+ golden cases and publish metrics.
