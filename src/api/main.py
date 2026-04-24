from fastapi import FastAPI, HTTPException, Query

from src.db.schema_introspector import get_schema_summary
from src.models.schemas import (
    FeedbackRequest,
    FeedbackResponse,
    HistoryResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
)
from src.services.query_service import QueryService

app = FastAPI(title="Text-to-SQL with Guardrails", version="0.2.0")
service = QueryService()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/query", response_model=QueryResponse)
def query(payload: QueryRequest) -> QueryResponse:
    row_limit = payload.options.row_limit if payload.options else None
    return service.process_question(
        payload.question,
        session_id=payload.session_id,
        row_limit_override=row_limit,
        sql_override=payload.sql_override,
    )


@app.get("/v1/schema", response_model=SchemaResponse)
def schema() -> SchemaResponse:
    summary = get_schema_summary()
    return SchemaResponse(tables=summary.get("tables", []))


@app.get("/v1/history", response_model=HistoryResponse)
def history(session_id: str | None = Query(default=None)) -> HistoryResponse:
    return HistoryResponse(items=service.get_history(session_id=session_id))


@app.post("/v1/feedback", response_model=FeedbackResponse)
def feedback(payload: FeedbackRequest) -> FeedbackResponse:
    try:
        return service.store_feedback(
            query_id=payload.query_id,
            session_id=payload.session_id,
            verdict=payload.verdict,
            notes=payload.notes,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
