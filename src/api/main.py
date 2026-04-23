from fastapi import FastAPI

from src.db.schema_introspector import get_schema_summary
from src.models.schemas import HistoryResponse, QueryRequest, QueryResponse, SchemaResponse
from src.services.query_service import QueryService

app = FastAPI(title='Text-to-SQL with Guardrails', version='0.1.0')
service = QueryService()


@app.get('/health')
def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.post('/v1/query', response_model=QueryResponse)
def query(payload: QueryRequest) -> QueryResponse:
    return service.process_question(payload.question)


@app.get('/v1/schema', response_model=SchemaResponse)
def schema() -> SchemaResponse:
    summary = get_schema_summary()
    return SchemaResponse(tables=summary.get('tables', []))


@app.get('/v1/history', response_model=HistoryResponse)
def history() -> HistoryResponse:
    return HistoryResponse(items=service.history)
