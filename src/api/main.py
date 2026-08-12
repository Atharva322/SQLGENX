from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from src.adapters.registry import default_adapter_registry
from src.config.settings import get_settings
from src.connections.models import ConnectionNotFoundError
from src.connections.repository import LegacyEnvConnectionRepository
from src.db.engine import connections_health
from src.db.schema_introspector import get_schema_summary
from src.models.schemas import (
    AdapterCatalogResponse,
    ConnectionsHealthResponse,
    ConnectionsResponse,
    FeedbackRequest,
    FeedbackResponse,
    HistoryResponse,
    QueryRequest,
    QueryResponse,
    SchemaResponse,
)
from src.services.query_service import QueryService
from src.runtime.async_runtime import AsyncRuntimeOverloaded, AsyncRuntimeTimeout, close_query_runtime

service = QueryService()


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await close_query_runtime()
    service.llm.close()


app = FastAPI(title="Text-to-SQL with Guardrails", version="0.2.0", lifespan=lifespan)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/query", response_model=QueryResponse)
async def query(payload: QueryRequest) -> QueryResponse:
    row_limit = payload.options.row_limit if payload.options else None
    try:
        return await service.process_question_async(
            payload.question,
            connection_id=payload.connection_id,
            session_id=payload.session_id,
            row_limit_override=row_limit,
            sql_override=payload.sql_override,
        )
    except AsyncRuntimeOverloaded as exc:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "query_runtime_overloaded",
                "message": "Query capacity is temporarily exhausted.",
                "retry_after_seconds": exc.retry_after_seconds,
                "queue_depth": exc.queue_depth,
                "capacity": exc.capacity,
            },
            headers={"Retry-After": str(exc.retry_after_seconds)},
        ) from exc
    except AsyncRuntimeTimeout as exc:
        raise HTTPException(
            status_code=504,
            detail={
                "error": "query_runtime_timeout",
                "message": "Query timed out before completion.",
                "timeout_stage": exc.stage,
                "timeout_seconds": exc.timeout_seconds,
            },
        ) from exc
    except ConnectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "connection_not_found", "message": str(exc)}) from exc


@app.get("/v1/schema", response_model=SchemaResponse)
def schema(connection_id: str | None = Query(default=None)) -> SchemaResponse:
    try:
        summary = get_schema_summary(connection_id=connection_id)
    except ConnectionNotFoundError as exc:
        raise HTTPException(status_code=404, detail={"error": "connection_not_found", "message": str(exc)}) from exc
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


@app.get("/v1/connections", response_model=ConnectionsResponse)
def connections() -> ConnectionsResponse:
    return ConnectionsResponse(
        connections=[
            connection.model_dump()
            for connection in LegacyEnvConnectionRepository().list_public()
        ]
    )


@app.get("/v1/adapters", response_model=AdapterCatalogResponse)
def adapters(include_experimental: bool = Query(default=False)) -> AdapterCatalogResponse:
    expose_experimental = (
        include_experimental
        and get_settings().connection_adapter_experimental_catalog_enabled
    )
    items = []
    for adapter in default_adapter_registry().list(include_experimental=expose_experimental):
        items.append(
            {
                "key": adapter.key,
                "display_name": adapter.display_name,
                "release_state": adapter.release_state,
                "sqlglot_dialect": adapter.sqlglot_dialect,
                "driver_name": adapter.driver_name,
                "default_port": adapter.default_port,
                "supported_server_versions": list(adapter.supported_server_versions),
                "capabilities": {
                    "read_only_execution": adapter.capabilities.read_only_execution,
                    "schema_introspection": adapter.capabilities.schema_introspection,
                    "explain": adapter.capabilities.explain,
                    "row_limit": adapter.capabilities.row_limit,
                    "supports_tls": adapter.capabilities.supports_tls,
                    "notes": list(adapter.capabilities.notes),
                },
            }
        )
    return AdapterCatalogResponse(adapters=items)


@app.get("/v1/connections/health", response_model=ConnectionsHealthResponse)
def connections_healthcheck() -> ConnectionsHealthResponse:
    return ConnectionsHealthResponse(connections=connections_health())

