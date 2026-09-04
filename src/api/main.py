from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, Header, HTTPException, Query

from src.adapters.registry import default_adapter_registry
from src.config.settings import get_settings
from src.connections.models import (
    ConnectionCreateRequest,
    ConnectionNotFoundError,
    ConnectionTestRequest,
    ConnectionTestResponse,
    ConnectionUpdateRequest,
    PublicConnection,
)
from src.connections.service import DEMO_OWNER_ID, get_connection_service
from src.db.engine import connections_health
from src.db.schema_introspector import get_schema_summary, refresh_schema_summary
from src.mcp.server import MCP_HTTP_PATH, StreamableHttpTransport, mcp as mcp_server
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

# The MCP server is a thin adapter over ``service`` (resolved lazily from this module), so
# MCP clients and REST clients share one schema cache, RAG index, history, and async runtime.
mcp_http_transport = StreamableHttpTransport(mcp_server)


def _owner_id(x_owner_id: str | None = Header(default=None)) -> str:
    return x_owner_id.strip() if x_owner_id and x_owner_id.strip() else DEMO_OWNER_ID


def _connection_not_found(exc: Exception) -> HTTPException:
    return HTTPException(
        status_code=404,
        detail={"error": "connection_not_found", "message": str(exc)},
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    # The streamable HTTP MCP transport needs its session manager running for the app's lifetime.
    async with mcp_http_transport.run():
        yield
    await close_query_runtime()
    service.llm.close()


app = FastAPI(title="Text-to-SQL with Guardrails", version="0.2.0", lifespan=lifespan)

# Streamable HTTP MCP endpoint at ``POST /mcp``. Registered as a route rather than a mount so
# the path is served exactly (a mount would 307-redirect ``/mcp`` to ``/mcp/``, which not
# every MCP client follows).
app.add_route(MCP_HTTP_PATH, mcp_http_transport, name="mcp", include_in_schema=False)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/v1/query", response_model=QueryResponse)
async def query(payload: QueryRequest, owner_id: str = Depends(_owner_id)) -> QueryResponse:
    row_limit = payload.options.row_limit if payload.options else None
    try:
        return await service.process_question_async(
            payload.question,
            connection_id=payload.connection_id,
            session_id=payload.session_id,
            row_limit_override=row_limit,
            sql_override=payload.sql_override,
            owner_id=owner_id,
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
        raise _connection_not_found(exc) from exc


@app.get("/v1/schema", response_model=SchemaResponse)
def schema(
    connection_id: str | None = Query(default=None),
    owner_id: str = Depends(_owner_id),
) -> SchemaResponse:
    try:
        summary = get_schema_summary(connection_id=connection_id, owner_id=owner_id)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc
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
def connections(owner_id: str = Depends(_owner_id)) -> ConnectionsResponse:
    return ConnectionsResponse(
        connections=[
            connection.model_dump()
            for connection in get_connection_service().list_public(owner_id)
        ]
    )


@app.post("/v1/connections/test", response_model=ConnectionTestResponse)
def test_connection(payload: ConnectionTestRequest) -> ConnectionTestResponse:
    return get_connection_service().test(payload)


@app.post("/v1/connections", response_model=PublicConnection)
def create_connection(
    payload: ConnectionCreateRequest,
    owner_id: str = Depends(_owner_id),
) -> PublicConnection:
    try:
        connection = get_connection_service().create(owner_id, payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_config", "message": str(exc)}) from exc
    if connection.verification_state == "failed":
        raise HTTPException(
            status_code=400,
            detail={
                "error": "connection_test_failed",
                "safe_error_code": connection.safe_error_code,
            },
        )
    return connection


@app.get("/v1/connections/health", response_model=ConnectionsHealthResponse)
def connections_healthcheck(owner_id: str = Depends(_owner_id)) -> ConnectionsHealthResponse:
    try:
        health = connections_health(owner_id=owner_id)
    except TypeError:
        health = connections_health()
    return ConnectionsHealthResponse(connections=health)


@app.get("/v1/connections/{connection_id}", response_model=PublicConnection)
def get_connection(connection_id: str, owner_id: str = Depends(_owner_id)) -> PublicConnection:
    try:
        return get_connection_service().get_public(owner_id, connection_id)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc


@app.patch("/v1/connections/{connection_id}", response_model=PublicConnection)
def update_connection(
    connection_id: str,
    payload: ConnectionUpdateRequest,
    owner_id: str = Depends(_owner_id),
) -> PublicConnection:
    try:
        return get_connection_service().update(owner_id, connection_id, payload)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"error": "invalid_config", "message": str(exc)}) from exc


@app.delete("/v1/connections/{connection_id}", response_model=PublicConnection)
def delete_connection(connection_id: str, owner_id: str = Depends(_owner_id)) -> PublicConnection:
    try:
        return get_connection_service().delete(owner_id, connection_id)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc


@app.get("/v1/connections/{connection_id}/schema", response_model=SchemaResponse)
def connection_schema(connection_id: str, owner_id: str = Depends(_owner_id)) -> SchemaResponse:
    try:
        summary = get_schema_summary(connection_id=connection_id, owner_id=owner_id)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc
    return SchemaResponse(tables=summary.get("tables", []))


@app.post("/v1/connections/{connection_id}/schema/refresh", response_model=SchemaResponse)
def refresh_connection_schema(
    connection_id: str,
    owner_id: str = Depends(_owner_id),
) -> SchemaResponse:
    try:
        summary = refresh_schema_summary(connection_id=connection_id, owner_id=owner_id)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc
    return SchemaResponse(tables=summary.get("tables", []))


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

