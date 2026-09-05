"""FastMCP server exposing SQLGENX's guarded text-to-SQL pipeline.

This module is a thin adapter over the module-level :class:`QueryService` owned by
``src.api.main``. It adds no validation, guardrail, or execution logic of its own: every
tool call goes through exactly the same code path as the REST endpoints, including the
async runtime concurrency limits, DDL/DML blocking, AST validation, LIMIT enforcement,
EXPLAIN thresholds, and hallucination scoring.
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from functools import partial
from typing import Annotated, Any, Literal

import anyio
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ResourceError, ToolError
from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
from mcp.server.transport_security import TransportSecuritySettings
from pydantic import Field
from starlette.responses import JSONResponse
from starlette.types import Receive, Scope, Send

from src.config.settings import get_settings
from src.connections.models import ConnectionNotFoundError
from src.connections.service import DEMO_OWNER_ID, get_connection_service
from src.db.engine import connections_health
from src.db.schema_introspector import get_schema_summary, refresh_schema_summary
from src.models.schemas import HistoryItem, QueryResponse, SchemaResponse
from src.runtime.async_runtime import AsyncRuntimeOverloaded, AsyncRuntimeTimeout
from src.semantic.layer import SemanticLayer
from src.services.query_service import QueryService

SERVER_NAME = "sqlgenx"

INSTRUCTIONS = (
    "SQLGENX turns natural-language questions into read-only SQL, validates it, runs it "
    "against a connected database, and returns rows with a confidence score and warnings. "
    "Call `list_connections` to discover connection ids and their health, `get_schema` to "
    "inspect tables, `query` to ask a question, `submit_feedback` to mark a result correct or "
    "incorrect, and `get_history` to review earlier queries. Resources `sqlgenx://connections`, "
    "`schema://{connection_id}` and `metrics://semantic` expose the same information as "
    "attachable context; the semantic metrics are the preferred vocabulary for questions. "
    "Every query is guarded: DDL/DML is blocked, LIMITs are enforced, and low-confidence or "
    "blocked queries are reported in `warnings`."
)

FALLBACK_CONNECTION_ID = "default"

# The HTTP transport is served inside the FastAPI app, which the operator deploys behind
# whatever host name they choose. FastMCP's default enables DNS-rebinding protection that
# only admits localhost Host headers, so it is switched off here; host validation belongs to
# the reverse proxy / FastAPI deployment, exactly as it does for the REST endpoints.
#
# ``MCP_HTTP_PATH`` is where ``src.api.main`` registers the transport (see
# ``StreamableHttpTransport`` below). It is registered as a plain route, not a ``Mount``, so
# ``POST /mcp`` is served directly instead of redirecting ``/mcp`` to ``/mcp/``.
MCP_HTTP_PATH = "/mcp"

mcp = FastMCP(
    SERVER_NAME,
    instructions=INSTRUCTIONS,
    stateless_http=True,
    json_response=True,
    streamable_http_path=MCP_HTTP_PATH,
    transport_security=TransportSecuritySettings(enable_dns_rebinding_protection=False),
)



class StreamableHttpTransport:
    """ASGI endpoint serving ``mcp`` over stateless streamable HTTP.

    ``FastMCP.streamable_http_app()`` caches a single ``StreamableHTTPSessionManager``, and a
    manager may only be ``run()`` once. That is fine for one process lifetime but breaks any
    process that enters the app lifespan more than once (FastAPI's ``TestClient`` does so per
    ``with`` block). This wrapper builds a fresh manager every time :meth:`run` is entered and
    otherwise behaves exactly like the SDK's ``StreamableHTTPASGIApp``.

    Usage in ``src.api.main``::

        transport = StreamableHttpTransport(mcp)
        app.add_route(MCP_HTTP_PATH, transport)
        # in lifespan:
        async with transport.run():
            yield
    """

    def __init__(self, server: FastMCP) -> None:
        self._server = server
        self._manager: StreamableHTTPSessionManager | None = None

    def _new_manager(self) -> StreamableHTTPSessionManager:
        settings = self._server.settings
        return StreamableHTTPSessionManager(
            app=self._server._mcp_server,
            json_response=settings.json_response,
            stateless=settings.stateless_http,
            security_settings=settings.transport_security,
            max_request_body_size=settings.max_request_body_size,
        )

    @asynccontextmanager
    async def run(self) -> AsyncIterator[None]:
        """Run the transport for the lifetime of the enclosing app lifespan."""
        manager = self._new_manager()
        self._manager = manager
        try:
            async with manager.run():
                yield
        finally:
            if self._manager is manager:
                self._manager = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        manager = self._manager
        if manager is None:
            response = JSONResponse(
                {
                    "jsonrpc": "2.0",
                    "id": None,
                    "error": {
                        "code": -32000,
                        "message": "MCP transport is not running: the app lifespan has not started.",
                    },
                },
                status_code=503,
            )
            await response(scope, receive, send)
            return
        await manager.handle_request(scope, receive, send)


_TRIMMED_QUERY_FIELDS = (
    "query_id",
    "connection_id",
    "session_id",
    "sql",
    "explanation",
    "results",
    "confidence",
    "signals",
    "warnings",
    "accessed",
)
_TRIMMED_HISTORY_FIELDS = (
    "query_id",
    "connection_id",
    "session_id",
    "question",
    "sql",
    "explanation",
    "confidence",
    "signals",
    "warnings",
    "results",
    "feedback",
)
_META_SUMMARY_FIELDS = (
    "rows_returned",
    "execution_time_ms",
    "validation_level",
    "failure_classification",
)


def _service() -> QueryService:
    """Return the QueryService shared with the REST API.

    Resolved lazily so that ``src.api.main`` can import this module to mount the HTTP
    transport without a circular import, and so that tests which swap
    ``src.api.main.service`` are honoured by the MCP layer too.
    """
    from src.api import main as api_main

    return api_main.service


def _owner_id() -> str:
    """Owner identity for all MCP calls.

    stdio has no request headers, so the owner comes from ``MCP_OWNER_ID`` and falls back to
    the demo owner used by the REST API when ``X-Owner-Id`` is absent. It is deliberately not
    a tool argument: a per-call owner would let any client impersonate any tenant.
    """
    configured = str(getattr(get_settings(), "mcp_owner_id", "") or "").strip()
    return configured or DEMO_OWNER_ID


def _default_connection_id() -> str:
    """Connection id used when a client omits ``connection_id``.

    Comes from ``MCP_DEFAULT_CONNECTION_ID`` so a deployment whose ``default`` connection is
    not the one MCP clients should hit (or is unreachable) can steer them without every client
    having to pass an id. Falls back to ``"default"``, matching the REST API.
    """
    configured = str(getattr(get_settings(), "mcp_default_connection_id", "") or "").strip()
    return configured or FALLBACK_CONNECTION_ID


def _resolve_connection_id(connection_id: str | None) -> str:
    cleaned = (connection_id or "").strip()
    return cleaned or _default_connection_id()


def _tool_error(payload: dict[str, Any]) -> ToolError:
    """Build a ToolError whose message is a machine-readable JSON payload.

    MCP has no HTTP status codes, so the structured ``detail`` bodies the REST handlers
    return are serialised into the error message instead. Clients see ``isError=true``.
    """
    return ToolError(json.dumps(payload, sort_keys=True))


def _connection_not_found(exc: Exception) -> ToolError:
    return _tool_error({"error": "connection_not_found", "message": str(exc)})


def _trim_query_response(response: QueryResponse, include_meta: bool) -> dict[str, Any]:
    full = response.model_dump(mode="json")
    if include_meta:
        return full
    trimmed = {key: full[key] for key in _TRIMMED_QUERY_FIELDS}
    meta = full.get("execution_meta", {})
    for key in _META_SUMMARY_FIELDS:
        trimmed[key] = meta.get(key)
    return trimmed


def _trim_history_item(item: HistoryItem, include_meta: bool) -> dict[str, Any]:
    full = item.model_dump(mode="json")
    if include_meta:
        return full
    trimmed = {key: full[key] for key in _TRIMMED_HISTORY_FIELDS}
    meta = full.get("execution_meta", {})
    for key in _META_SUMMARY_FIELDS:
        trimmed[key] = meta.get(key)
    return trimmed


async def _run_sync(func, /, *args, **kwargs):
    """Run blocking service calls off the event loop, as FastAPI does for sync routes."""
    return await anyio.to_thread.run_sync(partial(func, *args, **kwargs))


@mcp.tool(
    name="query",
    title="Ask the database a question",
    description=(
        "Translate a natural-language question into guarded, read-only SQL, execute it, and "
        "return the SQL, an explanation, result rows, a 0-1 confidence score, and warnings. "
        "Blocked or low-confidence queries are reported through `warnings` rather than raised. "
        "Set `include_meta=true` to receive the full execution trace, reasoning, schema-linking "
        "and constraint metadata (large)."
    ),
)
async def query(
    question: Annotated[str, Field(min_length=3, description="Natural-language question.")],
    connection_id: Annotated[
        str | None,
        Field(
            description=(
                "Connection id from `list_connections`. Omit to use the server's default "
                "connection."
            )
        ),
    ] = None,
    session_id: Annotated[
        str | None,
        Field(description="Conversation/session id used to group history and feedback."),
    ] = None,
    row_limit: Annotated[
        int | None,
        Field(ge=1, le=5000, description="Maximum rows to return (capped at 5000)."),
    ] = None,
    sql_override: Annotated[
        str | None,
        Field(
            description=(
                "Optional SQL to validate and execute instead of generating it. The same "
                "guardrails apply."
            )
        ),
    ] = None,
    include_meta: Annotated[
        bool, Field(description="Include full execution/reasoning metadata.")
    ] = False,
) -> dict[str, Any]:
    try:
        response = await _service().process_question_async(
            question,
            connection_id=_resolve_connection_id(connection_id),
            session_id=session_id,
            row_limit_override=row_limit,
            sql_override=sql_override,
            owner_id=_owner_id(),
        )
    except AsyncRuntimeOverloaded as exc:
        raise _tool_error(
            {
                "error": "query_runtime_overloaded",
                "message": "Query capacity is temporarily exhausted.",
                "retry_after_seconds": exc.retry_after_seconds,
                "queue_depth": exc.queue_depth,
                "capacity": exc.capacity,
            }
        ) from exc
    except AsyncRuntimeTimeout as exc:
        raise _tool_error(
            {
                "error": "query_runtime_timeout",
                "message": "Query timed out before completion.",
                "timeout_stage": exc.stage,
                "timeout_seconds": exc.timeout_seconds,
            }
        ) from exc
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc
    return _trim_query_response(response, include_meta)


@mcp.tool(
    name="get_schema",
    title="Get database schema",
    description=(
        "Return the tables and columns of a connection as seen by the SQL generator. "
        "Set `refresh=true` to re-introspect the database and drop the cached summary."
    ),
)
async def get_schema(
    connection_id: Annotated[
        str | None,
        Field(description="Connection id. Omit to use the server's default connection."),
    ] = None,
    refresh: Annotated[bool, Field(description="Re-introspect instead of using the cache.")] = False,
) -> dict[str, Any]:
    resolved = _resolve_connection_id(connection_id)
    try:
        return await _schema_payload(resolved, refresh=refresh)
    except ConnectionNotFoundError as exc:
        raise _connection_not_found(exc) from exc


async def _schema_payload(connection_id: str, *, refresh: bool = False) -> dict[str, Any]:
    loader = refresh_schema_summary if refresh else get_schema_summary
    summary = await _run_sync(loader, connection_id=connection_id, owner_id=_owner_id())
    payload = SchemaResponse(tables=summary.get("tables", [])).model_dump(mode="json")
    payload["connection_id"] = connection_id
    payload["schema_fingerprint"] = summary.get("schema_fingerprint")
    return payload


async def _connections_payload(include_health: bool) -> dict[str, Any]:
    owner_id = _owner_id()
    default_id = _default_connection_id()
    connections = await _run_sync(get_connection_service().list_public, owner_id)
    health: dict[str, dict[str, Any]] = {}
    if include_health:
        try:
            health = await _run_sync(connections_health, owner_id=owner_id)
        except TypeError:
            health = await _run_sync(connections_health)
    items = []
    for connection in connections:
        record = connection.model_dump(mode="json")
        record["is_default"] = connection.id == default_id
        record["health"] = health.get(connection.id) if include_health else None
        items.append(record)
    return {"default_connection_id": default_id, "connections": items}


@mcp.tool(
    name="list_connections",
    title="List database connections",
    description=(
        "List the database connections available to this server, which one is used when "
        "`connection_id` is omitted (`is_default`), and whether each is reachable (`health`). "
        "Prefer a connection whose `health.healthy` is true. Records are secret-free: "
        "passwords and connection URLs are never returned."
    ),
)
async def list_connections(
    include_health: Annotated[
        bool,
        Field(
            description=(
                "Probe each connection and include `health`. Costs one connection attempt per "
                "connection; unreachable hosts take up to the connect timeout."
            )
        ),
    ] = True,
) -> dict[str, Any]:
    return await _connections_payload(include_health)


@mcp.tool(
    name="submit_feedback",
    title="Submit feedback on a query",
    description=(
        "Record whether a previous query's SQL was correct or incorrect. Correct examples feed "
        "the few-shot retrieval index; incorrect ones are kept for evaluation."
    ),
)
async def submit_feedback(
    query_id: Annotated[str, Field(min_length=3, description="`query_id` from a `query` result.")],
    verdict: Annotated[Literal["correct", "incorrect"], Field(description="Verdict.")],
    notes: Annotated[str | None, Field(description="Optional free-text notes.")] = None,
    session_id: Annotated[
        str | None, Field(description="Session id the query was issued under, if any.")
    ] = None,
) -> dict[str, Any]:
    try:
        result = await _run_sync(
            _service().store_feedback,
            query_id=query_id,
            verdict=verdict,
            notes=notes,
            session_id=session_id,
        )
    except ValueError as exc:
        raise _tool_error({"error": "unknown_query_id", "message": str(exc)}) from exc
    return result.model_dump(mode="json")


@mcp.tool(
    name="get_history",
    title="Get query history",
    description=(
        "Return earlier queries handled by this server, optionally filtered by session id. "
        "Set `include_meta=true` for full execution metadata per item (large)."
    ),
)
async def get_history(
    session_id: Annotated[
        str | None, Field(description="Only return items from this session.")
    ] = None,
    include_meta: Annotated[
        bool, Field(description="Include full execution/reasoning metadata per item.")
    ] = False,
) -> dict[str, Any]:
    items = _service().get_history(session_id=session_id)
    return {"items": [_trim_history_item(item, include_meta) for item in items]}


# --------------------------------------------------------------------------------------
# Resources: attachable context for clients (Claude Desktop shows these in the resource
# picker). They expose the same information as the tools above, read-only.
# --------------------------------------------------------------------------------------


def _json(payload: Any) -> str:
    return json.dumps(payload, indent=2, sort_keys=True, default=str)


@mcp.resource(
    "sqlgenx://connections",
    name="connections",
    title="Database connections",
    description=(
        "Connections this server can query, the default connection id, and current health. "
        "Same content as the `list_connections` tool."
    ),
    mime_type="application/json",
)
async def connections_resource() -> str:
    return _json(await _connections_payload(include_health=True))


@mcp.resource(
    "schema://{connection_id}",
    name="schema",
    title="Database schema",
    description=(
        "Tables and columns of a connection as seen by the SQL generator. Use the connection "
        "ids from `sqlgenx://connections`."
    ),
    mime_type="application/json",
)
async def schema_resource(connection_id: str) -> str:
    try:
        return _json(await _schema_payload(_resolve_connection_id(connection_id)))
    except ConnectionNotFoundError as exc:
        raise ResourceError(_json({"error": "connection_not_found", "message": str(exc)})) from exc


@mcp.resource(
    "metrics://semantic",
    name="semantic_metrics",
    title="Semantic layer metrics",
    description=(
        "Governed metric, dimension, filter, entity and approved-join definitions from the "
        "semantic layer. Questions phrased with these metric names and dimensions compile to "
        "approved SQL deterministically instead of relying on free-form generation."
    ),
    mime_type="application/json",
)
async def semantic_metrics_resource() -> str:
    settings = get_settings()
    path = str(getattr(settings, "semantic_layer_path", "semantic/metrics.yaml"))
    layer = await _run_sync(SemanticLayer.from_path, path)
    return _json(
        {
            "enabled": bool(getattr(settings, "semantic_layer_enabled", True)),
            "path": path,
            "definition": layer.definition.model_dump(mode="json"),
        }
    )


__all__ = ["MCP_HTTP_PATH", "SERVER_NAME", "StreamableHttpTransport", "mcp"]
