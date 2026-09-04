"""MCP tool tests using the SDK's in-memory client (no subprocess, no socket).

These mirror ``tests/test_query_endpoint.py``: they call the real service with no mocks
except where an exception has to be injected, and rely on the deterministic offline
behaviour of the pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from mcp.shared.memory import create_connected_server_and_client_session
from mcp.types import CallToolResult

from src.api import main as api_main
from src.config.settings import get_settings
from src.connections.service import DEMO_OWNER_ID
from src.mcp import server as mcp_server
from src.runtime.async_runtime import AsyncRuntimeOverloaded, AsyncRuntimeTimeout

EXPECTED_TOOLS = {"query", "get_schema", "list_connections", "submit_feedback", "get_history"}


@pytest.fixture(autouse=True)
def fast_mcp_test_settings() -> None:
    api_main.service.settings.rag_enabled = False
    api_main.service.settings.async_query_total_timeout_seconds = 120.0
    get_settings.cache_clear()


def _error_payload(result: CallToolResult) -> dict:
    """Extract the structured JSON payload from a ToolError message.

    FastMCP prefixes tool exceptions with ``Error executing tool <name>: ``; the JSON built
    by ``src.mcp.server._tool_error`` follows that prefix.
    """
    assert result.isError is True
    text = result.content[0].text
    return json.loads(text[text.index("{") :])


async def _call(name: str, arguments: dict | None = None) -> CallToolResult:
    async with create_connected_server_and_client_session(mcp_server.mcp) as session:
        return await session.call_tool(name, arguments or {})


@pytest.mark.asyncio
async def test_lists_expected_tools() -> None:
    async with create_connected_server_and_client_session(mcp_server.mcp) as session:
        listed = await session.list_tools()
    names = {tool.name for tool in listed.tools}
    assert names == EXPECTED_TOOLS
    query_tool = next(tool for tool in listed.tools if tool.name == "query")
    props = query_tool.inputSchema["properties"]
    assert "question" in props
    assert "owner_id" not in props
    assert props["row_limit"]["anyOf"][0]["maximum"] == 5000


@pytest.mark.asyncio
async def test_query_returns_trimmed_shape_by_default() -> None:
    result = await _call("query", {"question": "What is total revenue by region?"})
    assert result.isError is False
    body = result.structuredContent
    for key in (
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
        "rows_returned",
        "execution_time_ms",
        "validation_level",
        "failure_classification",
    ):
        assert key in body, key
    assert "alignment_score" in body["signals"]
    for heavy in ("execution_meta", "reasoning", "linking_meta", "constraint_meta"):
        assert heavy not in body
    # The text fallback carries the same JSON for clients without structured-content support.
    assert json.loads(result.content[0].text)["query_id"] == body["query_id"]


@pytest.mark.asyncio
async def test_query_include_meta_returns_full_response() -> None:
    result = await _call(
        "query", {"question": "What is total revenue by region?", "include_meta": True}
    )
    assert result.isError is False
    body = result.structuredContent
    assert "execution_meta" in body
    assert "reasoning" in body
    assert "linking_meta" in body
    assert "constraint_meta" in body
    assert "rows_returned" in body["execution_meta"]


@pytest.mark.asyncio
async def test_query_guardrail_block_surfaces_in_warnings() -> None:
    result = await _call("query", {"question": "Drop table employees"})
    assert result.isError is False
    body = result.structuredContent
    warnings = " ".join(body["warnings"]).lower()
    assert "blocked" in warnings
    assert body["rows_returned"] == 0


@pytest.mark.asyncio
async def test_query_rejects_invalid_arguments_before_reaching_service(monkeypatch) -> None:
    async def should_not_run(*args, **kwargs):  # pragma: no cover - guard
        raise AssertionError("service must not be called for invalid arguments")

    monkeypatch.setattr(api_main.service, "process_question_async", should_not_run)
    result = await _call("query", {"question": "ok question", "row_limit": 999999})
    assert result.isError is True
    assert "row_limit" in result.content[0].text


@pytest.mark.asyncio
async def test_query_overload_maps_to_structured_tool_error(monkeypatch) -> None:
    async def overloaded(*args, **kwargs):
        raise AsyncRuntimeOverloaded(retry_after_seconds=3, queue_depth=2, capacity=4)

    monkeypatch.setattr(api_main.service, "process_question_async", overloaded)
    result = await _call("query", {"question": "What is total revenue by region?"})
    payload = _error_payload(result)
    assert payload["error"] == "query_runtime_overloaded"
    assert payload["retry_after_seconds"] == 3
    assert payload["queue_depth"] == 2
    assert payload["capacity"] == 4
    assert "postgresql://" not in result.content[0].text.lower()


@pytest.mark.asyncio
async def test_query_timeout_maps_to_structured_tool_error(monkeypatch) -> None:
    async def timeout(*args, **kwargs):
        raise AsyncRuntimeTimeout(timeout_seconds=0.01, stage="total_request")

    monkeypatch.setattr(api_main.service, "process_question_async", timeout)
    result = await _call("query", {"question": "What is total revenue by region?"})
    payload = _error_payload(result)
    assert payload["error"] == "query_runtime_timeout"
    assert payload["timeout_stage"] == "total_request"
    assert "postgresql://" not in result.content[0].text.lower()


@pytest.mark.asyncio
async def test_query_unknown_connection_maps_to_not_found() -> None:
    result = await _call(
        "query",
        {"question": "What is total revenue by region?", "connection_id": "does-not-exist"},
    )
    payload = _error_payload(result)
    assert payload["error"] == "connection_not_found"
    assert "does-not-exist" in payload["message"]


@pytest.mark.asyncio
async def test_query_uses_configured_owner_id(monkeypatch) -> None:
    captured: dict = {}

    async def capture(question, **kwargs):
        captured.update(kwargs)
        raise AsyncRuntimeTimeout(timeout_seconds=0.01, stage="total_request")

    monkeypatch.setattr(api_main.service, "process_question_async", capture)

    monkeypatch.delenv("MCP_OWNER_ID", raising=False)
    get_settings.cache_clear()
    await _call("query", {"question": "What is total revenue by region?"})
    assert captured["owner_id"] == DEMO_OWNER_ID

    monkeypatch.setenv("MCP_OWNER_ID", "tenant-42")
    get_settings.cache_clear()
    await _call("query", {"question": "What is total revenue by region?", "row_limit": 10})
    assert captured["owner_id"] == "tenant-42"
    assert captured["row_limit_override"] == 10
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_get_schema_returns_tables_list() -> None:
    result = await _call("get_schema", {})
    assert result.isError is False
    assert isinstance(result.structuredContent["tables"], list)


@pytest.mark.asyncio
async def test_get_schema_unknown_connection_maps_to_not_found() -> None:
    result = await _call("get_schema", {"connection_id": "does-not-exist"})
    payload = _error_payload(result)
    assert payload["error"] == "connection_not_found"


@pytest.mark.asyncio
async def test_list_connections_is_secret_free() -> None:
    result = await _call("list_connections", {})
    assert result.isError is False
    connections = result.structuredContent["connections"]
    assert any(connection["id"] == "default" for connection in connections)
    serialized = json.dumps(connections).lower()
    assert "password" not in serialized
    assert "postgresql://" not in serialized
    assert "@" not in serialized


@pytest.mark.asyncio
async def test_history_and_feedback_round_trip(monkeypatch) -> None:
    target_path = Path("logs") / "test_mcp_feedback.jsonl"
    target_path.unlink(missing_ok=True)
    monkeypatch.setattr(api_main.service, "_feedback_target_file", lambda verdict: target_path)

    query_result = await _call(
        "query", {"question": "What is total sales by region?", "session_id": "mcp_s1"}
    )
    assert query_result.isError is False
    query_id = query_result.structuredContent["query_id"]

    history = await _call("get_history", {"session_id": "mcp_s1"})
    assert history.isError is False
    items = history.structuredContent["items"]
    assert any(item["query_id"] == query_id for item in items)
    assert all(item["session_id"] == "mcp_s1" for item in items)
    assert all("execution_meta" not in item for item in items)

    history_full = await _call("get_history", {"session_id": "mcp_s1", "include_meta": True})
    assert all("execution_meta" in item for item in history_full.structuredContent["items"])

    feedback = await _call(
        "submit_feedback",
        {
            "query_id": query_id,
            "session_id": "mcp_s1",
            "verdict": "incorrect",
            "notes": "Expected a grouped result.",
        },
    )
    assert feedback.isError is False
    assert feedback.structuredContent["stored"] is True
    assert target_path.exists()
    stored = json.loads(target_path.read_text(encoding="utf-8").strip().splitlines()[-1])
    assert stored["query_id"] == query_id
    assert stored["verdict"] == "incorrect"
    target_path.unlink(missing_ok=True)


@pytest.mark.asyncio
async def test_submit_feedback_unknown_query_id_is_tool_error() -> None:
    result = await _call("submit_feedback", {"query_id": "qry_missing", "verdict": "correct"})
    payload = _error_payload(result)
    assert payload["error"] == "unknown_query_id"


def test_stdio_entry_point_is_importable() -> None:
    from src.mcp import __main__ as entry

    assert callable(entry.main)
