"""Model Context Protocol (MCP) adapter for SQLGENX.

Exposes the guarded text-to-SQL path of :class:`src.services.query_service.QueryService`
as MCP tools. The same ``FastMCP`` instance serves two transports:

* stdio - ``python -m src.mcp`` for local desktop clients (Claude Desktop, Cursor, ...).
* streamable HTTP - mounted at ``/mcp`` inside the FastAPI app in ``src.api.main``.
"""

from src.mcp.server import mcp

__all__ = ["mcp"]
