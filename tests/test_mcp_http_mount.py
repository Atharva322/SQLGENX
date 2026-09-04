"""The streamable HTTP MCP transport is served by the FastAPI app at ``POST /mcp``."""

from fastapi.testclient import TestClient

from src.api.main import app
from src.mcp.server import SERVER_NAME

MCP_HEADERS = {
    "Accept": "application/json, text/event-stream",
    "Content-Type": "application/json",
}


def _initialize_payload() -> dict:
    return {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "pytest", "version": "0"},
        },
    }


def test_mcp_endpoint_answers_initialize() -> None:
    # Context manager runs the lifespan, which starts the MCP session manager.
    with TestClient(app) as client:
        response = client.post(
            "/mcp", json=_initialize_payload(), headers=MCP_HEADERS, follow_redirects=False
        )
    assert response.status_code == 200
    body = response.json()
    assert body["result"]["serverInfo"]["name"] == SERVER_NAME
    assert "tools" in body["result"]["capabilities"]


def test_mcp_endpoint_lists_tools_statelessly() -> None:
    # Stateless transport: tools/list works without a prior initialize or session id.
    with TestClient(app) as client:
        response = client.post(
            "/mcp",
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            headers=MCP_HEADERS,
            follow_redirects=False,
        )
    assert response.status_code == 200
    names = {tool["name"] for tool in response.json()["result"]["tools"]}
    assert names == {"query", "get_schema", "list_connections", "submit_feedback", "get_history"}


def test_rest_endpoints_still_served_alongside_mcp() -> None:
    with TestClient(app) as client:
        assert client.get("/health").status_code == 200
        assert client.get("/openapi.json").status_code == 200
        # The MCP route is transport plumbing, not part of the REST contract.
        assert "/mcp" not in client.get("/openapi.json").json()["paths"]


def test_mcp_endpoint_reports_not_running_before_lifespan() -> None:
    # Without the context manager the lifespan never starts, so the transport has no session
    # manager. It must answer with a JSON-RPC error rather than crash.
    client = TestClient(app)
    response = client.post(
        "/mcp", json=_initialize_payload(), headers=MCP_HEADERS, follow_redirects=False
    )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == -32000


def test_mcp_transport_survives_repeated_lifespans() -> None:
    # The session manager is rebuilt per lifespan run; entering it twice must work.
    for _ in range(2):
        with TestClient(app) as client:
            response = client.post(
                "/mcp", json=_initialize_payload(), headers=MCP_HEADERS, follow_redirects=False
            )
            assert response.status_code == 200

