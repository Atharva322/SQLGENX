# MCP Interface

SQLGENX exposes its guarded text-to-SQL pipeline over the
[Model Context Protocol](https://modelcontextprotocol.io) so that Claude Desktop, Claude Code,
Cursor, and any other MCP client can ask questions of a connected database and get back SQL,
results, a confidence score, and warnings.

The MCP layer is a thin adapter over the same `QueryService` that serves `POST /v1/query`.
Every guardrail applies unchanged: DDL/DML blocking, AST validation, LIMIT enforcement,
EXPLAIN thresholds, hallucination scoring, and the async runtime's concurrency limits. No
validation or execution logic lives in the MCP layer.

```
MCP client --stdio--> src/mcp/server.py --> QueryService.process_question_async --> guardrails --> DB
MCP client --HTTP---> FastAPI app /mcp --> (same FastMCP instance) --> (same path)
```

## Transports

| Transport | How to start | Use for |
| --- | --- | --- |
| stdio | `python -m src.mcp` | Local desktop clients that spawn the server as a subprocess |
| Streamable HTTP | `uvicorn src.api.main:app` then `POST /mcp` | Remote clients; served by the deployed API on the same process and port |

Both transports use the single `FastMCP` instance in [`src/mcp/server.py`](../src/mcp/server.py),
and both share the REST API's module-level `QueryService`, so the schema cache, RAG index,
feedback history, and async runtime queue are shared rather than duplicated.

The HTTP transport is stateless (`stateless_http=True`) and replies with plain JSON
(`json_response=True`). There is no server-side session table, so it can sit behind a load
balancer without sticky routing.

## Tools

| Tool | Arguments | Wraps |
| --- | --- | --- |
| `query` | `question`, `connection_id?`, `session_id?`, `row_limit?` (1-5000), `sql_override?`, `include_meta?` | `QueryService.process_question_async` |
| `get_schema` | `connection_id?`, `refresh?` | `get_schema_summary` / `refresh_schema_summary` |
| `list_connections` | `include_health?` (default true) | `ConnectionService.list_public` plus `connections_health` (secret-free records, same as `GET /v1/connections` and `/v1/connections/health`) |
| `submit_feedback` | `query_id`, `verdict` (`correct` \| `incorrect`), `notes?`, `session_id?` | `QueryService.store_feedback` |
| `get_history` | `session_id?`, `include_meta?` | `QueryService.get_history` |

Every tool returns structured content (a JSON object) plus a JSON text fallback for clients
that do not read structured content.

### Response trimming

`QueryResponse.execution_meta` carries roughly forty trace and latency fields. Returning all
of it on every call would bloat the model's context, so `query` returns a trimmed shape by
default:

```
query_id, connection_id, session_id, sql, explanation, results, confidence, signals,
warnings, accessed, rows_returned, execution_time_ms, validation_level, failure_classification
```

Pass `include_meta: true` to receive the full response including `execution_meta`,
`reasoning`, `linking_meta`, and `constraint_meta`. `get_history` applies the same rule to
each item.

### Errors

MCP has no HTTP status codes, so the structured error bodies the REST handlers return are
serialised into the tool error message instead. Clients see `isError: true` and a message
whose JSON payload matches the REST `detail`:

| REST status | MCP error payload |
| --- | --- |
| 503 | `{"error": "query_runtime_overloaded", "retry_after_seconds": ..., "queue_depth": ..., "capacity": ...}` |
| 504 | `{"error": "query_runtime_timeout", "timeout_stage": ..., "timeout_seconds": ...}` |
| 404 | `{"error": "connection_not_found", "message": ...}` |
| 404 (feedback) | `{"error": "unknown_query_id", "message": ...}` |

Messages are built from the same sanitised fields the REST path uses, so connection URLs
and secrets never appear in them.

Guardrail blocks (for example a `DROP TABLE` request) are **not** errors: they come back as
a normal result with the block described in `warnings` and `rows_returned: 0`, exactly as
`POST /v1/query` does.

### Default connection

When a client omits `connection_id`, the tools use `MCP_DEFAULT_CONNECTION_ID` if set,
otherwise `default` (the same fallback as the REST API). Set it when the `default`
connection is not the one MCP clients should hit, for example when `default` points at a
remote database and a local one is registered under another id:

```
MCP_DEFAULT_CONNECTION_ID=local
```

`list_connections` reports which id is the default (`default_connection_id`, and
`is_default` on each record) and probes each connection so an assistant can pick a healthy
one before asking a question:

```json
{
  "default_connection_id": "local",
  "connections": [
    {"id": "default", "adapter_key": "postgresql", "host": "db.example.com", "is_default": false,
     "health": {"healthy": false, "error": "unreachable"}, "...": "..."},
    {"id": "local", "adapter_key": "postgresql", "host": "localhost", "is_default": true,
     "health": {"healthy": true, "error": ""}, "...": "..."}
  ]
}
```

Each probe is one connection attempt, so an unreachable host costs up to
`DB_CONNECT_TIMEOUT_SECONDS`. Pass `include_health: false` to skip the probe.

## Resources

Resources are read-only context an MCP client can attach to a conversation (Claude Desktop
shows them in the attachment picker). They expose the same information as the tools.

| URI | Content |
| --- | --- |
| `sqlgenx://connections` | Connections, default id, and health. Same as `list_connections`. |
| `schema://{connection_id}` | Tables and columns of one connection, plus its schema fingerprint. Template: substitute an id from `sqlgenx://connections`. |
| `metrics://semantic` | The semantic layer definition from `SEMANTIC_LAYER_PATH`: metrics (id, version, expression, allowed dimensions and filters, owner, sensitivity, example questions), dimensions, filters, entities and approved joins. |

All three are `application/json`. `metrics://semantic` is the most useful one to attach:
questions phrased with those metric names and dimensions ("total revenue by region")
compile to approved SQL deterministically, and the `explanation` field of the `query`
result says when that happened.

## Owner identity

The REST API reads `X-Owner-Id` from a request header. stdio has no headers, so the MCP layer
reads `MCP_OWNER_ID` from the environment (or `.env.local`) and falls back to the demo owner
used when `X-Owner-Id` is absent. It is deliberately not a tool argument: a per-call owner
would let any client impersonate any tenant.

For the HTTP transport this is acceptable for a single-tenant deployment. A multi-tenant HTTP
deployment should put bearer authentication in front of `/mcp` (FastMCP supports a
`TokenVerifier`); that is out of scope for this version.

## Client configuration

### Claude Desktop (stdio)

Add to `claude_desktop_config.json` and restart Claude Desktop:

```json
{
  "mcpServers": {
    "sqlgenx": {
      "command": "/path/to/SQLGENX/.venv/bin/python",
      "args": ["-m", "src.mcp"],
      "cwd": "/path/to/SQLGENX",
      "env": {
        "DATABASE_URL": "postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company",
        "MCP_OWNER_ID": "demo"
      }
    }
  }
}
```

On Windows use `C:\\path\\to\\SQLGENX\\.venv\\Scripts\\python.exe` as the command.

Environment variables placed in `env` override `.env.local`; anything not set there is read
from `.env.local` in `cwd` as usual, so `LLM_PROVIDER` and API keys can stay in that file.

### Claude Code (stdio)

```bash
claude mcp add sqlgenx --cwd /path/to/SQLGENX -- /path/to/SQLGENX/.venv/bin/python -m src.mcp
```

### Any client (streamable HTTP)

Start the API and point the client at the `/mcp` endpoint:

```bash
uvicorn src.api.main:app --host 0.0.0.0 --port 8000
# endpoint: http://localhost:8000/mcp
```

```bash
claude mcp add --transport http sqlgenx http://localhost:8000/mcp
```

### Docker / Compose

The `api` service in `docker-compose.yml` serves the HTTP transport automatically at
`http://localhost:8000/mcp`; no extra service or port is needed. To use stdio from inside
the container instead:

```bash
docker compose exec -i api python -m src.mcp
```

## Manual verification

1. Add the Claude Desktop config above and restart Claude Desktop.
2. Ask "what tables are in the default connection?" and confirm the `get_schema` tool result
   lists tables.
3. Ask "total revenue by region" and confirm the `query` tool result shows `sql`, `results`,
   `confidence`, and `warnings`.
4. Ask "drop the employees table" and confirm the result is a warning about a blocked
   statement with zero rows, not an executed statement.

## Operational notes

- **stdio and stdout.** The JSON-RPC stream runs over stdout. loguru writes to stderr and
  the audit logger writes to `logs/query_audit.log`, so nothing in the import path writes to
  stdout. Keep it that way when adding modules.
- **Concurrency.** The tools call `process_question_async`, never `process_question`, so the
  async runtime's worker and queue limits (`ASYNC_QUERY_MAX_WORKERS`,
  `ASYNC_QUERY_QUEUE_LIMIT`, `ASYNC_QUERY_TOTAL_TIMEOUT_SECONDS`) apply to MCP calls exactly
  as they do to REST calls.
- **Result size.** `row_limit` is capped at 5000 to match `QueryOptions`, and
  `MAX_RESULT_ROWS` still bounds what the executor returns.
- **Host validation.** FastMCP's default DNS-rebinding protection only admits localhost
  `Host` headers, which would break a deployed API. It is disabled on the mounted transport;
  host validation belongs to the reverse proxy, as it does for the REST endpoints.

## Troubleshooting

- **`query_runtime_timeout` on the first call.** The first query on a fresh process loads the
  RAG embedding model and introspects the database, and both count against
  `ASYNC_QUERY_TOTAL_TIMEOUT_SECONDS` (default 30). If the database is unreachable or the
  model has to be downloaded, the call times out and the tool returns the structured
  timeout error above, exactly as `POST /v1/query` returns a 504. Raise the timeout, set
  `RAG_ENABLED=false`, or run one warm-up query before handing the server to a client.
- **`connection_not_found`.** The connection id is not visible to the configured owner.
  Call `list_connections` to see what the server can reach and check `MCP_OWNER_ID`.
- **`UNANSWERABLE` with an empty `get_schema` result.** The connection the tool used is not
  reachable, so the generator saw no tables. Check `health` in `list_connections`, point
  `MCP_DEFAULT_CONNECTION_ID` at a healthy connection or pass `connection_id`, and call
  `get_schema` with `refresh: true` once the database is up (the empty summary is cached for
  `SCHEMA_CACHE_TTL_SECONDS`).
- **Claude Desktop shows the server as disconnected.** Run the same command from a shell
  (`python -m src.mcp`) and read stderr; the JSON-RPC stream is on stdout, so any stray
  stdout output from an imported module breaks the handshake.

## Tests

```bash
pytest -q tests/test_mcp_server.py tests/test_mcp_http_mount.py
```

`tests/test_mcp_server.py` drives the tools through the SDK's in-memory client (no
subprocess, no socket). `tests/test_mcp_http_mount.py` posts JSON-RPC `initialize` and
`tools/list` to `/mcp` through FastAPI's `TestClient`, which runs the app lifespan.
