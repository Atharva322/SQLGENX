# First-Query Schema Preparation Implementation Plan

## Problem

The first SQL generation request can timeout because it performs expensive schema preparation work inline with the user query.

Observed demo failure:

```text
POST /v1/query -> 504 Gateway Timeout
schema_linking_ms: 50716
query_execution_ms: 16
```

The generated SQL and database execution are fast. The timeout is caused by cold schema/RAG/linking work being charged to the first user-facing query.

## Goal

Move schema preparation out of `POST /v1/query` and into explicit connection lifecycle events:

- application startup for the default connection
- connection test/save
- connection selection
- schema refresh

The SQL generation path should use prepared schema context and fail fast with a clear `schema_not_ready` response when context is missing or stale.

## Non-Goals

- Do not weaken SQL safety, AST validation, guardrails, or adapter release gates.
- Do not remove schema linking or RAG permanently.
- Do not turn deterministic demo timing into a production latency claim.
- Do not expose credentials, full URLs, or raw driver errors in readiness responses.

## Target UX

Current:

```text
User clicks Generate SQL
-> schema introspection
-> RAG cold start
-> schema linking
-> SQL generation
-> validation
-> execution
-> possible 504
```

Target:

```text
App loads or connection selected
-> prepare schema context
-> UI shows "Schema ready"
-> user clicks Generate SQL
-> SQL generation, validation, execution
```

If context is not ready:

```text
Generate SQL disabled
```

or backend response:

```http
409 Conflict
{
  "error": "schema_not_ready",
  "connection_id": "default",
  "message": "Schema context is still preparing. Retry after preparation completes."
}
```

## Backend Design

### 1. Add Preparation Models

Add models similar to:

```python
class ConnectionPrepareResponse(BaseModel):
    connection_id: str
    owner_id: str
    ready: bool
    status: Literal["not_started", "preparing", "ready", "failed", "stale"]
    schema_fingerprint: str | None = None
    table_count: int = 0
    prepared_at: str | None = None
    elapsed_ms: int | None = None
    safe_error_code: ConnectionErrorCode | None = None
```

Recommended location:

```text
src/connections/models.py
```

### 2. Add Context Preparation Service

Add a service that prepares and tracks schema context readiness.

Recommended file:

```text
src/connections/prepare.py
```

Responsibilities:

- resolve exact connection ID
- introspect schema through existing adapter/schema path
- compute or reuse schema fingerprint
- load feedback examples
- prewarm RAG retrieval if enabled
- run schema-linking prewarm using deterministic warmup prompts
- store readiness by owner, connection, version, schema fingerprint
- sanitize failures

Suggested cache key:

```text
owner_id + connection_id + connection_version + schema_fingerprint
```

Suggested in-memory status shape:

```python
PreparedContextState(
    owner_id="demo-user",
    connection_id="default",
    connection_version=1,
    schema_fingerprint="...",
    status="ready",
    prepared_at="...",
    elapsed_ms=1234,
)
```

This can start as an in-memory implementation, matching the current runtime connection repository style. A persistent or distributed cache can come later.

### 3. Add Backend Endpoints

Add:

```text
POST /v1/connections/{connection_id}/prepare
GET /v1/connections/{connection_id}/prepare
```

Behavior:

- `POST` starts preparation synchronously for now, returning readiness metadata.
- `GET` returns current readiness state.
- Both use `x-owner-id` like existing connection endpoints.
- Unknown connection IDs return `404`.
- Unauthorized runtime connections return `404` or sanitized access denial, matching existing ownership policy.

Recommended location:

```text
src/api/main.py
```

### 4. Integrate With Existing Workflows

Call prepare after:

```text
POST /v1/connections/test
POST /v1/connections
PATCH /v1/connections/{id}
POST /v1/connections/{id}/schema/refresh
```

Important nuance:

- `test` can prepare a temporary schema context only if the test succeeds.
- `create` and `update` should prepare the saved version.
- `schema/refresh` must invalidate the previous readiness state and prepare the new fingerprint.
- `delete` must remove readiness state for the deleted connection.

### 5. Prepare Default Connection On Startup

For the default/demo environment connection:

```text
FastAPI startup
-> prepare default connection in background
```

Keep startup resilient:

- do not crash the app if preparation fails
- store status as `failed`
- surface sanitized error code through readiness endpoint

Implementation options:

- synchronous startup for local/demo only
- background task on startup
- first UI load triggers prepare

Recommended first step:

Use UI-triggered prepare plus a manual CLI/prewarm command. Add startup background prepare only after the endpoint is stable.

### 6. Guard `/v1/query`

Before expensive query processing:

```python
state = prepare_service.get_state(owner_id, connection_id)
if settings.require_prepared_schema_context and not state.ready:
    raise HTTPException(status_code=409, detail={...})
```

Add setting:

```text
REQUIRE_PREPARED_SCHEMA_CONTEXT=false
```

Rollout:

1. Default `false` to preserve current behavior.
2. Enable `true` for demo/staging.
3. Enable later in production once frontend readiness UX exists.

## Frontend Design

### 1. Add API Wrappers

Recommended file:

```text
lib/genxsql-api.ts
```

Add:

```ts
prepareConnection(ownerId, connectionId)
fetchConnectionPrepareStatus(ownerId, connectionId)
```

Add Next routes:

```text
app/api/connections/[connectionId]/prepare/route.ts
```

Methods:

```text
POST -> FastAPI POST /v1/connections/{id}/prepare
GET  -> FastAPI GET /v1/connections/{id}/prepare
```

### 2. Add UI State

In `components/chat-console.tsx`, track:

```ts
schemaReady: boolean
schemaPreparing: boolean
schemaPrepareError?: string
```

On app load:

```text
load connections
select default
prepare selected connection
```

On connection switch:

```text
clear generated SQL/results
set preparing
POST prepare
set ready
```

On schema refresh:

```text
POST refresh
POST prepare
```

### 3. Disable Generate Until Ready

Update Generate button disabled condition:

```text
loading || question too short || no selected connection || schemaPreparing || !schemaReady
```

Status copy:

```text
Preparing schema...
Schema ready
Schema preparation failed
```

Do not display implementation details like RAG, embeddings, or cache internals in the main UI.

## RAG/Schema Linking Prewarm Strategy

Preparation should do real work but avoid fake user queries.

Suggested steps:

1. `get_schema_summary(connection_id, owner_id)`
2. load prompt assets and feedback examples
3. if `RAG_ENABLED=true`, call `retrieve_context` with warmup prompts:

```text
List departments
Show total sales by region
Count employees by department
```

4. run `run_schema_linking` for those prompts to warm tokenization, model imports, embedding cache, and schema scoring

The warmup prompts should be generic and never executed as SQL.

## Settings

Add:

```text
REQUIRE_PREPARED_SCHEMA_CONTEXT=false
PREPARE_DEFAULT_CONNECTION_ON_STARTUP=false
SCHEMA_PREPARE_WARMUP_PROMPTS=List departments|Show total sales by region|Count employees by department
SCHEMA_PREPARE_TIMEOUT_SECONDS=120
```

Demo settings:

```powershell
$env:REQUIRE_PREPARED_SCHEMA_CONTEXT="true"
$env:SCHEMA_PREPARE_TIMEOUT_SECONDS="120"
$env:ASYNC_QUERY_TOTAL_TIMEOUT_SECONDS="120"
```

If the demo machine still has slow local embedding startup:

```powershell
$env:RAG_ENABLED="false"
```

## API Contract

### POST Prepare

Request:

```http
POST /v1/connections/default/prepare
x-owner-id: demo-user
```

Response:

```json
{
  "connection_id": "default",
  "owner_id": "demo-user",
  "ready": true,
  "status": "ready",
  "schema_fingerprint": "02c25b4710b8b14c",
  "table_count": 12,
  "prepared_at": "2026-08-22T20:20:00Z",
  "elapsed_ms": 1840,
  "safe_error_code": null
}
```

### GET Prepare Status

Request:

```http
GET /v1/connections/default/prepare
x-owner-id: demo-user
```

Response before prepare:

```json
{
  "connection_id": "default",
  "owner_id": "demo-user",
  "ready": false,
  "status": "not_started",
  "schema_fingerprint": null,
  "table_count": 0,
  "prepared_at": null,
  "elapsed_ms": null,
  "safe_error_code": null
}
```

## Tests

### Backend Unit Tests

Add tests for:

- prepare unknown connection returns not found
- prepare default connection returns ready with schema fingerprint
- prepare response contains no password or full URL
- readiness cache key includes owner, connection, version, and fingerprint
- deleting a connection clears readiness
- schema refresh invalidates stale readiness
- `/v1/query` returns `409 schema_not_ready` when required and not prepared
- `/v1/query` proceeds when prepared

Suggested files:

```text
tests/test_connection_prepare.py
tests/test_query_endpoint.py
```

### Integration Tests

Add PostgreSQL integration:

```text
tests/integration/test_connection_prepare_postgres.py
```

Cases:

- prepare against real PostgreSQL
- first query after prepare completes under threshold
- schema refresh changes or preserves fingerprint correctly
- no unsafe execution occurs during prepare

### Frontend Tests

Add component/API route tests:

- app load triggers prepare for selected connection
- Generate disabled while preparing
- Generate enabled after ready
- switching connection clears generated SQL and prepares new connection
- prepare failure shows a non-secret error

### E2E Tests

Add Playwright flow:

```text
load app
wait for Schema ready
generate "List departments"
assert no schema_not_ready
assert generated SQL/result visible
switch connection if available
assert generated SQL cleared
```

## Observability

Add execution events:

```text
schema_prepare_started
schema_prepare_completed
schema_prepare_failed
schema_prepare_cache_hit
schema_prepare_invalidated
query_blocked_schema_not_ready
```

Record:

- connection ID
- owner ID only if existing logs already allow it
- schema fingerprint
- elapsed ms
- status
- safe error code

Never record:

- password
- full URL
- raw driver exception text
- API keys

## Rollout Plan

### Step 1: Backend Prepare Endpoint

- Add models and in-memory readiness service.
- Add `POST` and `GET` prepare endpoints.
- Add backend tests.
- Keep `/v1/query` behavior unchanged.

### Step 2: Frontend Readiness UX

- Add Next API route wrappers.
- Add prepare state to chat console.
- Disable Generate until ready.
- Add frontend tests.

### Step 3: Query Guard

- Add `REQUIRE_PREPARED_SCHEMA_CONTEXT`.
- Return `409 schema_not_ready` when enabled and state is not ready.
- Enable in demo config first.

### Step 4: Lifecycle Integration

- Trigger prepare after save/update/refresh.
- Invalidate on delete/update/schema fingerprint change.
- Add integration tests.

### Step 5: Optional Startup Prewarm

- Add `PREPARE_DEFAULT_CONNECTION_ON_STARTUP`.
- Start with disabled default.
- Enable only in demo/staging once stable.

## Demo Commands After Implementation

Backend:

```powershell
$env:DATABASE_URL="postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company"
$env:CONNECTION_URLS_JSON='{"default":"postgresql://text2sql_user:text2sql_pass@localhost:5432/sample_company"}'
$env:LLM_PROVIDER="deterministic"
$env:LLM_MODEL="sqlgenx-fixture-v1"
$env:RAG_EMBEDDING_LOCAL_ONLY="true"
$env:REQUIRE_PREPARED_SCHEMA_CONTEXT="true"
$env:SCHEMA_PREPARE_TIMEOUT_SECONDS="120"
$env:ASYNC_QUERY_TOTAL_TIMEOUT_SECONDS="120"
.venv\Scripts\python.exe -m uvicorn src.api.main:app --host 127.0.0.1 --port 8000
```

Manual prepare:

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://127.0.0.1:8000/v1/connections/default/prepare `
  -Headers @{"x-owner-id"="demo-user"}
```

Frontend:

```powershell
$env:AUTH_DEV_BYPASS="true"
$env:GENXSQL_API_BASE_URL="http://127.0.0.1:8000"
$env:NEXT_PUBLIC_GENXSQL_API_BASE_URL="http://127.0.0.1:8000"
npm run dev -- --hostname 127.0.0.1 --port 3000
```

## Acceptance Criteria

- First visible user query no longer performs cold schema/RAG setup.
- UI clearly shows schema preparation state.
- Generate is disabled until selected connection is ready.
- `/v1/query` can fail fast with `409 schema_not_ready` when strict readiness is enabled.
- Preparation never executes generated SQL.
- Preparation responses and logs are sanitized.
- Schema refresh and connection updates invalidate readiness.
- PostgreSQL and MySQL verified adapter gates still pass.
- Existing safety thresholds remain unchanged.

## Risks

- Local embedding/model import can still be slow during prepare.
- In-memory readiness state will reset on server restart.
- Multi-worker deployments need shared readiness/cache state.
- Startup prewarm can hide failures if not surfaced in health/readiness APIs.

## Recommendation

Implement this as a dedicated phase before adding more adapter work. It improves demo reliability and product UX without changing SQL safety policy or adapter verification status.
