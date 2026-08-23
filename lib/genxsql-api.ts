import type {
  AdapterCatalogItem,
  ConnectionPrepareStatus,
  ConnectionTestResult,
  GenerateSqlResponse,
  PublicConnection,
  RuntimeAdapterKey,
  RuntimeConnectionConfig,
  SchemaContext
} from "@/lib/types/contracts";

type UpstreamConnection = {
  id: string;
  display_name: string;
  adapter_key: string;
  dialect: string;
  host?: string | null;
  port?: number | null;
  database?: string | null;
  username?: string | null;
  tls_mode?: string | null;
  verification_state: string;
  health_state: string;
  schema_fingerprint?: string | null;
  safe_error_code?: string | null;
  version?: number;
  created_at: string;
  updated_at: string;
};

type UpstreamAdapter = {
  key: string;
  display_name: string;
  release_state: "hidden" | "experimental" | "verified";
  default_port?: number | null;
  capabilities?: Record<string, unknown>;
};

type UpstreamPrepareStatus = {
  connection_id: string;
  owner_id: string;
  ready: boolean;
  status: "not_started" | "preparing" | "ready" | "failed" | "stale";
  schema_fingerprint?: string | null;
  table_count: number;
  prepared_at?: string | null;
  elapsed_ms?: number | null;
  safe_error_code?: string | null;
};

export function genxsqlApiBaseUrl(): string {
  const configured = process.env.GENXSQL_API_BASE_URL ?? process.env.NEXT_PUBLIC_GENXSQL_API_BASE_URL;
  if (!configured) {
    throw new Error("GENXSQL_API_BASE_URL is required. FastAPI is the single database authority.");
  }
  return configured;
}

export function ownerHeaders(ownerId: string): HeadersInit {
  return { "content-type": "application/json", "x-owner-id": ownerId };
}

export async function fetchConnections(ownerId: string): Promise<PublicConnection[]> {
  const response = await fetch(new URL("/v1/connections", genxsqlApiBaseUrl()), {
    headers: ownerHeaders(ownerId),
    cache: "no-store"
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to load connections."));
  }
  return (Array.isArray(data.connections) ? data.connections : []).map(mapConnection);
}

export async function fetchAdapters(ownerId: string): Promise<AdapterCatalogItem[]> {
  const response = await fetch(new URL("/v1/adapters", genxsqlApiBaseUrl()), {
    headers: ownerHeaders(ownerId),
    cache: "no-store"
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to load adapters."));
  }
  return (Array.isArray(data.adapters) ? (data.adapters as UpstreamAdapter[]) : []).map((adapter) => ({
    key: String(adapter.key),
    displayName: String(adapter.display_name),
    releaseState: adapter.release_state,
    defaultPort: adapter.default_port ?? null,
    capabilities: adapter.capabilities ?? {}
  }));
}

export async function testConnection(
  ownerId: string,
  adapterKey: RuntimeAdapterKey,
  config: RuntimeConnectionConfig
): Promise<ConnectionTestResult> {
  const response = await fetch(new URL("/v1/connections/test", genxsqlApiBaseUrl()), {
    method: "POST",
    headers: ownerHeaders(ownerId),
    body: JSON.stringify({ adapter_key: adapterKey, config: toConnectionConfig(config) })
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Connection test failed."));
  }
  return {
    ok: Boolean(data.ok),
    safeErrorCode: data.safe_error_code ?? null,
    schemaFingerprint: data.schema_fingerprint ?? null
  };
}

export async function createConnection(
  ownerId: string,
  adapterKey: RuntimeAdapterKey,
  id: string,
  displayName: string,
  config: RuntimeConnectionConfig
): Promise<PublicConnection> {
  const response = await fetch(new URL("/v1/connections", genxsqlApiBaseUrl()), {
    method: "POST",
    headers: ownerHeaders(ownerId),
    body: JSON.stringify({
      id,
      display_name: displayName,
      adapter_key: adapterKey,
      config: toConnectionConfig(config)
    })
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to save connection."));
  }
  return mapConnection(data);
}

export async function updateConnection(
  ownerId: string,
  id: string,
  displayName: string,
  config?: RuntimeConnectionConfig
): Promise<PublicConnection> {
  const payload: Record<string, unknown> = { display_name: displayName };
  if (config) {
    payload.config = toConnectionConfig(config);
  }
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}`, genxsqlApiBaseUrl()), {
    method: "PATCH",
    headers: ownerHeaders(ownerId),
    body: JSON.stringify(payload)
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to update connection."));
  }
  return mapConnection(data);
}

export async function deleteConnection(ownerId: string, id: string): Promise<PublicConnection> {
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}`, genxsqlApiBaseUrl()), {
    method: "DELETE",
    headers: ownerHeaders(ownerId)
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to delete connection."));
  }
  return mapConnection(data);
}

export async function refreshConnectionSchema(ownerId: string, id: string): Promise<SchemaContext> {
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}/schema/refresh`, genxsqlApiBaseUrl()), {
    method: "POST",
    headers: ownerHeaders(ownerId)
  });
  return schemaFromResponse(response, id, "Failed to refresh schema.");
}

export async function fetchConnectionSchema(ownerId: string, id: string): Promise<SchemaContext> {
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}/schema`, genxsqlApiBaseUrl()), {
    headers: ownerHeaders(ownerId),
    cache: "no-store"
  });
  return schemaFromResponse(response, id, "Failed to load schema.");
}

export async function prepareConnection(ownerId: string, id: string): Promise<ConnectionPrepareStatus> {
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}/prepare`, genxsqlApiBaseUrl()), {
    method: "POST",
    headers: ownerHeaders(ownerId)
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to prepare schema."));
  }
  return mapPrepareStatus(data as UpstreamPrepareStatus);
}

export async function fetchConnectionPrepareStatus(ownerId: string, id: string): Promise<ConnectionPrepareStatus> {
  const response = await fetch(new URL(`/v1/connections/${encodeURIComponent(id)}/prepare`, genxsqlApiBaseUrl()), {
    headers: ownerHeaders(ownerId),
    cache: "no-store"
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "Failed to load schema preparation status."));
  }
  return mapPrepareStatus(data as UpstreamPrepareStatus);
}

export async function runGenxsqlQuery(
  ownerId: string,
  question: string,
  connectionId: string,
  conversationId: string
): Promise<GenerateSqlResponse> {
  const response = await fetch(new URL("/v1/query", genxsqlApiBaseUrl()), {
    method: "POST",
    headers: ownerHeaders(ownerId),
    body: JSON.stringify({
      question,
      connection_id: connectionId,
      session_id: conversationId
    })
  });
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, "GENXSQL API query failed."));
  }
  const tables = Array.isArray(data.accessed?.tables) ? data.accessed.tables.map(String) : [];
  const columns = Array.isArray(data.accessed?.columns) ? data.accessed.columns.map(String) : [];
  const warnings = Array.isArray(data.warnings) ? data.warnings.map(String) : [];
  const blocked = data.sql === "UNANSWERABLE";
  const queryId = typeof data.query_id === "string" ? data.query_id : "";
  const rows = Array.isArray(data.results) ? data.results : [];

  return {
    queryId,
    connectionId,
    conversationId,
    generatedSql: String(data.sql ?? "UNANSWERABLE"),
    explanation: String(data.explanation ?? "Generated through the GENXSQL QueryService API."),
    confidence: typeof data.confidence === "number" ? data.confidence : 0,
    referenced: { tables, columns },
    safety: {
      status: blocked ? "blocked" : warnings.length > 0 ? "warning" : "safe",
      reasons: warnings
    },
    preExecuted: {
      queryId,
      status: blocked ? "error" : "success",
      rows,
      rowCount: rows.length,
      executionMs: Number(data.execution_meta?.execution_time_ms ?? 0),
      error: blocked ? "GENXSQL returned UNANSWERABLE." : undefined
    }
  };
}

async function schemaFromResponse(response: Response, connectionId: string, fallback: string): Promise<SchemaContext> {
  const data = await safeJson(response);
  if (!response.ok) {
    throw new Error(readError(data, fallback));
  }
  return {
    database: connectionId,
    refreshedAt: new Date().toISOString(),
    tables: Array.isArray(data.tables) ? data.tables : []
  };
}

async function safeJson(response: Response): Promise<any> {
  try {
    return await response.json();
  } catch {
    return {};
  }
}

function readError(data: any, fallback: string): string {
  if (typeof data?.detail?.message === "string") {
    return data.detail.message;
  }
  if (typeof data?.detail?.error === "string") {
    return data.detail.error;
  }
  if (typeof data?.error === "string") {
    return data.error;
  }
  return fallback;
}

function mapConnection(connection: UpstreamConnection): PublicConnection {
  return {
    id: connection.id,
    displayName: connection.display_name,
    adapterKey: connection.adapter_key,
    dialect: connection.dialect,
    host: connection.host ?? null,
    port: connection.port ?? null,
    database: connection.database ?? null,
    username: connection.username ?? null,
    tlsMode: connection.tls_mode ?? null,
    verificationState: connection.verification_state,
    healthState: connection.health_state,
    schemaFingerprint: connection.schema_fingerprint ?? null,
    safeErrorCode: connection.safe_error_code ?? null,
    version: connection.version ?? 1,
    createdAt: connection.created_at,
    updatedAt: connection.updated_at
  };
}

function mapPrepareStatus(status: UpstreamPrepareStatus): ConnectionPrepareStatus {
  return {
    connectionId: status.connection_id,
    ownerId: status.owner_id,
    ready: Boolean(status.ready),
    status: status.status,
    schemaFingerprint: status.schema_fingerprint ?? null,
    tableCount: Number(status.table_count ?? 0),
    preparedAt: status.prepared_at ?? null,
    elapsedMs: typeof status.elapsed_ms === "number" ? status.elapsed_ms : null,
    safeErrorCode: status.safe_error_code ?? null
  };
}

function toConnectionConfig(config: RuntimeConnectionConfig): Record<string, unknown> {
  return {
    host: config.host,
    port: config.port,
    database: config.database,
    username: config.username,
    password: config.password,
    tls_mode: config.tlsMode,
    charset: "utf8mb4"
  };
}
