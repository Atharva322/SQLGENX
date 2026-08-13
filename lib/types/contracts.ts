export type SafetyStatus = "safe" | "warning" | "blocked";

export interface GenerateSqlRequest {
  question: string;
  connectionId: string;
  conversationId?: string;
}

export interface ReferencedSchema {
  tables: string[];
  columns: string[];
}

export interface GenerateSqlResponse {
  queryId: string;
  connectionId: string;
  conversationId: string;
  generatedSql: string;
  explanation: string;
  confidence: number;
  referenced: ReferencedSchema;
  safety: {
    status: SafetyStatus;
    reasons: string[];
  };
  preExecuted?: ExecuteSqlResponse;
}

export interface ExecuteSqlRequest {
  queryId: string;
  connectionId: string;
  sql: string;
}

export interface ExecuteSqlResponse {
  queryId: string;
  status: "success" | "error";
  rows: Record<string, unknown>[];
  rowCount: number;
  executionMs: number;
  error?: string;
}

export interface SchemaColumn {
  name: string;
  type: string;
}

export interface SchemaTable {
  table: string;
  columns: SchemaColumn[];
}

export interface SchemaContext {
  database: string;
  refreshedAt: string;
  tables: SchemaTable[];
}

export interface PublicConnection {
  id: string;
  displayName: string;
  adapterKey: string;
  dialect: string;
  host?: string | null;
  port?: number | null;
  database?: string | null;
  username?: string | null;
  tlsMode?: string | null;
  verificationState: string;
  healthState: string;
  schemaFingerprint?: string | null;
  safeErrorCode?: string | null;
  version: number;
  createdAt: string;
  updatedAt: string;
}

export interface AdapterCatalogItem {
  key: string;
  displayName: string;
  releaseState: "hidden" | "experimental" | "verified";
  defaultPort?: number | null;
  capabilities: Record<string, unknown>;
}

export interface RuntimeConnectionConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
  tlsMode: "disable" | "prefer" | "require" | "verify-ca" | "verify-full";
}

export type RuntimeAdapterKey = "postgresql" | "mysql";

export interface ConnectionTestResult {
  ok: boolean;
  safeErrorCode?: string | null;
  schemaFingerprint?: string | null;
}

export interface HistoryEntry {
  queryId: string;
  connectionId: string;
  connectionVersion?: number;
  conversationId: string;
  userId: string;
  question: string;
  generatedSql: string;
  explanation: string;
  confidence: number;
  safetyStatus: SafetyStatus;
  safetyReasons: string[];
  approved: boolean;
  executed: boolean;
  executionStatus?: "success" | "error";
  executionMs?: number;
  error?: string;
  createdAt: string;
  updatedAt: string;
}
