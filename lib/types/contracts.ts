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
  conversationId: string;
  generatedSql: string;
  explanation: string;
  confidence: number;
  referenced: ReferencedSchema;
  safety: {
    status: SafetyStatus;
    reasons: string[];
  };
}

export interface ExecuteSqlRequest {
  queryId: string;
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

export interface HistoryEntry {
  queryId: string;
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
