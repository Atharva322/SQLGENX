"use client";

import React, { useEffect, useMemo, useState } from "react";
import type {
  ConnectionTestResult,
  ExecuteSqlResponse,
  GenerateSqlResponse,
  PostgresConnectionConfig,
  PublicConnection
} from "@/lib/types/contracts";

interface Message {
  role: "user" | "assistant";
  text: string;
}

const PAGE_SIZE = 10;
const DEFAULT_FORM: PostgresConnectionConfig = {
  host: "localhost",
  port: 5432,
  database: "sample_company",
  username: "text2sql_user",
  password: "",
  tlsMode: "prefer"
};

function downloadCsv(rows: Record<string, unknown>[]): void {
  if (rows.length === 0) {
    return;
  }
  const headers = Object.keys(rows[0]);
  const lines = [
    headers.join(","),
    ...rows.map((row) =>
      headers
        .map((key) => {
          const val = row[key];
          const escaped = String(val ?? "").replaceAll('"', '""');
          return `"${escaped}"`;
        })
        .join(",")
    )
  ];
  const blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = `genxsql-${Date.now()}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

export function ChatConsole(): React.JSX.Element {
  const [question, setQuestion] = useState("");
  const [conversationId, setConversationId] = useState<string>();
  const [messages, setMessages] = useState<Message[]>([]);
  const [connections, setConnections] = useState<PublicConnection[]>([]);
  const [selectedConnectionId, setSelectedConnectionId] = useState("default");
  const [connectionForm, setConnectionForm] = useState({ id: "", displayName: "", ...DEFAULT_FORM });
  const [testedConfigKey, setTestedConfigKey] = useState<string>();
  const [testResult, setTestResult] = useState<ConnectionTestResult>();
  const [connectionBusy, setConnectionBusy] = useState(false);
  const [generated, setGenerated] = useState<GenerateSqlResponse | null>(null);
  const [executed, setExecuted] = useState<ExecuteSqlResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [running, setRunning] = useState(false);
  const [error, setError] = useState<string>();
  const [page, setPage] = useState(0);

  const pagedRows = useMemo(() => {
    if (!executed?.rows) {
      return [];
    }
    const start = page * PAGE_SIZE;
    return executed.rows.slice(start, start + PAGE_SIZE);
  }, [executed, page]);

  const selectedConnection = useMemo(
    () => connections.find((connection) => connection.id === selectedConnectionId),
    [connections, selectedConnectionId]
  );
  const configKey = JSON.stringify({
    host: connectionForm.host,
    port: Number(connectionForm.port),
    database: connectionForm.database,
    username: connectionForm.username,
    password: connectionForm.password,
    tlsMode: connectionForm.tlsMode
  });
  const canSaveConnection =
    connectionForm.id.trim().length >= 3 &&
    connectionForm.displayName.trim().length > 0 &&
    connectionForm.password.length > 0 &&
    testResult?.ok === true &&
    testedConfigKey === configKey;

  useEffect(() => {
    loadConnections().catch((err) => {
      setConnections([
        {
          id: "default",
          displayName: "Default",
          adapterKey: "postgresql",
          dialect: "postgres",
          verificationState: "unknown",
          healthState: "unknown",
          version: 1,
          createdAt: "",
          updatedAt: ""
        }
      ]);
      setError(err instanceof Error ? err.message : "Failed to load connections.");
    });
  }, []);

  async function loadConnections(nextSelectedId?: string): Promise<void> {
    const response = await fetch("/api/connections", { cache: "no-store" });
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.error ?? "Failed to load connections.");
    }
    const loaded = Array.isArray(data.connections) ? (data.connections as PublicConnection[]) : [];
    setConnections(loaded);
    const preferred = nextSelectedId ?? selectedConnectionId;
    if (loaded.some((connection) => connection.id === preferred)) {
      setSelectedConnectionId(preferred);
    } else if (loaded.length > 0) {
      setSelectedConnectionId(loaded[0].id);
    }
  }

  function onSelectConnection(connectionId: string): void {
    setSelectedConnectionId(connectionId);
    setGenerated(null);
    setExecuted(null);
    setPage(0);
  }

  async function onTestConnection(): Promise<void> {
    setConnectionBusy(true);
    setError(undefined);
    try {
      const response = await fetch("/api/connections/test", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ config: connectionForm })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Connection test failed.");
      }
      setTestResult(data as ConnectionTestResult);
      setTestedConfigKey(configKey);
    } catch (err) {
      setTestResult({ ok: false, safeErrorCode: "unreachable" });
      setError(err instanceof Error ? err.message : "Connection test failed.");
    } finally {
      setConnectionBusy(false);
    }
  }

  async function onSaveConnection(): Promise<void> {
    setConnectionBusy(true);
    setError(undefined);
    try {
      const response = await fetch("/api/connections", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          id: connectionForm.id,
          displayName: connectionForm.displayName,
          config: connectionForm
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Failed to save connection.");
      }
      setConnectionForm({ id: "", displayName: "", ...DEFAULT_FORM });
      setTestResult(undefined);
      setTestedConfigKey(undefined);
      await loadConnections((data as PublicConnection).id);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save connection.");
    } finally {
      setConnectionBusy(false);
    }
  }

  async function onRefreshSchema(): Promise<void> {
    setConnectionBusy(true);
    setError(undefined);
    try {
      const response = await fetch(`/api/connections/${encodeURIComponent(selectedConnectionId)}/schema/refresh`, {
        method: "POST"
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Failed to refresh schema.");
      }
      await loadConnections(selectedConnectionId);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to refresh schema.");
    } finally {
      setConnectionBusy(false);
    }
  }

  async function onDeleteSelected(): Promise<void> {
    if (selectedConnectionId === "default") {
      return;
    }
    setConnectionBusy(true);
    setError(undefined);
    try {
      const response = await fetch(`/api/connections/${encodeURIComponent(selectedConnectionId)}`, {
        method: "DELETE"
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Failed to delete connection.");
      }
      setGenerated(null);
      setExecuted(null);
      await loadConnections("default");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete connection.");
    } finally {
      setConnectionBusy(false);
    }
  }

  async function onGenerate(): Promise<void> {
    setLoading(true);
    setError(undefined);
    setExecuted(null);
    try {
      const response = await fetch("/api/chat/generate-sql", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          question,
          connectionId: selectedConnectionId,
          conversationId
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Generation failed");
      }
      const payload = data as GenerateSqlResponse;
      setGenerated(payload);
      setExecuted(payload.preExecuted ?? null);
      setConversationId(payload.conversationId);
      setMessages((prev) => [...prev, { role: "user", text: question }, { role: "assistant", text: payload.explanation }]);
      setQuestion("");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Generation failed");
    } finally {
      setLoading(false);
    }
  }

  async function onExecute(): Promise<void> {
    if (!generated) {
      return;
    }
    setRunning(true);
    setError(undefined);
    try {
      const response = await fetch("/api/chat/execute-sql", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          queryId: generated.queryId,
          connectionId: generated.connectionId,
          sql: generated.generatedSql
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Execution failed");
      }
      setExecuted(data as ExecuteSqlResponse);
      setPage(0);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Execution failed");
    } finally {
      setRunning(false);
    }
  }

  return (
    <div className="stack">
      <div className="card">
        <h1 style={{ marginTop: 0 }}>Enterprise NL to SQL Assistant</h1>
        <p style={{ color: "var(--ink-soft)" }}>
          Ask in plain English. Review generated SQL against the connected GENXSQL schema.
        </p>
        <div className="toolbar">
          <label>
            Connection
            <select value={selectedConnectionId} onChange={(e) => onSelectConnection(e.target.value)}>
              {connections.map((connection) => (
                <option key={connection.id} value={connection.id}>
                  {connection.displayName} ({connection.id})
                </option>
              ))}
            </select>
          </label>
          <span className={`pill ${selectedConnection?.verificationState === "verified" ? "ok" : "warn"}`}>
            {selectedConnection?.verificationState ?? "unknown"}
          </span>
          <span style={{ color: "var(--ink-soft)" }}>
            {selectedConnection?.adapterKey ?? "adapter"} · v{selectedConnection?.version ?? 1}
          </span>
          <button className="secondary" disabled={connectionBusy} onClick={onRefreshSchema}>
            Refresh Schema
          </button>
          <button className="secondary" disabled={connectionBusy || selectedConnectionId === "default"} onClick={onDeleteSelected}>
            Delete
          </button>
        </div>
        <div className="stack">
          <textarea
            rows={4}
            placeholder="Example: Show top 10 customers by total order value in the last 90 days."
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
          />
          <div className="row">
            <button disabled={loading || question.trim().length < 3 || !selectedConnectionId} onClick={onGenerate}>
              {loading ? "Generating..." : "Generate SQL"}
            </button>
            {conversationId ? <span style={{ color: "var(--ink-soft)" }}>Conversation: {conversationId}</span> : null}
          </div>
        </div>
      </div>

      <div className="card stack">
        <strong>Add PostgreSQL Connection</strong>
        <div className="form-grid">
          <input
            placeholder="Connection ID"
            value={connectionForm.id}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, id: e.target.value }));
              setTestResult(undefined);
            }}
          />
          <input
            placeholder="Display name"
            value={connectionForm.displayName}
            onChange={(e) => setConnectionForm((form) => ({ ...form, displayName: e.target.value }))}
          />
          <input
            placeholder="Host"
            value={connectionForm.host}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, host: e.target.value }));
              setTestResult(undefined);
            }}
          />
          <input
            type="number"
            placeholder="Port"
            value={connectionForm.port}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, port: Number(e.target.value) }));
              setTestResult(undefined);
            }}
          />
          <input
            placeholder="Database"
            value={connectionForm.database}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, database: e.target.value }));
              setTestResult(undefined);
            }}
          />
          <input
            placeholder="Username"
            value={connectionForm.username}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, username: e.target.value }));
              setTestResult(undefined);
            }}
          />
          <input
            type="password"
            placeholder="Password"
            value={connectionForm.password}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, password: e.target.value }));
              setTestResult(undefined);
            }}
          />
          <select
            value={connectionForm.tlsMode}
            onChange={(e) => {
              setConnectionForm((form) => ({ ...form, tlsMode: e.target.value as PostgresConnectionConfig["tlsMode"] }));
              setTestResult(undefined);
            }}
          >
            <option value="prefer">Prefer TLS</option>
            <option value="require">Require TLS</option>
            <option value="disable">Disable TLS</option>
            <option value="verify-ca">Verify CA</option>
            <option value="verify-full">Verify Full</option>
          </select>
        </div>
        <div className="row">
          <button className="secondary" disabled={connectionBusy} onClick={onTestConnection}>
            {connectionBusy ? "Testing..." : "Test Connection"}
          </button>
          <button disabled={!canSaveConnection || connectionBusy} onClick={onSaveConnection}>
            Save Connection
          </button>
          {testResult ? (
            <span className={`pill ${testResult.ok ? "ok" : "error"}`}>
              {testResult.ok ? "test passed" : testResult.safeErrorCode ?? "test failed"}
            </span>
          ) : null}
        </div>
      </div>

      {error ? (
        <div className="card">
          <span className="pill error">Error</span>
          <p>{error}</p>
        </div>
      ) : null}

      {generated ? (
        <div className="card stack">
          <div className="row">
            <strong>Generated SQL</strong>
            <span className={`pill ${generated.safety.status === "safe" ? "ok" : generated.safety.status === "warning" ? "warn" : "error"}`}>
              {generated.safety.status}
            </span>
            <span style={{ color: "var(--ink-soft)" }}>Confidence: {(generated.confidence * 100).toFixed(0)}%</span>
            <span style={{ color: "var(--ink-soft)" }}>Connection: {generated.connectionId}</span>
          </div>
          <pre>{generated.generatedSql}</pre>
          {generated.safety.reasons.length > 0 ? (
            <div>
              <strong>Safety reasons</strong>
              <ul>
                {generated.safety.reasons.map((r) => (
                  <li key={r}>{r}</li>
                ))}
              </ul>
            </div>
          ) : null}
          <div>
            <strong>Referenced tables:</strong> {generated.referenced.tables.join(", ") || "None"}
          </div>
          <div>
            <strong>Referenced columns:</strong> {generated.referenced.columns.join(", ") || "None"}
          </div>
          <div className="row">
            <button disabled={generated.safety.status !== "safe" || running} onClick={onExecute}>
              {running ? "Running..." : "Run Query"}
            </button>
            <button className="secondary" onClick={() => setGenerated(null)}>
              Clear SQL
            </button>
          </div>
        </div>
      ) : null}

      {executed ? (
        <div className="card stack">
          <div className="row">
            <strong>Results</strong>
            <span style={{ color: "var(--ink-soft)" }}>
              {executed.rowCount} rows in {executed.executionMs} ms
            </span>
            <button className="secondary" onClick={() => downloadCsv(executed.rows)}>
              Export CSV
            </button>
          </div>
          {executed.rows.length === 0 ? (
            <p>No rows returned.</p>
          ) : (
            <>
              <table>
                <thead>
                  <tr>
                    {Object.keys(executed.rows[0]).map((key) => (
                      <th key={key}>{key}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {pagedRows.map((row, idx) => (
                    <tr key={idx}>
                      {Object.keys(executed.rows[0]).map((key) => (
                        <td key={key}>{String(row[key] ?? "")}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="row">
                <button className="secondary" disabled={page <= 0} onClick={() => setPage((p) => p - 1)}>
                  Prev
                </button>
                <span>
                  Page {page + 1} / {Math.max(1, Math.ceil(executed.rows.length / PAGE_SIZE))}
                </span>
                <button
                  className="secondary"
                  disabled={(page + 1) * PAGE_SIZE >= executed.rows.length}
                  onClick={() => setPage((p) => p + 1)}
                >
                  Next
                </button>
              </div>
            </>
          )}
        </div>
      ) : null}

      <div className="card stack">
        <strong>Thread History</strong>
        {messages.length === 0 ? <p style={{ color: "var(--ink-soft)" }}>No messages yet.</p> : null}
        {messages.map((m, idx) => (
          <div key={`${m.role}-${idx}`}>
            <strong>{m.role === "user" ? "User" : "Assistant"}:</strong> {m.text}
          </div>
        ))}
      </div>
    </div>
  );
}
