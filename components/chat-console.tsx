"use client";

import React, { useMemo, useState } from "react";
import type { ExecuteSqlResponse, GenerateSqlResponse } from "@/lib/types/contracts";

interface Message {
  role: "user" | "assistant";
  text: string;
}

const PAGE_SIZE = 10;

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
          connectionId: "default",
          conversationId
        })
      });
      const data = await response.json();
      if (!response.ok) {
        throw new Error(data.error ?? "Generation failed");
      }
      const payload = data as GenerateSqlResponse;
      setGenerated(payload);
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
          Ask in plain English. Review generated MySQL SQL. Run only after approval.
        </p>
        <div className="stack">
          <textarea
            rows={4}
            placeholder="Example: Show top 10 customers by total order value in the last 90 days."
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
          />
          <div className="row">
            <button disabled={loading || question.trim().length < 3} onClick={onGenerate}>
              {loading ? "Generating..." : "Generate SQL"}
            </button>
            {conversationId ? <span style={{ color: "var(--ink-soft)" }}>Conversation: {conversationId}</span> : null}
          </div>
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
