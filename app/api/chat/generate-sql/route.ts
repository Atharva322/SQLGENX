import { NextResponse } from "next/server";
import { z } from "zod";
import { requireSession } from "@/lib/auth/session";
import { generateSql } from "@/lib/llm/sql-generator";
import { getSchemaContext } from "@/lib/schema/cache";
import { extractReferences } from "@/lib/sql/references";
import { checkSqlSafety } from "@/lib/sql/safety";
import { addHistory } from "@/lib/store/history-store";
import type { GenerateSqlRequest, GenerateSqlResponse } from "@/lib/types/contracts";
import { createId } from "@/lib/utils/id";

const requestSchema = z.object({
  question: z.string().min(3),
  connectionId: z.string().min(1),
  conversationId: z.string().optional()
}) satisfies z.ZodType<GenerateSqlRequest>;

export async function POST(req: Request): Promise<Response> {
  try {
    const body = requestSchema.parse(await req.json());
    const genxsqlApiBaseUrl = process.env.GENXSQL_API_BASE_URL;
    if (genxsqlApiBaseUrl) {
      return generateViaGenxsqlApi(body, genxsqlApiBaseUrl);
    }

    const user = await requireSession();
    const conversationId = body.conversationId ?? createId("conv");
    const queryId = createId("qry");

    const schema = await getSchemaContext(false);
    const llm = await generateSql(body.question, schema);
    const safety = checkSqlSafety(llm.sql);
    const referenced = extractReferences(llm.sql, schema);

    const now = new Date().toISOString();
    addHistory({
      queryId,
      conversationId,
      userId: user.userId,
      question: body.question,
      generatedSql: llm.sql,
      explanation: llm.explanation,
      confidence: llm.confidence,
      safetyStatus: safety.status,
      safetyReasons: safety.reasons,
      approved: false,
      executed: false,
      createdAt: now,
      updatedAt: now
    });

    const response: GenerateSqlResponse = {
      queryId,
      conversationId,
      generatedSql: llm.sql,
      explanation: llm.explanation,
      confidence: llm.confidence,
      referenced,
      safety
    };

    return NextResponse.json(response);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to generate SQL.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}

async function generateViaGenxsqlApi(body: GenerateSqlRequest, apiBaseUrl: string): Promise<Response> {
  const conversationId = body.conversationId ?? createId("conv");
  const upstream = await fetch(new URL("/v1/query", apiBaseUrl), {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      question: body.question,
      connection_id: body.connectionId,
      session_id: conversationId
    })
  });
  const data = await upstream.json();
  if (!upstream.ok) {
    return NextResponse.json({ error: data.detail ?? data.error ?? "GENXSQL API query failed." }, { status: upstream.status });
  }

  const tables = Array.isArray(data.accessed?.tables) ? data.accessed.tables : [];
  const columns = Array.isArray(data.accessed?.columns) ? data.accessed.columns : [];
  const queryId = typeof data.query_id === "string" ? data.query_id : createId("qry");
  const warnings = Array.isArray(data.warnings) ? data.warnings.map(String) : [];
  const blocked = data.sql === "UNANSWERABLE";

  const response: GenerateSqlResponse = {
    queryId,
    conversationId,
    generatedSql: data.sql,
    explanation: data.explanation ?? "Generated through the GENXSQL QueryService API.",
    confidence: typeof data.confidence === "number" ? data.confidence : 0,
    referenced: {
      tables: tables.map(String),
      columns: columns.map(String)
    },
    safety: {
      status: blocked ? "blocked" : warnings.length > 0 ? "warning" : "safe",
      reasons: warnings
    },
    preExecuted: {
      queryId,
      status: blocked ? "error" : "success",
      rows: Array.isArray(data.results) ? data.results : [],
      rowCount: Array.isArray(data.results) ? data.results.length : 0,
      executionMs: Number(data.execution_meta?.execution_time_ms ?? 0),
      error: blocked ? "GENXSQL returned UNANSWERABLE." : undefined
    }
  };

  return NextResponse.json(response);
}
