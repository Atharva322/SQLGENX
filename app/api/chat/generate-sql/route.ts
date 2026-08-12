import { NextResponse } from "next/server";
import { z } from "zod";
import { requireSession } from "@/lib/auth/session";
import { runGenxsqlQuery } from "@/lib/genxsql-api";
import { addHistory } from "@/lib/store/history-store";
import type { GenerateSqlRequest } from "@/lib/types/contracts";
import { createId } from "@/lib/utils/id";

const requestSchema = z.object({
  question: z.string().min(3),
  connectionId: z.string().min(1),
  conversationId: z.string().optional()
}) satisfies z.ZodType<GenerateSqlRequest>;

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    const body = requestSchema.parse(await req.json());
    const conversationId = body.conversationId ?? createId("conv");
    const result = await runGenxsqlQuery(user.userId, body.question, body.connectionId, conversationId);
    const now = new Date().toISOString();

    addHistory({
      queryId: result.queryId,
      connectionId: body.connectionId,
      conversationId,
      userId: user.userId,
      question: body.question,
      generatedSql: result.generatedSql,
      explanation: result.explanation,
      confidence: result.confidence,
      safetyStatus: result.safety.status,
      safetyReasons: result.safety.reasons,
      approved: false,
      executed: Boolean(result.preExecuted && result.preExecuted.status === "success"),
      executionStatus: result.preExecuted?.status,
      executionMs: result.preExecuted?.executionMs,
      createdAt: now,
      updatedAt: now
    });

    return NextResponse.json(result);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to generate SQL.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
