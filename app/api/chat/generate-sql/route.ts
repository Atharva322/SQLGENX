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
    const user = await requireSession();
    const body = requestSchema.parse(await req.json());
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
