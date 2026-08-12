import { NextResponse } from "next/server";
import { z } from "zod";
import { assertCanExecute, requireSession } from "@/lib/auth/session";
import { runGenxsqlQuery } from "@/lib/genxsql-api";
import { getHistoryByQueryId, updateHistory } from "@/lib/store/history-store";
import type { ExecuteSqlRequest, ExecuteSqlResponse } from "@/lib/types/contracts";

const requestSchema = z.object({
  queryId: z.string().min(1),
  connectionId: z.string().min(1),
  sql: z.string().min(1)
}) satisfies z.ZodType<ExecuteSqlRequest>;

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    assertCanExecute(user);
    const body = requestSchema.parse(await req.json());
    const history = getHistoryByQueryId(body.queryId);
    if (!history || history.userId !== user.userId) {
      return NextResponse.json({ error: "Query history entry not found." }, { status: 404 });
    }
    if (history.connectionId !== body.connectionId) {
      return NextResponse.json({ error: "Connection changed. Regenerate SQL before running." }, { status: 409 });
    }
    if (history.generatedSql !== body.sql) {
      return NextResponse.json({ error: "Edited SQL must be regenerated through GENXSQL before execution." }, { status: 409 });
    }

    updateHistory(body.queryId, { approved: true });
    const rerun = await runGenxsqlQuery(user.userId, history.question, history.connectionId, history.conversationId);
    const execution = rerun.preExecuted;
    if (!execution || execution.status !== "success") {
      updateHistory(body.queryId, {
        executed: true,
        executionStatus: "error",
        error: execution?.error ?? "GENXSQL did not execute the query."
      });
      return NextResponse.json(
        {
          queryId: body.queryId,
          status: "error",
          rows: [],
          rowCount: 0,
          executionMs: execution?.executionMs ?? 0,
          error: execution?.error ?? "GENXSQL did not execute the query."
        } satisfies ExecuteSqlResponse,
        { status: 400 }
      );
    }

    const response: ExecuteSqlResponse = {
      ...execution,
      queryId: body.queryId
    };
    updateHistory(body.queryId, {
      executed: true,
      executionStatus: "success",
      executionMs: response.executionMs
    });
    return NextResponse.json(response);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Execution failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
