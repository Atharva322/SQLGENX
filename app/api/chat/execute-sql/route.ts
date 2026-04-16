import { NextResponse } from "next/server";
import { z } from "zod";
import { assertCanExecute, requireSession } from "@/lib/auth/session";
import { runQuery } from "@/lib/db/mysql";
import { repairSql } from "@/lib/llm/sql-generator";
import { getSchemaContext } from "@/lib/schema/cache";
import { checkSqlSafety } from "@/lib/sql/safety";
import { getHistoryByQueryId, updateHistory } from "@/lib/store/history-store";
import type { ExecuteSqlRequest, ExecuteSqlResponse } from "@/lib/types/contracts";

const requestSchema = z.object({
  queryId: z.string().min(1),
  sql: z.string().min(1)
}) satisfies z.ZodType<ExecuteSqlRequest>;

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    assertCanExecute(user);
    const body = requestSchema.parse(await req.json());
    const history = getHistoryByQueryId(body.queryId);
    if (!history) {
      return NextResponse.json({ error: "Query history entry not found." }, { status: 404 });
    }

    const safety = checkSqlSafety(body.sql);
    if (safety.status !== "safe") {
      updateHistory(body.queryId, {
        approved: true,
        executed: false,
        safetyStatus: "blocked",
        safetyReasons: safety.reasons
      });
      return NextResponse.json({ error: "SQL blocked by safety policy.", reasons: safety.reasons }, { status: 400 });
    }

    updateHistory(body.queryId, { approved: true });

    try {
      const run = await runQuery(body.sql);
      const response: ExecuteSqlResponse = {
        queryId: body.queryId,
        status: "success",
        rows: run.rows,
        rowCount: run.rows.length,
        executionMs: Math.round(run.ms)
      };
      updateHistory(body.queryId, {
        executed: true,
        executionStatus: "success",
        executionMs: response.executionMs
      });
      return NextResponse.json(response);
    } catch (error) {
      const originalError = error instanceof Error ? error.message : "Execution failed";
      const schema = await getSchemaContext(false);
      const repairedSql = await repairSql(body.sql, originalError, history.question, schema);
      const repairedSafety = checkSqlSafety(repairedSql);

      if (repairedSafety.status !== "safe") {
        updateHistory(body.queryId, {
          executed: true,
          executionStatus: "error",
          error: originalError
        });
        return NextResponse.json(
          {
            queryId: body.queryId,
            status: "error",
            rows: [],
            rowCount: 0,
            executionMs: 0,
            error: originalError
          } satisfies ExecuteSqlResponse,
          { status: 400 }
        );
      }

      const retry = await runQuery(repairedSql);
      updateHistory(body.queryId, {
        generatedSql: repairedSql,
        executed: true,
        executionStatus: "success",
        executionMs: Math.round(retry.ms)
      });

      return NextResponse.json({
        queryId: body.queryId,
        status: "success",
        rows: retry.rows,
        rowCount: retry.rows.length,
        executionMs: Math.round(retry.ms),
        repaired: true,
        sql: repairedSql
      });
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : "Execution failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
