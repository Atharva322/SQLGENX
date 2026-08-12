import { POST as generatePost } from "@/app/api/chat/generate-sql/route";
import { POST as executePost } from "@/app/api/chat/execute-sql/route";
import { runGenxsqlQuery } from "@/lib/genxsql-api";

vi.mock("@/lib/auth/session", () => ({
  requireSession: vi.fn(async () => ({ userId: "u1", role: "analyst", email: "a@b.com" })),
  assertCanExecute: vi.fn()
}));

vi.mock("@/lib/genxsql-api", () => ({
  runGenxsqlQuery: vi.fn(async (_ownerId: string, question: string, connectionId: string, conversationId: string) => ({
    queryId: "qry_fastapi",
    connectionId,
    conversationId,
    generatedSql: "SELECT id, total_amount FROM orders",
    explanation: `Generated for ${question}.`,
    confidence: 0.87,
    referenced: { tables: ["orders"], columns: ["orders.id", "orders.total_amount"] },
    safety: { status: "safe", reasons: [] },
    preExecuted: {
      queryId: "qry_fastapi",
      status: "success",
      rows: [{ id: 1, total_amount: 99.5 }],
      rowCount: 1,
      executionMs: 12
    }
  }))
}));

describe("api generate/execute flow", () => {
  it("uses FastAPI as the single selected-connection authority", async () => {
    const genReq = new Request("http://localhost/api/chat/generate-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        question: "show orders",
        connectionId: "runtime_pg"
      })
    });

    const genRes = await generatePost(genReq);
    expect(genRes.status).toBe(200);
    const genBody = await genRes.json();
    expect(genBody.connectionId).toBe("runtime_pg");
    expect(genBody.generatedSql).toMatch(/select/i);
    expect(runGenxsqlQuery).toHaveBeenCalledWith("u1", "show orders", "runtime_pg", expect.any(String));

    const exeReq = new Request("http://localhost/api/chat/execute-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        queryId: genBody.queryId,
        connectionId: "runtime_pg",
        sql: genBody.generatedSql
      })
    });

    const exeRes = await executePost(exeReq);
    expect(exeRes.status).toBe(200);
    const exeBody = await exeRes.json();
    expect(exeBody.status).toBe("success");
    expect(exeBody.rowCount).toBe(1);
    expect(runGenxsqlQuery).toHaveBeenLastCalledWith("u1", "show orders", "runtime_pg", genBody.conversationId);
  });

  it("rejects execution after switching connections", async () => {
    const genReq = new Request("http://localhost/api/chat/generate-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        question: "show orders",
        connectionId: "runtime_pg_a"
      })
    });
    const genBody = await (await generatePost(genReq)).json();

    const exeReq = new Request("http://localhost/api/chat/execute-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        queryId: genBody.queryId,
        connectionId: "runtime_pg_b",
        sql: genBody.generatedSql
      })
    });

    const exeRes = await executePost(exeReq);
    expect(exeRes.status).toBe(409);
  });
});
