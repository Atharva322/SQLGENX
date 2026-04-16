import { POST as generatePost } from "@/app/api/chat/generate-sql/route";
import { POST as executePost } from "@/app/api/chat/execute-sql/route";

vi.mock("@/lib/auth/session", () => ({
  requireSession: vi.fn(async () => ({ userId: "u1", role: "analyst", email: "a@b.com" })),
  assertCanExecute: vi.fn()
}));

vi.mock("@/lib/schema/cache", () => ({
  getSchemaContext: vi.fn(async () => ({
    database: "sales",
    refreshedAt: new Date().toISOString(),
    tables: [
      {
        table: "orders",
        columns: [
          { name: "id", type: "int" },
          { name: "total_amount", type: "decimal" }
        ]
      }
    ]
  }))
}));

vi.mock("@/lib/llm/sql-generator", () => ({
  generateSql: vi.fn(async () => ({
    sql: "SELECT id, total_amount FROM orders",
    explanation: "Lists order totals.",
    confidence: 0.87
  })),
  repairSql: vi.fn(async () => "SELECT id, total_amount FROM orders")
}));

vi.mock("@/lib/db/mysql", () => ({
  runQuery: vi.fn(async () => ({
    rows: [{ id: 1, total_amount: 99.5 }],
    ms: 12
  }))
}));

describe("api generate/execute flow", () => {
  it("generates SQL and executes approved query", async () => {
    const genReq = new Request("http://localhost/api/chat/generate-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        question: "show orders",
        connectionId: "default"
      })
    });

    const genRes = await generatePost(genReq);
    expect(genRes.status).toBe(200);
    const genBody = await genRes.json();
    expect(genBody.generatedSql).toMatch(/select/i);
    expect(genBody.queryId).toBeTruthy();

    const exeReq = new Request("http://localhost/api/chat/execute-sql", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        queryId: genBody.queryId,
        sql: genBody.generatedSql
      })
    });

    const exeRes = await executePost(exeReq);
    expect(exeRes.status).toBe(200);
    const exeBody = await exeRes.json();
    expect(exeBody.status).toBe("success");
    expect(exeBody.rowCount).toBe(1);
  });
});
