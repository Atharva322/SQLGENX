import { checkSqlSafety } from "@/lib/sql/safety";

describe("checkSqlSafety", () => {
  it("allows simple select", () => {
    const result = checkSqlSafety("SELECT id, name FROM users");
    expect(result.status).toBe("safe");
    expect(result.reasons).toHaveLength(0);
  });

  it("blocks write keywords", () => {
    const result = checkSqlSafety("DELETE FROM users WHERE id = 1");
    expect(result.status).toBe("blocked");
    expect(result.reasons.join(" ")).toMatch(/blocked/i);
  });

  it("blocks multi statement", () => {
    const result = checkSqlSafety("SELECT * FROM users; SELECT * FROM accounts");
    expect(result.status).toBe("blocked");
  });

  it("blocks comments", () => {
    const result = checkSqlSafety("SELECT * FROM users -- hide");
    expect(result.status).toBe("blocked");
  });
});
