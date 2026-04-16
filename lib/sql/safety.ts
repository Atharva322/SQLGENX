import type { SafetyStatus } from "@/lib/types/contracts";

const blockedKeywordPattern =
  /\b(insert|update|delete|drop|alter|truncate|create|replace|grant|revoke|call|execute|merge|lock|unlock)\b/i;

const blockedFunctionPattern = /\b(load_file|outfile|dumpfile|benchmark|sleep)\s*\(/i;

const sqlCommentPattern = /(--|\/\*|\*\/|#)/;

export interface SafetyCheckResult {
  status: SafetyStatus;
  reasons: string[];
}

export function checkSqlSafety(sql: string): SafetyCheckResult {
  const reasons: string[] = [];
  const trimmed = sql.trim();
  if (!trimmed) {
    return { status: "blocked", reasons: ["SQL is empty."] };
  }

  if (sqlCommentPattern.test(trimmed)) {
    reasons.push("SQL comments are blocked to prevent hidden statements.");
  }

  if (trimmed.includes(";")) {
    const semicolons = trimmed.split(";").filter((part) => part.trim().length > 0);
    if (semicolons.length > 1 || trimmed.endsWith(";")) {
      reasons.push("Multi-statement SQL is not allowed.");
    }
  }

  if (!/^(with\b|select\b)/i.test(trimmed)) {
    reasons.push("Only SELECT or CTE (WITH ... SELECT) statements are allowed.");
  }

  if (blockedKeywordPattern.test(trimmed)) {
    reasons.push("Detected write or DDL keywords that are blocked.");
  }

  if (blockedFunctionPattern.test(trimmed)) {
    reasons.push("Detected unsafe function usage.");
  }

  if (reasons.length > 0) {
    return { status: "blocked", reasons };
  }
  return { status: "safe", reasons: [] };
}
