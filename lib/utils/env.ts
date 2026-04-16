const requiredEnv = ["OPENAI_API_KEY", "MYSQL_HOST", "MYSQL_USER", "MYSQL_PASSWORD", "MYSQL_DATABASE"] as const;

export function validateEnv(): { ok: boolean; missing: string[] } {
  const missing = requiredEnv.filter((k) => !process.env[k]);
  return { ok: missing.length === 0, missing };
}
