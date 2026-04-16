import { getPool } from "@/lib/db/mysql";
import type { SchemaContext } from "@/lib/types/contracts";

let cache: SchemaContext | null = null;
let inflight: Promise<SchemaContext> | null = null;

async function introspectSchema(): Promise<SchemaContext> {
  const database = process.env.MYSQL_DATABASE ?? "";
  const sql = `
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = ?
    ORDER BY TABLE_NAME, ORDINAL_POSITION
  `;

  const [rows] = await getPool().query(sql, [database]);
  const grouped = new Map<string, { name: string; type: string }[]>();

  for (const row of rows as Record<string, string>[]) {
    const table = row.TABLE_NAME;
    const current = grouped.get(table) ?? [];
    current.push({
      name: row.COLUMN_NAME,
      type: row.DATA_TYPE
    });
    grouped.set(table, current);
  }

  return {
    database,
    refreshedAt: new Date().toISOString(),
    tables: Array.from(grouped.entries()).map(([table, columns]) => ({
      table,
      columns
    }))
  };
}

export async function getSchemaContext(forceRefresh = false): Promise<SchemaContext> {
  if (!forceRefresh && cache) {
    return cache;
  }
  if (!inflight) {
    inflight = introspectSchema().finally(() => {
      inflight = null;
    });
  }
  cache = await inflight;
  return cache;
}
