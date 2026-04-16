import type { ReferencedSchema, SchemaContext } from "@/lib/types/contracts";

export function extractReferences(sql: string, schema: SchemaContext): ReferencedSchema {
  const lowered = sql.toLowerCase();
  const tableMatches = new Set<string>();
  const columnMatches = new Set<string>();

  for (const table of schema.tables) {
    if (new RegExp(`\\b${escapeRegExp(table.table.toLowerCase())}\\b`).test(lowered)) {
      tableMatches.add(table.table);
    }
    for (const column of table.columns) {
      if (new RegExp(`\\b${escapeRegExp(column.name.toLowerCase())}\\b`).test(lowered)) {
        columnMatches.add(`${table.table}.${column.name}`);
      }
    }
  }

  return {
    tables: [...tableMatches],
    columns: [...columnMatches]
  };
}

function escapeRegExp(value: string): string {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
