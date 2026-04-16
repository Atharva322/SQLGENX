import OpenAI from "openai";
import type { SchemaContext } from "@/lib/types/contracts";

export interface SqlGenerationResult {
  sql: string;
  explanation: string;
  confidence: number;
}

function getClient(): OpenAI {
  return new OpenAI({ apiKey: process.env.OPENAI_API_KEY });
}

function buildPrompt(question: string, schema: SchemaContext): string {
  const schemaText = schema.tables
    .map((table) => {
      const cols = table.columns.map((col) => `${col.name} (${col.type})`).join(", ");
      return `- ${table.table}: ${cols}`;
    })
    .join("\n");

  return [
    "You are an enterprise SQL assistant.",
    "Generate MySQL read-only SQL from user requests.",
    "Rules:",
    "1) Output one SELECT query only.",
    "2) Never use DDL or DML.",
    "3) Use only listed tables/columns.",
    "4) If impossible, return a best-effort SELECT explaining limits.",
    "5) Never include comments.",
    "",
    "Schema:",
    schemaText,
    "",
    `Question: ${question}`,
    "",
    "Return JSON with keys: sql, explanation, confidence (0..1)."
  ].join("\n");
}

export async function generateSql(question: string, schema: SchemaContext): Promise<SqlGenerationResult> {
  const response = await getClient().responses.create({
    model: process.env.OPENAI_SQL_MODEL ?? "gpt-4.1-mini",
    temperature: 0,
    input: buildPrompt(question, schema),
    text: {
      format: {
        type: "json_schema",
        name: "sql_result",
        schema: {
          type: "object",
          additionalProperties: false,
          required: ["sql", "explanation", "confidence"],
          properties: {
            sql: { type: "string" },
            explanation: { type: "string" },
            confidence: { type: "number", minimum: 0, maximum: 1 }
          }
        }
      }
    }
  });

  const text = response.output_text || "{}";
  const parsed = JSON.parse(text) as SqlGenerationResult;
  return {
    sql: parsed.sql?.trim() ?? "",
    explanation: parsed.explanation ?? "Generated SQL based on available schema context.",
    confidence: typeof parsed.confidence === "number" ? parsed.confidence : 0.5
  };
}

export async function repairSql(originalSql: string, dbError: string, question: string, schema: SchemaContext): Promise<string> {
  const response = await getClient().responses.create({
    model: process.env.OPENAI_SQL_MODEL ?? "gpt-4.1-mini",
    temperature: 0,
    input: [
      "Fix the SQL query while staying read-only for MySQL.",
      `Question: ${question}`,
      `Original SQL: ${originalSql}`,
      `Database error: ${dbError}`,
      `Schema tables: ${schema.tables.map((t) => t.table).join(", ")}`,
      "Return only SQL text."
    ].join("\n")
  });

  return (response.output_text || "").trim();
}
