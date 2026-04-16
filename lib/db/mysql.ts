import mysql, { type Pool } from "mysql2/promise";

let pool: Pool | null = null;

export function getPool(): Pool {
  if (pool) {
    return pool;
  }
  pool = mysql.createPool({
    host: process.env.MYSQL_HOST,
    port: Number(process.env.MYSQL_PORT ?? 3306),
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASSWORD,
    database: process.env.MYSQL_DATABASE,
    waitForConnections: true,
    connectionLimit: 8,
    queueLimit: 0
  });
  return pool;
}

export async function runQuery(sql: string): Promise<{ rows: Record<string, unknown>[]; ms: number }> {
  const start = performance.now();
  const [rows] = await getPool().query(sql);
  const end = performance.now();
  return { rows: rows as Record<string, unknown>[], ms: end - start };
}
