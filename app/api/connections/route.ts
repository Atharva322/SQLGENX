import { NextResponse } from "next/server";
import { z } from "zod";
import { requireSession } from "@/lib/auth/session";
import { createConnection, fetchConnections } from "@/lib/genxsql-api";
import type { PostgresConnectionConfig } from "@/lib/types/contracts";

const configSchema = z.object({
  host: z.string().min(1),
  port: z.coerce.number().int().min(1).max(65535),
  database: z.string().min(1),
  username: z.string().min(1),
  password: z.string().min(1),
  tlsMode: z.enum(["disable", "prefer", "require", "verify-ca", "verify-full"])
});

const createSchema = z.object({
  id: z.string().min(3),
  displayName: z.string().min(1),
  config: configSchema
});

export async function GET(): Promise<Response> {
  try {
    const user = await requireSession();
    return NextResponse.json({ connections: await fetchConnections(user.userId) });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to load connections.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    const body = createSchema.parse(await req.json());
    const connection = await createConnection(
      user.userId,
      body.id,
      body.displayName,
      body.config as PostgresConnectionConfig
    );
    return NextResponse.json(connection);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to save connection.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
