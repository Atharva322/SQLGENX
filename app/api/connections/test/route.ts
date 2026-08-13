import { NextResponse } from "next/server";
import { z } from "zod";
import { requireSession } from "@/lib/auth/session";
import { testConnection } from "@/lib/genxsql-api";
import type { RuntimeConnectionConfig } from "@/lib/types/contracts";

const requestSchema = z.object({
  adapterKey: z.enum(["postgresql", "mysql"]),
  config: z.object({
    host: z.string().min(1),
    port: z.coerce.number().int().min(1).max(65535),
    database: z.string().min(1),
    username: z.string().min(1),
    password: z.string().min(1),
    tlsMode: z.enum(["disable", "prefer", "require", "verify-ca", "verify-full"])
  })
});

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    const body = requestSchema.parse(await req.json());
    return NextResponse.json(await testConnection(user.userId, body.adapterKey, body.config as RuntimeConnectionConfig));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Connection test failed.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
