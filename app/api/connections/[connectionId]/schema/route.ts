import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { fetchConnectionSchema } from "@/lib/genxsql-api";

export async function GET(_req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    return NextResponse.json(await fetchConnectionSchema(user.userId, params.connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to load schema.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
