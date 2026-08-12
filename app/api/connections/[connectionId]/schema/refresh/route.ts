import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { refreshConnectionSchema } from "@/lib/genxsql-api";

export async function POST(_req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    return NextResponse.json(await refreshConnectionSchema(user.userId, params.connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to refresh schema.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
