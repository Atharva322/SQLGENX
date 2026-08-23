import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { fetchConnectionPrepareStatus, prepareConnection } from "@/lib/genxsql-api";

export async function GET(_req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    return NextResponse.json(await fetchConnectionPrepareStatus(user.userId, params.connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to load schema preparation status.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}

export async function POST(_req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    return NextResponse.json(await prepareConnection(user.userId, params.connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to prepare schema.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
