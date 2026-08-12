import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { fetchConnectionSchema } from "@/lib/genxsql-api";

export async function GET(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    const connectionId = new URL(req.url).searchParams.get("connectionId") ?? "default";
    return NextResponse.json(await fetchConnectionSchema(user.userId, connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to fetch schema context.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
