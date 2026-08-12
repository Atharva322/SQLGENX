import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { refreshConnectionSchema } from "@/lib/genxsql-api";

export async function POST(req: Request): Promise<Response> {
  try {
    const user = await requireSession();
    const connectionId = new URL(req.url).searchParams.get("connectionId") ?? "default";
    return NextResponse.json(await refreshConnectionSchema(user.userId, connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to refresh schema context.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
