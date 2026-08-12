import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { fetchAdapters } from "@/lib/genxsql-api";

export async function GET(): Promise<Response> {
  try {
    const user = await requireSession();
    return NextResponse.json({ adapters: await fetchAdapters(user.userId) });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to load adapters.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
