import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { getSchemaContext } from "@/lib/schema/cache";

export async function GET(): Promise<Response> {
  try {
    await requireSession();
    const schema = await getSchemaContext(false);
    return NextResponse.json(schema);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to fetch schema context.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
