import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { getSchemaContext } from "@/lib/schema/cache";

export async function POST(): Promise<Response> {
  try {
    await requireSession();
    const schema = await getSchemaContext(true);
    return NextResponse.json(schema);
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to refresh schema context.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
