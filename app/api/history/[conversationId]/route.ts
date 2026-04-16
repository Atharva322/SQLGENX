import { NextResponse } from "next/server";
import { requireSession } from "@/lib/auth/session";
import { getHistoryByConversation } from "@/lib/store/history-store";

interface RouteParams {
  params: Promise<{ conversationId: string }>;
}

export async function GET(_: Request, { params }: RouteParams): Promise<Response> {
  try {
    await requireSession();
    const { conversationId } = await params;
    const items = getHistoryByConversation(conversationId);
    return NextResponse.json({ conversationId, items });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to fetch history.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
