import type { HistoryEntry } from "@/lib/types/contracts";

const byQueryId = new Map<string, HistoryEntry>();
const byConversationId = new Map<string, HistoryEntry[]>();

export function addHistory(entry: HistoryEntry): void {
  byQueryId.set(entry.queryId, entry);
  const list = byConversationId.get(entry.conversationId) ?? [];
  list.push(entry);
  byConversationId.set(entry.conversationId, list);
}

export function updateHistory(queryId: string, patch: Partial<HistoryEntry>): HistoryEntry | undefined {
  const existing = byQueryId.get(queryId);
  if (!existing) {
    return undefined;
  }
  const updated = { ...existing, ...patch, updatedAt: new Date().toISOString() };
  byQueryId.set(queryId, updated);
  const list = byConversationId.get(existing.conversationId);
  if (list) {
    const idx = list.findIndex((item) => item.queryId === queryId);
    if (idx >= 0) {
      list[idx] = updated;
    }
  }
  return updated;
}

export function getHistoryByConversation(conversationId: string): HistoryEntry[] {
  const list = byConversationId.get(conversationId) ?? [];
  return [...list].sort((a, b) => a.createdAt.localeCompare(b.createdAt));
}

export function getHistoryByQueryId(queryId: string): HistoryEntry | undefined {
  return byQueryId.get(queryId);
}
