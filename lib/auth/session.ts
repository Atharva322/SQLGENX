import { headers } from "next/headers";
import { verifyBearerToken } from "@/lib/auth/cognito";

export interface SessionUser {
  userId: string;
  email?: string;
  role: "analyst" | "admin" | "viewer";
}

export async function requireSession(): Promise<SessionUser> {
  const h = await headers();
  const authHeader = h.get("authorization");
  const allowDevBypass = process.env.AUTH_DEV_BYPASS === "true";

  if (!authHeader) {
    if (!allowDevBypass) {
      throw new Error("Missing authorization token.");
    }
    return {
      userId: h.get("x-user-id") ?? "demo-user",
      email: h.get("x-user-email") ?? "demo@example.com",
      role: (h.get("x-user-role") ?? "analyst") as SessionUser["role"]
    };
  }

  const token = authHeader.replace(/^Bearer\s+/i, "").trim();
  const payload = await verifyBearerToken(token);
  const groups = payload["cognito:groups"];
  const role = resolveRole(groups);
  return {
    userId: String(payload.sub ?? ""),
    email: typeof payload.email === "string" ? payload.email : undefined,
    role
  };
}

export function assertCanExecute(user: SessionUser): void {
  const allowed = new Set<SessionUser["role"]>(["analyst", "admin"]);
  if (!allowed.has(user.role)) {
    throw new Error("User role is not allowed to execute SQL.");
  }
}

function resolveRole(groups: unknown): SessionUser["role"] {
  if (Array.isArray(groups)) {
    if (groups.includes("genxsql-admin")) {
      return "admin";
    }
    if (groups.includes("genxsql-viewer")) {
      return "viewer";
    }
  }
  return "analyst";
}
