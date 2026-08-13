import { NextResponse } from "next/server";
import { z } from "zod";
import { requireSession } from "@/lib/auth/session";
import { deleteConnection, updateConnection } from "@/lib/genxsql-api";
import type { RuntimeConnectionConfig } from "@/lib/types/contracts";

const updateSchema = z.object({
  displayName: z.string().min(1),
  config: z
    .object({
      host: z.string().min(1),
      port: z.coerce.number().int().min(1).max(65535),
      database: z.string().min(1),
      username: z.string().min(1),
      password: z.string().min(1),
      tlsMode: z.enum(["disable", "prefer", "require", "verify-ca", "verify-full"])
    })
    .optional()
});

export async function PATCH(req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    const body = updateSchema.parse(await req.json());
    return NextResponse.json(
      await updateConnection(
        user.userId,
        params.connectionId,
        body.displayName,
        body.config as RuntimeConnectionConfig | undefined
      )
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to update connection.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}

export async function DELETE(_req: Request, context: { params: Promise<{ connectionId: string }> }): Promise<Response> {
  try {
    const user = await requireSession();
    const params = await context.params;
    return NextResponse.json(await deleteConnection(user.userId, params.connectionId));
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to delete connection.";
    return NextResponse.json({ error: message }, { status: 400 });
  }
}
