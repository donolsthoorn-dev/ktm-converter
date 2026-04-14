import type { NextRequest } from "next/server";

/** Gedeeld geheim voor trigger + lijst (MVP; later vervangen door Supabase Auth). */
export function jobApiAuthorized(request: NextRequest): boolean {
  const secret = process.env.JOB_TRIGGER_SECRET;
  if (!secret) return false;
  const auth = request.headers.get("authorization");
  return auth === `Bearer ${secret}`;
}

export function jobApiUnauthorizedResponse() {
  return new Response(JSON.stringify({ error: "Unauthorized" }), {
    status: 401,
    headers: { "Content-Type": "application/json" },
  });
}
