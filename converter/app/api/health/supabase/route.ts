import { NextResponse } from "next/server";
import { createServiceRoleClient } from "@/lib/supabase/server";

/**
 * Controleert of Supabase bereikbaar is. Vereist service role op de server
 * (Vercel: zet SUPABASE_SERVICE_ROLE_KEY + NEXT_PUBLIC_SUPABASE_URL).
 */
export async function GET() {
  try {
    const supabase = createServiceRoleClient();
    const { error } = await supabase.from("sync_runs").select("id").limit(1);
    if (error) {
      return NextResponse.json(
        { ok: false, error: error.message, hint: error.hint },
        { status: 503 },
      );
    }
    return NextResponse.json({ ok: true, table: "sync_runs" });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Onbekende fout";
    return NextResponse.json({ ok: false, error: message }, { status: 500 });
  }
}
