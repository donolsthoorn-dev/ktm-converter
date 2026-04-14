import { NextRequest, NextResponse } from "next/server";
import { createServiceRoleClient } from "@/lib/supabase/server";
import { jobApiAuthorized, jobApiUnauthorizedResponse } from "@/lib/jobs-auth";

export async function GET(request: NextRequest) {
  if (!process.env.JOB_TRIGGER_SECRET) {
    return NextResponse.json(
      { error: "JOB_TRIGGER_SECRET ontbreekt op de server." },
      { status: 500 },
    );
  }
  if (!jobApiAuthorized(request)) return jobApiUnauthorizedResponse();

  try {
    const supabase = createServiceRoleClient();
    const { data, error } = await supabase
      .from("jobs")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(50);

    if (error) {
      return NextResponse.json({ error: error.message, hint: error.hint }, { status: 503 });
    }
    return NextResponse.json({ jobs: data ?? [] });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Onbekende fout";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
