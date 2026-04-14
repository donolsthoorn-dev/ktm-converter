import { NextRequest, NextResponse } from "next/server";
import { createServiceRoleClient } from "@/lib/supabase/server";
import { jobApiAuthorized, jobApiUnauthorizedResponse } from "@/lib/jobs-auth";

type Body = {
  job_type?: string;
  payload?: Record<string, unknown>;
  trigger_source?: "manual" | "schedule" | "api";
};

export async function POST(request: NextRequest) {
  if (!process.env.JOB_TRIGGER_SECRET) {
    return NextResponse.json(
      { error: "JOB_TRIGGER_SECRET ontbreekt op de server." },
      { status: 500 },
    );
  }
  if (!jobApiAuthorized(request)) return jobApiUnauthorizedResponse();

  let body: Body;
  try {
    body = (await request.json()) as Body;
  } catch {
    return NextResponse.json({ error: "Ongeldige JSON" }, { status: 400 });
  }

  const jobType = (body.job_type ?? "").trim();
  if (!jobType) {
    return NextResponse.json({ error: "job_type is verplicht" }, { status: 400 });
  }

  const triggerSource = body.trigger_source ?? "api";
  const payload = body.payload && typeof body.payload === "object" ? body.payload : {};

  try {
    const supabase = createServiceRoleClient();
    const { data, error } = await supabase
      .from("jobs")
      .insert({
        job_type: jobType,
        status: "queued",
        trigger_source: triggerSource,
        payload,
      })
      .select("id, created_at, job_type, status, trigger_source")
      .single();

    if (error) {
      return NextResponse.json({ error: error.message, hint: error.hint }, { status: 503 });
    }
    return NextResponse.json({ job: data });
  } catch (e) {
    const message = e instanceof Error ? e.message : "Onbekende fout";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
