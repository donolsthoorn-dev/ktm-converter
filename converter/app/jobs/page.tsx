"use client";

import type { CSSProperties } from "react";
import { useCallback, useState } from "react";
import Link from "next/link";

type JobRow = {
  id: string;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  job_type: string;
  status: string;
  trigger_source: string;
  payload: Record<string, unknown>;
  log_summary: string | null;
  error_message: string | null;
};

const JOB_TYPES = [
  { value: "worker_stub", label: "Worker stub (test)" },
  { value: "ingest_input", label: "Nieuwe input verwerken (later)" },
  { value: "shopify_mirror", label: "Shopify → Supabase spiegel (later)" },
];

export default function JobsDashboardPage() {
  const [secret, setSecret] = useState("");
  const [saved, setSaved] = useState(false);
  const [jobs, setJobs] = useState<JobRow[] | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [jobType, setJobType] = useState(JOB_TYPES[0].value);
  const [triggerMsg, setTriggerMsg] = useState<string | null>(null);

  const authHeader = useCallback(() => {
    return { Authorization: `Bearer ${secret.trim()}` };
  }, [secret]);

  const loadJobs = useCallback(async () => {
    setError(null);
    setLoading(true);
    try {
      const res = await fetch("/api/jobs", { headers: { ...authHeader() } });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setJobs(data.jobs ?? []);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Laden mislukt");
      setJobs(null);
    } finally {
      setLoading(false);
    }
  }, [authHeader]);

  const triggerJob = useCallback(async () => {
    setTriggerMsg(null);
    setError(null);
    try {
      const res = await fetch("/api/jobs/trigger", {
        method: "POST",
        headers: { ...authHeader(), "Content-Type": "application/json" },
        body: JSON.stringify({ job_type: jobType, trigger_source: "manual" }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error ?? res.statusText);
      setTriggerMsg(`Job aangemaakt: ${data.job?.id ?? "?"}`);
      await loadJobs();
    } catch (e) {
      setError(e instanceof Error ? e.message : "Trigger mislukt");
    }
  }, [authHeader, jobType, loadJobs]);

  return (
    <main style={{ maxWidth: 900, margin: "0 auto", padding: "2rem 1.25rem" }}>
      <h1 style={{ fontSize: "1.35rem", fontWeight: 600 }}>Jobs</h1>
      <p style={{ color: "var(--muted)", lineHeight: 1.6 }}>
        Voer hetzelfde geheim in als <code style={{ color: "var(--fg)" }}>JOB_TRIGGER_SECRET</code>{" "}
        (lokaal/Vercel). Alleen voor intern gebruik; later vervangen we dit door login.
      </p>

      <section style={{ marginTop: "1.25rem", display: "flex", flexWrap: "wrap", gap: "0.75rem" }}>
        <input
          type="password"
          autoComplete="off"
          placeholder="JOB_TRIGGER_SECRET"
          value={secret}
          onChange={(e) => {
            setSecret(e.target.value);
            setSaved(false);
          }}
          style={{
            flex: "1 1 220px",
            padding: "0.5rem 0.65rem",
            borderRadius: 6,
            border: "1px solid #334155",
            background: "#111827",
            color: "var(--fg)",
          }}
        />
        <button
          type="button"
          onClick={() => {
            setSaved(true);
            void loadJobs();
          }}
          disabled={!secret.trim()}
          style={btnStyle}
        >
          Opslaan &amp; verversen
        </button>
      </section>

      {saved && (
        <section style={{ marginTop: "1.25rem" }}>
          <label style={{ display: "block", marginBottom: 6, color: "var(--muted)" }}>Nieuwe job</label>
          <div style={{ display: "flex", flexWrap: "wrap", gap: "0.75rem", alignItems: "center" }}>
            <select
              value={jobType}
              onChange={(e) => setJobType(e.target.value)}
              style={{
                padding: "0.5rem 0.65rem",
                borderRadius: 6,
                border: "1px solid #334155",
                background: "#111827",
                color: "var(--fg)",
              }}
            >
              {JOB_TYPES.map((j) => (
                <option key={j.value} value={j.value}>
                  {j.label}
                </option>
              ))}
            </select>
            <button type="button" onClick={() => void triggerJob()} style={btnStyle}>
              Handmatig starten
            </button>
          </div>
          {triggerMsg && (
            <p style={{ marginTop: "0.75rem", color: "#34d399", fontSize: "0.9rem" }}>{triggerMsg}</p>
          )}
        </section>
      )}

      {error && (
        <p style={{ marginTop: "1rem", color: "#f87171" }} role="alert">
          {error}
        </p>
      )}

      <section style={{ marginTop: "1.5rem" }}>
        <h2 style={{ fontSize: "1.05rem", fontWeight: 600 }}>Laatste 50</h2>
        {loading && <p style={{ color: "var(--muted)" }}>Laden…</p>}
        {jobs && jobs.length === 0 && !loading && (
          <p style={{ color: "var(--muted)" }}>Nog geen jobs. Voer het geheim in en ververs.</p>
        )}
        {jobs && jobs.length > 0 && (
          <div style={{ overflowX: "auto", marginTop: "0.75rem" }}>
            <table style={{ width: "100%", borderCollapse: "collapse", fontSize: "0.85rem" }}>
              <thead>
                <tr style={{ textAlign: "left", borderBottom: "1px solid #334155" }}>
                  <th style={th}>Aangemaakt</th>
                  <th style={th}>Type</th>
                  <th style={th}>Status</th>
                  <th style={th}>Trigger</th>
                  <th style={th}>Log / fout</th>
                </tr>
              </thead>
              <tbody>
                {jobs.map((j) => (
                  <tr key={j.id} style={{ borderBottom: "1px solid #1e293b" }}>
                    <td style={td}>{new Date(j.created_at).toLocaleString("nl-NL")}</td>
                    <td style={td}>{j.job_type}</td>
                    <td style={td}>{j.status}</td>
                    <td style={td}>{j.trigger_source}</td>
                    <td style={td}>
                      {j.error_message && (
                        <span style={{ color: "#f87171" }}>{j.error_message}</span>
                      )}
                      {!j.error_message && (j.log_summary ?? "—")}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <p style={{ marginTop: "2rem", fontSize: "0.85rem", color: "var(--muted)" }}>
        <Link href="/">← Home</Link>
      </p>
    </main>
  );
}

const th: CSSProperties = { padding: "0.5rem 0.5rem 0.5rem 0", color: "var(--muted)", fontWeight: 600 };
const td: CSSProperties = { padding: "0.45rem 0.5rem 0.45rem 0", verticalAlign: "top" };
const btnStyle: CSSProperties = {
  padding: "0.5rem 1rem",
  borderRadius: 6,
  border: "none",
  background: "#2563eb",
  color: "#fff",
  cursor: "pointer",
  fontWeight: 500,
};
