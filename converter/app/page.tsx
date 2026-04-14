export default function HomePage() {
  return (
    <main style={{ maxWidth: 720, margin: "0 auto", padding: "2rem 1.25rem" }}>
      <h1 style={{ fontSize: "1.5rem", fontWeight: 600 }}>KTM Converter</h1>
      <p style={{ color: "var(--muted)", lineHeight: 1.6 }}>
        Webapp voor status, triggers en (later) exports. De overnight Shopify → Supabase
        sync draait via GitHub Actions of een worker; deze app leest uit Supabase.
      </p>
      <ul style={{ lineHeight: 1.8 }}>
        <li>
          <a href="/api/health">/api/health</a> — basis health check
        </li>
        <li>
          <a href="/api/health/supabase">/api/health/supabase</a> — database bereikbaar
          (vereist env vars)
        </li>
        <li>
          <a href="/jobs">/jobs</a> — joblijst + handmatig triggeren (met{" "}
          <code>JOB_TRIGGER_SECRET</code>)
        </li>
      </ul>
    </main>
  );
}
