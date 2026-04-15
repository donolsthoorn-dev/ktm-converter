#!/usr/bin/env python3
"""
Pakt één `queued` job uit Supabase `jobs`, zet deze op running → success/failed (stub).

Bedoeld voor GitHub Actions (schedule / workflow_dispatch). Vereist o.a.:

  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
  Voor job_type shopify_catalog_mirror: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN
  (zie .env.example / workflow job-worker.yml)

Gebruik (vanaf projectroot):

  SUPABASE_URL=https://xxx.supabase.co SUPABASE_SERVICE_ROLE_KEY=eyJ... \\
 python3 scripts/supabase_job_worker.py

Exitcode: 0 als er geen job was of de run gelukt is; 1 bij configuratie-/HTTP-fout.
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

_REQUEST_TIMEOUT = (15, 60)


def _rest_base() -> str:
    url = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    if not url:
        print("SUPABASE_URL ontbreekt", file=sys.stderr)
        raise SystemExit(1)
    return f"{url}/rest/v1"


def _headers() -> dict[str, str]:
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not key:
        print("SUPABASE_SERVICE_ROLE_KEY ontbreekt", file=sys.stderr)
        raise SystemExit(1)
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _pick_queued(session: requests.Session, base: str, headers: dict[str, str]) -> dict[str, Any] | None:
    r = session.get(
        f"{base}/jobs",
        headers=headers,
        params={
            "select": "*",
            "status": "eq.queued",
            "order": "created_at.asc",
            "limit": "1",
        },
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        return None
    return rows[0]


def _patch_job(
    session: requests.Session,
    base: str,
    headers: dict[str, str],
    job_id: uuid.UUID,
    body: dict[str, Any],
) -> None:
    hid = str(job_id)
    r = session.patch(
        f"{base}/jobs",
        headers=headers,
        params={"id": f"eq.{hid}"},
        json=body,
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()


def _run_stub(job: dict[str, Any]) -> tuple[str, str | None]:
    """Returns (log_summary, error_message)."""
    jt = job.get("job_type") or ""
    if jt == "worker_stub":
        time.sleep(1)
        return (f"Stub voltooid voor job_type={jt}", None)
    return ("", f"Onbekend job_type (nog niet geïmplementeerd): {jt!r}")


def _run_shopify_mirror(
    job: dict[str, Any],
    session: requests.Session,
    base: str,
    headers: dict[str, str],
) -> tuple[str, str | None]:
    del job  # payload kan later gebruikt worden
    from modules.shopify_supabase_mirror import run_mirror

    log_lines: list[str] = []

    def _log(msg: str) -> None:
        log_lines.append(msg)
        # Realtime output in GitHub Actions; zonder dit lijkt de job "vast" te hangen.
        print(msg, flush=True)

    stats, err = run_mirror(session, base, headers, log=_log)
    summary = "\n".join(log_lines).strip()
    if not summary and stats:
        summary = json.dumps(stats, ensure_ascii=False)
    return (summary, err)


def _dispatch_job(
    job: dict[str, Any],
    session: requests.Session,
    base: str,
    headers: dict[str, str],
) -> tuple[str, str | None]:
    jt = job.get("job_type") or ""
    if jt == "shopify_catalog_mirror":
        return _run_shopify_mirror(job, session, base, headers)
    return _run_stub(job)


def main() -> int:
    base = _rest_base()
    headers = _headers()
    session = requests.Session()
    session.trust_env = False

    try:
        job = _pick_queued(session, base, headers)
    except requests.RequestException as e:
        print(f"Supabase GET mislukt: {e}", file=sys.stderr)
        if e.response is not None:
            print(e.response.text[:2000], file=sys.stderr)
        return 1

    if not job:
        print("Geen queued jobs.")
        return 0

    jid = uuid.UUID(str(job["id"]))
    print(f"Job {jid} ({job.get('job_type')}) → running")

    try:
        _patch_job(
            session,
            base,
            headers,
            jid,
            {"status": "running", "started_at": _iso_now()},
        )
    except requests.RequestException as e:
        print(f"PATCH running mislukt: {e}", file=sys.stderr)
        return 1

    log_summary, err = _dispatch_job(job, session, base, headers)
    finished: dict[str, Any] = {
        "finished_at": _iso_now(),
        "log_summary": log_summary or None,
        "error_message": err,
        "status": "failed" if err else "success",
    }

    try:
        _patch_job(session, base, headers, jid, finished)
    except requests.RequestException as e:
        print(f"PATCH afronden mislukt: {e}", file=sys.stderr)
        return 1

    print(f"Job {jid} → {finished['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
