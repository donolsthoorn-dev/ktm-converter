#!/usr/bin/env python3
"""
Zet één rij in Supabase `jobs` met status queued (voor schedule / automatisering).

Gebruik (vanaf projectroot):

  python3 scripts/queue_supabase_job.py shopify_catalog_mirror
  python3 scripts/queue_supabase_job.py shopify_catalog_mirror --trigger schedule

Vereist: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

_TIMEOUT = (15, 60)


def main() -> int:
    p = argparse.ArgumentParser(description="Queue Supabase job")
    p.add_argument("job_type", help="bv. shopify_catalog_mirror")
    p.add_argument(
        "--trigger",
        choices=("manual", "schedule", "api"),
        default="schedule",
        help="trigger_source in DB (default: schedule)",
    )
    args = p.parse_args()

    base = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base or not key:
        print("SUPABASE_URL en SUPABASE_SERVICE_ROLE_KEY zijn verplicht", file=sys.stderr)
        return 1

    url = f"{base}/rest/v1/jobs"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    body = {
        "job_type": args.job_type.strip(),
        "status": "queued",
        "trigger_source": args.trigger,
        "payload": {},
    }
    r = requests.post(
        url,
        headers=headers,
        data=json.dumps(body),
        timeout=_TIMEOUT,
    )
    if not r.ok:
        print(r.text[:2000], file=sys.stderr)
        return 1
    rows = r.json()
    jid = rows[0].get("id") if rows else "?"
    print(f"Job queued: {jid} type={args.job_type}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
