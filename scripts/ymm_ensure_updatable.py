#!/usr/bin/env python3
"""
Controleer of er producten zijn die YMM-bijwerking kunnen krijgen, en werk bij.

"Kan bijwerken" = canonical_product_fits_on heeft ymm_json én content_hash wijkt af
van pushed_hash (of pushed_hash leeg). Optioneel daarna backfill van nog lege
Shopify-velden vanuit shopify_ymm_projection.

  python3 scripts/ymm_ensure_updatable.py --report-only
  python3 scripts/ymm_ensure_updatable.py --write --push --backfill --limit 500
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()


def _supabase() -> tuple[object, str, dict[str, str]]:
    import requests

    base = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base or not key:
        raise SystemExit("SUPABASE_URL en SUPABASE_SERVICE_ROLE_KEY verplicht")
    rest_base = f"{base}/rest/v1"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    session = requests.Session()
    session.trust_env = False
    return session, rest_base, headers


def _count_pending(session, rest_base: str, headers: dict[str, str], *, limit: int) -> int:
    from modules.shopify_ymm_supabase_pipeline import _fetch_canonical_fits_on_rows

    # limit=0 in fetch = geen limiet; hier willen we een teller. Cap scan voor snelle report.
    scan_cap = limit if limit > 0 else 50_000
    rows = _fetch_canonical_fits_on_rows(
        session,
        rest_base,
        headers,
        product_ids=None,
        limit=scan_cap,
        only_diff=True,
    )
    return len(rows)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report-only", action="store_true", help="Alleen tellen/loggen")
    ap.add_argument("--write", action="store_true", help="Shopify/Supabase echt schrijven")
    ap.add_argument("--push", action="store_true", help="Diff-push als er pending is")
    ap.add_argument(
        "--backfill",
        action="store_true",
        help="Lege Shopify YMM-velden vullen vanuit projection",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max producten per push/backfill (0 = geen limiet)",
    )
    args = ap.parse_args()

    dry_run = not args.write
    session, rest_base, headers = _supabase()

    pending = _count_pending(session, rest_base, headers, limit=args.limit or 50_000)
    print(f"YMM bij te werken (canonical diff): {pending}", flush=True)
    print(f"DIGEST_METRIC ymm_pending={pending}", flush=True)

    if args.report_only or (not args.push and not args.backfill):
        return 0

    from modules.shopify_ymm_supabase_pipeline import (
        run_shopify_ymm_backfill_from_supabase,
        run_shopify_ymm_push_from_supabase,
    )

    combined: dict = {"pending_before": pending}

    if args.push:
        if pending <= 0:
            print("Geen YMM-diff — push overgeslagen.", flush=True)
            combined["push"] = {"skipped": True, "reason": "pending=0"}
        else:
            payload = {
                "dry_run": dry_run,
                "overwrite": True,
                "only_diff": True,
                "limit": max(0, args.limit),
            }
            stats, err = run_shopify_ymm_push_from_supabase(
                payload, session, rest_base, headers
            )
            combined["push"] = stats
            if err:
                print(f"FOUT push: {err}", file=sys.stderr)
                print(json.dumps(combined, indent=2, ensure_ascii=False))
                return 1
            pending_after = _count_pending(
                session, rest_base, headers, limit=args.limit or 50_000
            )
            combined["pending_after_push"] = pending_after
            print(f"YMM nog pending na push: {pending_after}", flush=True)
            print(f"DIGEST_METRIC ymm_pending_after={pending_after}", flush=True)

    if args.backfill:
        payload = {
            "dry_run": dry_run,
            "limit": max(0, args.limit),
        }
        stats, err = run_shopify_ymm_backfill_from_supabase(
            payload, session, rest_base, headers
        )
        combined["backfill"] = stats
        if err:
            print(f"FOUT backfill: {err}", file=sys.stderr)
            print(json.dumps(combined, indent=2, ensure_ascii=False))
            return 1
        cands = int(stats.get("candidate_products") or 0)
        upd = int(stats.get("updated_products") or 0)
        print(
            f"YMM backfill lege velden: candidates={cands}, updated={upd}",
            flush=True,
        )
        print(
            f"DIGEST_METRIC ymm_backfill_candidates={cands} ymm_backfill_updated={upd}",
            flush=True,
        )

    print(json.dumps(combined, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
