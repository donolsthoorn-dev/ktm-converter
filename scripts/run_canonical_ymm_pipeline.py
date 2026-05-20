#!/usr/bin/env python3
"""
XML (cross-brand) → Supabase canonical_product_fits_on → projection → Shopify (diff push).

Lokaal testen met enkele handles (dry-run):

  python3 scripts/run_canonical_ymm_pipeline.py \\
    --handles wp-a54029994500,hsq-a54029994500,a54029994500 \\
    --dry-run

Echte write (Supabase + Shopify):

  python3 scripts/run_canonical_ymm_pipeline.py \\
    --handles wp-a54029994500,hsq-a54029994500,a54029994500 \\
    --write

Volledige catalogus (alle producten in Supabase met SKU):

  python3 scripts/run_canonical_ymm_pipeline.py --sync-only --write

Alleen bepaalde handles:

  python3 scripts/run_canonical_ymm_pipeline.py --handles wp-a54029994500 --sync-only --write

CSV-export voor Metafields Manager blijft apart:

  python3 scripts/export_product_metafields.py --brand ktm|hsq|wp
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


def main() -> int:
    p = argparse.ArgumentParser(description="Canonical YMM pipeline (XML → Supabase → Shopify)")
    p.add_argument(
        "--handles",
        default="",
        help="Komma-gescheiden Shopify handles; weglaten = alle producten met SKU in Supabase.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max aantal producten (0 = geen limiet). Handig voor eerste test-run.",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Geen writes naar Supabase of Shopify (default als --write niet gezet)",
    )
    p.add_argument(
        "--write",
        action="store_true",
        help="Schrijf naar Supabase en Shopify (overschrijft metafields).",
    )
    p.add_argument(
        "--sync-only",
        action="store_true",
        help="Alleen stap 1+2 (Supabase sync + projection), geen Shopify push.",
    )
    p.add_argument(
        "--push-only",
        action="store_true",
        help="Alleen diff-push naar Shopify (canonical_product_fits_on moet al gevuld zijn).",
    )
    p.add_argument(
        "--full-push",
        action="store_true",
        help="Push alle opgegeven handles, niet alleen content_hash <> pushed_hash.",
    )
    args = p.parse_args()

    dry_run = not args.write
    if args.dry_run:
        dry_run = True

    handles = [h.strip().lower() for h in (args.handles or "").split(",") if h.strip()]
    payload: dict = {
        "dry_run": dry_run,
        "overwrite": True,
        "only_diff": not args.full_push,
        "limit": max(0, args.limit),
    }
    if handles:
        payload["only_handles"] = handles

    scope = f"{len(handles)} handles" if handles else "hele catalogus (SKU in Supabase)"
    print(f"Run: dry_run={dry_run}, scope={scope}, limit={payload['limit']}", flush=True)

    try:
        import requests
    except ImportError:
        print("pip install requests", file=sys.stderr)
        return 1

    base = os.environ.get("SUPABASE_URL", "").strip().rstrip("/")
    key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not base or not key:
        print("SUPABASE_URL en SUPABASE_SERVICE_ROLE_KEY verplicht", file=sys.stderr)
        return 1

    rest_base = f"{base}/rest/v1"
    headers = {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    session = requests.Session()
    session.trust_env = False

    if args.push_only:
        from modules.shopify_ymm_supabase_pipeline import run_shopify_ymm_push_from_supabase

        stats, err = run_shopify_ymm_push_from_supabase(payload, session, rest_base, headers)
    elif args.sync_only:
        from modules.canonical_ymm_supabase_sync import run_sync_canonical_ymm_to_supabase
        from modules.shopify_ymm_supabase_pipeline import run_refresh_shopify_ymm_projection

        stats, err = run_sync_canonical_ymm_to_supabase(payload, session, rest_base, headers)
        if not err:
            stats2, err = run_refresh_shopify_ymm_projection(session, rest_base, headers)
            stats = {"sync": stats, "projection": stats2}
    else:
        from modules.shopify_ymm_supabase_pipeline import run_canonical_ymm_pipeline

        stats, err = run_canonical_ymm_pipeline(payload, session, rest_base, headers)

    print(json.dumps(stats, indent=2, ensure_ascii=False))
    if err:
        print(f"FOUT: {err}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
