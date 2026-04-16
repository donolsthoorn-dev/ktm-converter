#!/usr/bin/env python3
"""
Stap 2: lees het JSON van shopify_compare_export_images.py en koppel ontbrekende
afbeeldingen aan producten in Shopify (parallelle POST’s).

  python3 scripts/shopify_apply_missing_images.py
  python3 scripts/shopify_apply_missing_images.py --tasks output/logs/shopify_missing_image_tasks.json
  python3 scripts/shopify_apply_missing_images.py --dry-run
  python3 scripts/shopify_apply_missing_images.py --apply-workers 12
  python3 scripts/shopify_apply_missing_images.py --skip-input
  python3 scripts/shopify_apply_missing_images.py --input-dir /pad/naar/images

Stap 1: scripts/shopify_compare_export_images.py
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

from modules.shopify_export_images_lib import (  # noqa: E402
    apply_missing_images_parallel,
    default_tasks_path,
    flatten_tasks_from_payload,
    load_tasks_json,
)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Voeg ontbrekende productimages toe op basis van een taken-JSON (stap 2)."
    )
    ap.add_argument(
        "--tasks",
        metavar="PATH",
        default=None,
        help=f"JSON van shopify_compare_export_images.py (default: {default_tasks_path()})",
    )
    ap.add_argument(
        "--apply-workers",
        type=int,
        default=8,
        metavar="N",
        help="Parallelle POST’s (default: 8)",
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Toon aantal POST’s, voer geen wijzigingen uit",
    )
    ap.add_argument(
        "--skip-input",
        action="store_true",
        help="Geen lokale bestanden onder input/ gebruiken (alleen CSV-URL’s naar Shopify)",
    )
    ap.add_argument(
        "--input-dir",
        metavar="DIR",
        default=None,
        help=f"Zoekmap voor lokale afbeeldingen (default: {config.INPUT_DIR})",
    )
    args = ap.parse_args()

    if os.environ.get("KTM_SKIP_SHOPIFY_API", "").strip().lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            "KTM_SKIP_SHOPIFY_API is gezet — dit script heeft live Shopify nodig.",
            file=sys.stderr,
        )
        raise SystemExit(1)

    if not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        raise SystemExit(1)

    path = args.tasks if args.tasks is not None else default_tasks_path()
    if not os.path.isfile(path):
        print(
            f"Geen takenbestand: {path!r}\n"
            "Eerst: python3 scripts/shopify_compare_export_images.py",
            file=sys.stderr,
        )
        raise SystemExit(1)

    payload = load_tasks_json(path)
    tasks = flatten_tasks_from_payload(payload)
    src_csv = payload.get("source_csv") or "(onbekend)"
    print(f"Takenbestand: {path}", flush=True)
    print(f"Bron-CSV in bestand: {src_csv}", flush=True)
    print(f"Te verwerken image-POST’s: {len(tasks)}", flush=True)

    if not tasks:
        print("Geen taken — niets te doen.", flush=True)
        return

    if args.dry_run:
        print("Dry-run: geen POST’s uitgevoerd.", flush=True)
        return

    prefer_input = not args.skip_input
    input_dir = args.input_dir or config.INPUT_DIR
    print(
        f"\nImages koppelen (parallel, apply-workers={args.apply_workers}"
        + (
            f", lokale bron={input_dir!r}"
            if prefer_input
            else ", alleen CSV-URL’s"
        )
        + ")...",
        flush=True,
    )
    ok, fail = apply_missing_images_parallel(
        tasks,
        args.apply_workers,
        input_dir=input_dir,
        prefer_input=prefer_input,
    )
    print(f"\nKlaar: toegevoegd OK={ok}, mislukt={fail}.", flush=True)


if __name__ == "__main__":
    main()
