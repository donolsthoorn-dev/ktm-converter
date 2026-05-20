#!/usr/bin/env python3
"""
Draai main.py voor KTM, HSQ en WP (alleen product-CSV's — geen Supabase, geen Shopify).

Daarna: delta-CSV's handmatig importeren in Shopify Admin (zie docs/STAPPENPLAN.md §3).

  python3 scripts/run_main_all_brands.py
  python3 scripts/run_main_all_brands.py --brands ktm,hsq
  python3 scripts/run_main_all_brands.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

DEFAULT_BRANDS = ("ktm", "hsq", "wp")


def main() -> int:
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    from modules.brand_config import VALID_BRAND_IDS, normalize_brand_id

    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--brands",
        default=",".join(DEFAULT_BRANDS),
        help=f"Komma-gescheiden (default: {','.join(DEFAULT_BRANDS)})",
    )
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    brands = [normalize_brand_id(b.strip()) for b in args.brands.split(",") if b.strip()]
    for b in brands:
        if b not in VALID_BRAND_IDS:
            print(f"Onbekend merk: {b}", file=sys.stderr)
            return 1

    failed = False
    for brand_id in brands:
        cmd = [sys.executable, "-u", str(ROOT / "main.py")]
        if brand_id != "ktm":
            cmd.extend(["--brand", brand_id])
        if args.dry_run:
            print(f"[dry-run] {' '.join(cmd)}", flush=True)
            continue
        print(f"\n=== {brand_id} ===", flush=True)
        t0 = time.time()
        proc = subprocess.run(cmd, cwd=ROOT)
        if proc.returncode != 0:
            print(f"main.py ({brand_id}) mislukt (exit {proc.returncode})", file=sys.stderr)
            failed = True
        else:
            print(f"Klaar in {time.time() - t0:.0f}s", flush=True)

    if not args.dry_run and not failed:
        print(
            "\nVolgende stap: Shopify Admin → import delta-CSV per merk (STAPPENPLAN §3).",
            flush=True,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
