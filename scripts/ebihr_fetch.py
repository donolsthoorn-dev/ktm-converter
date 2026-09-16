#!/usr/bin/env python3
"""
Fetch eBihr V3 catalogs + VSE exports → motox/e-bihr/raw/{timestamp}/

  python3 scripts/ebihr_fetch.py
  python3 scripts/ebihr_fetch.py --catalogs Products Prices Stocks
  python3 scripts/ebihr_fetch.py --no-vse
  python3 scripts/ebihr_fetch.py --out-dir motox/e-bihr/raw/manual_test
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.client import BihrClient, V3_CATALOGS  # noqa: E402
from modules.ebihr.constants import RAW_ROOT  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch eBihr V3 + VSE raw data.")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Doelmap (default: motox/e-bihr/raw/<unix_ts>).",
    )
    ap.add_argument(
        "--catalogs",
        nargs="+",
        default=["Products", "Prices", "Stocks"],
        choices=list(V3_CATALOGS),
        help="V3 catalogi om op te halen.",
    )
    ap.add_argument("--language", default="nl", help="Catalog language (default: nl).")
    ap.add_argument("--no-vse", action="store_true", help="Sla VSE vehicles/links over.")
    args = ap.parse_args()

    out = args.out_dir or (RAW_ROOT / str(int(time.time())))
    client = BihrClient()
    client.fetch_all(
        out,
        catalogs=tuple(args.catalogs),
        language=args.language,
        include_vse=not args.no_vse,
    )
    print(f"Klaar: {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
