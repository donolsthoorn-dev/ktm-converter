#!/usr/bin/env python3
"""
Wekelijkse PIM-refresh op de VPS: FTP → XML+PHO extract → zip weg → oude XML prune.

Geen Shopify-writes. Prijs/ETA blijft in de nachtelijke GHA.

  python3 scripts/vps_pim_refresh_weekly.py
  python3 scripts/vps_pim_refresh_weekly.py --brands hsq --dry-run

Na succes: output/logs/pim_ready_YYYYMMDD (touch).
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

PIM_ZIPS: dict[str, tuple[str, str]] = {
    # brand -> (zip filename, prepare --input-dir)
    "ktm": ("XML_Export_PIM_KTM_International.zip", "input"),
    "hsq": ("XML_Export_PIM_HQV_International.zip", "input/hsq"),
    "wp": ("XML_Export_PIM_WP_International.zip", "input/wp"),
}


def _run(cmd: list[str], *, dry_run: bool) -> int:
    print("+", " ".join(cmd), flush=True)
    if dry_run:
        return 0
    return subprocess.call(cmd)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brands", default="ktm,hsq,wp", help="Komma-gescheiden merken")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true", help="Alleen prepare uit bestaande staging")
    args = ap.parse_args()

    brands = [b.strip().lower() for b in args.brands.split(",") if b.strip()]
    for b in brands:
        if b not in PIM_ZIPS:
            print(f"Onbekend merk: {b}", file=sys.stderr)
            return 2

    staging = Path(os.environ.get("KTM_SFTP_LOCAL_DIR", "downloads/ftp"))
    staging.mkdir(parents=True, exist_ok=True)
    log_dir = ROOT / "output" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    py = sys.executable
    for brand in brands:
        zip_name, input_dir = PIM_ZIPS[brand]
        print(f"\n=== {brand.upper()}: {zip_name} ===", flush=True)

        if not args.skip_fetch:
            rc = _run(
                [
                    py,
                    "scripts/fetch_input_sftp.py",
                    "--files",
                    zip_name,
                ],
                dry_run=args.dry_run,
            )
            if rc != 0:
                print(f"Fetch mislukt voor {brand} (exit {rc})", file=sys.stderr)
                return rc

        zip_path = staging / zip_name
        if not args.dry_run and not zip_path.is_file():
            print(f"Zip ontbreekt na fetch: {zip_path}", file=sys.stderr)
            return 1

        rc = _run(
            [
                py,
                "scripts/prepare_input_from_ftp.py",
                "--staging",
                str(staging),
                "--input-dir",
                input_dir,
                "--files",
                zip_name,
                "--extract-xml-from-zips",
                "--extract-media-from-zips",
                "--prune-old-xml",
            ],
            dry_run=args.dry_run,
        )
        if rc != 0:
            print(f"Prepare mislukt voor {brand} (exit {rc})", file=sys.stderr)
            return rc

    # Staging opruimen (restanten)
    if not args.dry_run and staging.is_dir():
        for p in staging.iterdir():
            if p.is_file() and p.suffix.lower() == ".zip":
                p.unlink(missing_ok=True)
                print(f"Staging zip weg: {p.name}")

    day = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%Y%m%d")
    ready = log_dir / f"pim_ready_{day}"
    if args.dry_run:
        print(f"[dry-run] would touch {ready}")
    else:
        ready.write_text(f"brands={','.join(brands)}\n", encoding="utf-8")
        print(f"Ready-flag: {ready}")

    print("PIM-refresh klaar.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
