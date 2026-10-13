#!/usr/bin/env python3
"""
Wekelijkse PIM-refresh op de VPS: FTP → XML+PHO extract → zip weg → oude XML prune,
plus prijs-CSV’s (0150/1100/0910/0140) voor rebuild/main.py.

Geen Shopify-writes. Prijs/ETA-*sync* naar Shopify blijft nachtelijke GHA.

  python3 scripts/vps_pim_refresh_weekly.py
  python3 scripts/vps_pim_refresh_weekly.py --brands hsq --dry-run
  python3 scripts/vps_pim_refresh_weekly.py --force   # negeer FTP-fingerprint

Zonder wijziging op FTP (size/mtime): exit 0 + flag weekly_source_unchanged
(zodat run_new_products_weekly de zware stappen overslaat).

Na succesvolle refresh: output/logs/pim_ready_YYYYMMDD + fingerprint opslaan.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.brand_config import PRICELIST_CSV_MERGE_ORDER  # noqa: E402
from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

sys.path.insert(0, str(ROOT / "scripts"))
from fetch_input_sftp import collect_remote_fingerprints  # noqa: E402

PIM_ZIPS: dict[str, tuple[str, str]] = {
    # brand -> (zip filename, prepare --input-dir)
    "ktm": ("XML_Export_PIM_KTM_International.zip", "input"),
    "hsq": ("XML_Export_PIM_HQV_International.zip", "input/hsq"),
    "wp": ("XML_Export_PIM_WP_International.zip", "input/wp"),
}

# main.py / rebuild heeft deze CSV’s nodig (niet in de PIM-zip).
PRICE_CSVS: tuple[str, ...] = tuple(PRICELIST_CSV_MERGE_ORDER)

FINGERPRINT_PATH = ROOT / "output" / "logs" / "weekly_ftp_fingerprint.json"
UNCHANGED_FLAG = ROOT / "output" / "logs" / "weekly_source_unchanged"


def _run(cmd: list[str], *, dry_run: bool) -> int:
    print("+", " ".join(cmd), flush=True)
    if dry_run:
        return 0
    return subprocess.call(cmd)


def _watched_pim_files(brands: list[str]) -> list[str]:
    """Alleen PIM-zips bepalen of de weekpipeline mag stoppen.

    Prijs-CSV’s wijzigen vaak nachtelijks; die triggeren geen rebuild/import
    (prijs/ETA zit al in ktm_price_eta_status_sync).
    """
    return [PIM_ZIPS[b][0] for b in brands]


def _load_prev_fingerprint() -> dict:
    if not FINGERPRINT_PATH.is_file():
        return {}
    try:
        return json.loads(FINGERPRINT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _fingerprints_equal(prev: dict, current: dict[str, dict]) -> bool:
    prev_files = (prev or {}).get("files") or {}
    if not prev_files or not current:
        return False
    if set(prev_files.keys()) != set(current.keys()):
        return False
    for name, cur in current.items():
        old = prev_files.get(name) or {}
        if cur.get("missing") or old.get("missing"):
            return False
        if cur.get("size") != old.get("size"):
            return False
        # mtime optioneel: als beide gezet en verschillend → changed
        cm, om = cur.get("mtime"), old.get("mtime")
        if cm is not None and om is not None and cm != om:
            return False
    return True


def _save_fingerprint(files: dict[str, dict], brands: list[str]) -> None:
    FINGERPRINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "brands": brands,
        "files": files,
    }
    FINGERPRINT_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brands", default="ktm,hsq,wp", help="Komma-gescheiden merken")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-fetch", action="store_true", help="Alleen prepare uit bestaande staging")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Negeer FTP-fingerprint; altijd ophalen/extracten",
    )
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
    UNCHANGED_FLAG.unlink(missing_ok=True)

    watched = _watched_pim_files(brands)
    remote_fp: dict[str, dict] = {}
    if not args.skip_fetch and not args.force:
        print("=== FTP fingerprint PIM-zips (size/mtime, geen download) ===", flush=True)
        try:
            remote_fp = collect_remote_fingerprints(watched)
        except SystemExit:
            raise
        except Exception as exc:
            print(f"Fingerprint mislukt ({exc}); ga door met volledige fetch.", flush=True)
            remote_fp = {}
        else:
            for name, meta in remote_fp.items():
                print(
                    f"  {name}: size={meta.get('size')} mtime={meta.get('mtime')} "
                    f"missing={meta.get('missing')}",
                    flush=True,
                )
            prev = _load_prev_fingerprint()
            if _fingerprints_equal(prev, remote_fp):
                UNCHANGED_FLAG.write_text(
                    f"at={datetime.now(timezone.utc).isoformat()}\nbrands={','.join(brands)}\n",
                    encoding="utf-8",
                )
                print(
                    "PIM-zips ongewijzigd t.o.v. vorige weekrun — PIM/extract overgeslagen "
                    f"(flag {UNCHANGED_FLAG.name}). Prijs-CSV’s blijven nachtelijke sync.",
                    flush=True,
                )
                return 0

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

    # Prijs-CSV’s (0150/1100/0910/0140) — zelfde FTP als nachtelijke prijs-sync
    print("\n=== Prijs-CSV’s ===", flush=True)
    price_files = ",".join(PRICE_CSVS)
    if not args.skip_fetch:
        rc = _run(
            [py, "scripts/fetch_input_sftp.py", "--files", price_files],
            dry_run=args.dry_run,
        )
        if rc != 0:
            print(f"Fetch prijs-CSV’s mislukt (exit {rc})", file=sys.stderr)
            return rc
    rc = _run(
        [
            py,
            "scripts/prepare_input_from_ftp.py",
            "--staging",
            str(staging),
            "--input-dir",
            "input",
            "--files",
            price_files,
        ],
        dry_run=args.dry_run,
    )
    if rc != 0:
        print(f"Prepare prijs-CSV’s mislukt (exit {rc})", file=sys.stderr)
        return rc

    # Staging opruimen (restanten)
    if not args.dry_run and staging.is_dir():
        for p in staging.iterdir():
            if p.is_file() and p.suffix.lower() in {".zip", ".csv"}:
                p.unlink(missing_ok=True)
                print(f"Staging weg: {p.name}")

    day = datetime.now(ZoneInfo("Europe/Amsterdam")).strftime("%Y%m%d")
    ready = log_dir / f"pim_ready_{day}"
    if args.dry_run:
        print(f"[dry-run] would touch {ready}")
    else:
        ready.write_text(f"brands={','.join(brands)}\n", encoding="utf-8")
        print(f"Ready-flag: {ready}")
        # Fingerprint na succes (herstuur remote of zojuist gemeten)
        if not remote_fp:
            try:
                remote_fp = collect_remote_fingerprints(watched)
            except Exception as exc:
                print(f"Kon fingerprint na fetch niet opslaan: {exc}", flush=True)
                remote_fp = {}
        if remote_fp:
            _save_fingerprint(remote_fp, brands)
            print(f"Fingerprint opgeslagen: {FINGERPRINT_PATH}", flush=True)

    print("PIM-refresh klaar.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
