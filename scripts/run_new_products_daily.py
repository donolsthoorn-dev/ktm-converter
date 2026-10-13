#!/usr/bin/env python3
"""
Dagelijkse nieuwe-producten pipeline (VPS na PIM-refresh).

  1) optioneel: vps_pim_refresh_weekly.py  (FTP fingerprint → skip als ongewijzigd)
  2) rebuild_shopify_deltas.py
  3) shopify_import_delta_products.py
  4) shopify_sync_missing_product_images.py  (na PIM-refresh: ontbrekende foto’s op bestaande)
  5) catalog mirror + YMM + complementary
     — altijd na een echte PIM-refresh (ook bij created=0, voor fitment/EMPF-updates)
     — overgeslagen bij fingerprint-noop of dry-run (tenzij --force-downstream)

  python3 scripts/run_new_products_daily.py
  python3 scripts/run_new_products_daily.py --skip-pim --brands hsq --dry-run-import
  python3 scripts/run_new_products_daily.py --force-pim
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.env_loader import load_dotenv  # noqa: E402

load_dotenv()

UNCHANGED_FLAG = ROOT / "output" / "logs" / "weekly_source_unchanged"
IMPORT_STATS = ROOT / "output" / "logs" / "last_delta_import_stats.json"
IMAGES_STATS = ROOT / "output" / "logs" / "last_missing_images_stats.json"

_PRODUCTS_DIR = {
    "ktm": ROOT / "output" / "products",
    "hsq": ROOT / "output" / "hsq" / "products",
    "wp": ROOT / "output" / "wp" / "products",
}
_INPUT_DIR = {
    "ktm": "input",
    "hsq": "input/hsq",
    "wp": "input/wp",
}


def _run(cmd: list[str], *, dry_run: bool = False) -> int:
    print("\n+", " ".join(cmd), flush=True)
    if dry_run:
        return 0
    return subprocess.call(cmd)


def _read_created_count() -> int | None:
    if not IMPORT_STATS.is_file():
        return None
    try:
        data = json.loads(IMPORT_STATS.read_text(encoding="utf-8"))
        return int(data.get("created") or 0)
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None


def _latest_delta(brand: str) -> Path | None:
    folder = _PRODUCTS_DIR.get(brand)
    if not folder:
        return None
    paths = glob.glob(str(folder / "shopify_export_delta_*.csv"))
    if not paths:
        return None
    return Path(max(paths, key=os.path.getmtime))


def _sync_missing_images(py: str, brands: list[str], *, dry_run: bool) -> int:
    """Voeg ontbrekende delta-foto’s toe aan bestaande Shopify-producten. Exit 0 tenzij hard fail."""
    total_ok = total_fail = 0
    by_brand: dict[str, dict[str, int]] = {}
    log_dir = ROOT / "output" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    for brand in brands:
        csv_path = _latest_delta(brand)
        if not csv_path:
            print(f"missing-images {brand}: geen delta-CSV, overslaan", flush=True)
            continue
        tasks_out = log_dir / f"missing_image_tasks_{brand}.json"
        cmd = [
            py,
            "scripts/shopify_sync_missing_product_images.py",
            "--csv",
            str(csv_path),
            "--input-dir",
            _INPUT_DIR[brand],
            "--tasks-out",
            str(tasks_out),
        ]
        if dry_run:
            cmd.append("--dry-run")
        print("\n+", " ".join(cmd), flush=True)
        proc = subprocess.run(cmd, capture_output=True, text=True)
        sys.stdout.write(proc.stdout or "")
        sys.stderr.write(proc.stderr or "")
        if proc.returncode != 0:
            print(
                f"missing-images {brand}: exit {proc.returncode} (pipeline gaat door)",
                flush=True,
            )
            by_brand[brand] = {"ok": 0, "fail": 0, "error": proc.returncode}
            continue
        ok = fail = 0
        combined = (proc.stdout or "") + (proc.stderr or "")
        m = re.search(
            r"DIGEST_METRIC missing_images_ok=(\d+) missing_images_fail=(\d+)",
            combined,
        )
        if m:
            ok, fail = int(m.group(1)), int(m.group(2))
        else:
            m2 = re.search(r"Klaar:\s*OK=(\d+),\s*mislukt=(\d+)", combined)
            if m2:
                ok, fail = int(m2.group(1)), int(m2.group(2))
        total_ok += ok
        total_fail += fail
        by_brand[brand] = {"ok": ok, "fail": fail}
        print(f"missing-images {brand}: ok={ok} fail={fail}", flush=True)

    print(
        f"missing-images totaal: ok={total_ok} fail={total_fail}",
        flush=True,
    )
    IMAGES_STATS.write_text(
        json.dumps(
            {
                "ok": total_ok,
                "fail": total_fail,
                "by_brand": by_brand,
                "dry_run": dry_run,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--brands", default="ktm,hsq,wp")
    ap.add_argument("--skip-pim", action="store_true", help="PIM-refresh overslaan")
    ap.add_argument(
        "--force-pim",
        action="store_true",
        help="PIM altijd draaien (negeer FTP-fingerprint)",
    )
    ap.add_argument("--skip-rebuild", action="store_true")
    ap.add_argument("--skip-import", action="store_true")
    ap.add_argument("--skip-missing-images", action="store_true")
    ap.add_argument("--skip-ymm", action="store_true")
    ap.add_argument("--skip-complementary", action="store_true")
    ap.add_argument(
        "--force-downstream",
        action="store_true",
        help="YMM/complementary/images forceren (ook bij dry-run of created=0 zonder PIM-refresh)",
    )
    ap.add_argument("--dry-run-import", action="store_true")
    ap.add_argument("--import-limit", type=int, default=0)
    ap.add_argument("--worker-rounds", type=int, default=40, help="Max supabase_job_worker loops")
    args = ap.parse_args()

    py = sys.executable
    brands = args.brands
    brand_list = [b.strip().lower() for b in brands.split(",") if b.strip()]
    pim_refreshed = False

    if not args.skip_pim:
        pim_cmd = [py, "scripts/vps_pim_refresh_weekly.py", "--brands", brands]
        if args.force_pim:
            pim_cmd.append("--force")
        rc = _run(pim_cmd)
        if rc != 0:
            return rc
        if UNCHANGED_FLAG.is_file():
            print(
                "\nFTP-bronnen ongewijzigd — rebuild/import/images/YMM/complementary overgeslagen.",
                flush=True,
            )
            print("Dagelijkse pipeline klaar (noop).", flush=True)
            return 0
        pim_refreshed = True

    if not args.skip_rebuild:
        rc = _run([py, "-u", "scripts/rebuild_shopify_deltas.py", "--brands", brands])
        if rc != 0:
            return rc

    created: int | None = None
    if not args.skip_import:
        IMPORT_STATS.unlink(missing_ok=True)
        cmd = [py, "scripts/shopify_import_delta_products.py", "--brands", brands]
        if args.dry_run_import:
            cmd.append("--dry-run")
        if args.import_limit:
            cmd.extend(["--limit", str(args.import_limit)])
        rc = _run(cmd)
        if rc != 0:
            return rc
        created = _read_created_count()

    if args.dry_run_import and not args.force_downstream:
        print(
            "\nDry-run import — images/YMM/complementary overgeslagen "
            "(gebruik --force-downstream om toch te draaien).",
            flush=True,
        )
        print("\nDagelijkse nieuwe-producten pipeline klaar.", flush=True)
        return 0

    # Gat dicht: na echte PIM-refresh altijd images/YMM/complementary, ook bij created=0
    skip_heavy = (
        not args.force_downstream
        and not pim_refreshed
        and created is not None
        and created == 0
    )
    if skip_heavy:
        print(
            "\nImport created=0 en geen PIM-refresh — images/YMM/complementary overgeslagen "
            "(gebruik --force-downstream of laat PIM draaien).",
            flush=True,
        )
        print("\nDagelijkse nieuwe-producten pipeline klaar.", flush=True)
        return 0

    if pim_refreshed and created == 0:
        print(
            "\nPIM vernieuwd, created=0 — images/YMM/complementary voor bestaande producten.",
            flush=True,
        )

    if not args.skip_missing_images and (pim_refreshed or args.force_downstream):
        _sync_missing_images(py, brand_list, dry_run=False)

    if not args.skip_ymm:
        rc = _run(
            [
                py,
                "scripts/queue_supabase_job.py",
                "shopify_catalog_mirror",
                "--trigger",
                "manual",
            ]
        )
        if rc != 0:
            return rc
        for i in range(max(1, args.worker_rounds)):
            print(f"\n--- supabase_job_worker round {i + 1}/{args.worker_rounds} ---", flush=True)
            proc = subprocess.run(
                [py, "scripts/supabase_job_worker.py"],
                capture_output=True,
                text=True,
            )
            sys.stdout.write(proc.stdout or "")
            sys.stderr.write(proc.stderr or "")
            if proc.returncode != 0:
                return proc.returncode
            combined = (proc.stdout or "") + (proc.stderr or "")
            if "Geen queued jobs." in combined:
                break
            time.sleep(2)

        rc = _run([py, "scripts/run_canonical_ymm_pipeline.py", "--sync-only", "--write"])
        if rc != 0:
            return rc
        rc = _run([py, "scripts/run_canonical_ymm_pipeline.py", "--push-only", "--write"])
        if rc != 0:
            return rc

    if not args.skip_complementary:
        cmd = [py, "scripts/shopify_sync_empfart_complementary.py", "--force-write"]
        if created is None or created > 0 or pim_refreshed or args.force_downstream:
            cmd.append("--force-refresh-cache")
        rc = _run(cmd)
        if rc != 0:
            return rc

    print("\nDagelijkse nieuwe-producten pipeline klaar.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
