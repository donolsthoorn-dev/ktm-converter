#!/usr/bin/env python3
"""
Wekelijkse nieuwe-producten pipeline (bedoeld op VPS na PIM-refresh).

  1) optioneel: vps_pim_refresh_weekly.py  (FTP fingerprint → skip als ongewijzigd)
  2) rebuild_shopify_deltas.py
  3) shopify_import_delta_products.py
  4) catalog mirror + YMM + complementary  (alleen als er nieuwe producten zijn)

  python3 scripts/run_new_products_weekly.py
  python3 scripts/run_new_products_weekly.py --skip-pim --brands hsq --dry-run-import
  python3 scripts/run_new_products_weekly.py --force-pim   # negeer FTP-fingerprint
"""

from __future__ import annotations

import argparse
import json
import os
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
    ap.add_argument("--skip-ymm", action="store_true")
    ap.add_argument("--skip-complementary", action="store_true")
    ap.add_argument(
        "--force-downstream",
        action="store_true",
        help="YMM/complementary ook draaien als import 0 nieuwe producten had",
    )
    ap.add_argument("--dry-run-import", action="store_true")
    ap.add_argument("--import-limit", type=int, default=0)
    ap.add_argument("--worker-rounds", type=int, default=40, help="Max supabase_job_worker loops")
    args = ap.parse_args()

    py = sys.executable
    brands = args.brands

    if not args.skip_pim:
        pim_cmd = [py, "scripts/vps_pim_refresh_weekly.py", "--brands", brands]
        if args.force_pim:
            pim_cmd.append("--force")
        rc = _run(pim_cmd)
        if rc != 0:
            return rc
        if UNCHANGED_FLAG.is_file():
            print(
                "\nFTP-bronnen ongewijzigd — rebuild/import/YMM/complementary overgeslagen.",
                flush=True,
            )
            print("Wekelijkse pipeline klaar (noop).", flush=True)
            return 0

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
            "\nDry-run import — YMM/mirror/complementary overgeslagen "
            "(gebruik --force-downstream om toch te draaien).",
            flush=True,
        )
        print("\nWekelijkse nieuwe-producten pipeline klaar.", flush=True)
        return 0

    skip_heavy = (
        not args.force_downstream
        and created is not None
        and created == 0
    )
    if skip_heavy:
        print(
            "\nImport created=0 — YMM/mirror/complementary overgeslagen "
            "(gebruik --force-downstream om toch te draaien).",
            flush=True,
        )
        print("\nWekelijkse nieuwe-producten pipeline klaar.", flush=True)
        return 0

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
        # Alleen force-refresh als we echt nieuwe producten hebben (of force)
        cmd = [py, "scripts/shopify_sync_empfart_complementary.py", "--force-write"]
        if created is None or created > 0 or args.force_downstream:
            cmd.append("--force-refresh-cache")
        rc = _run(cmd)
        if rc != 0:
            return rc

    print("\nWekelijkse nieuwe-producten pipeline klaar.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
