#!/usr/bin/env python3
"""
Ruim Motox e-bihr handle-duplicaten op: numerieke handles met Shopify-suffix -1/-2/-3
terwijl de basis-handle (zonder suffix) ook bestaat.

Zet standaard op DRAFT + unpublish van alle kanalen (geen hard delete).

  python3 scripts/motox_ebihr_cleanup_handle_dupes.py --dry-run
  python3 scripts/motox_ebihr_cleanup_handle_dupes.py --apply
  python3 scripts/motox_ebihr_cleanup_handle_dupes.py --apply --limit 50
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.constants import OUTPUT_ROOT  # noqa: E402
from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.motox_shopify_cache import load_motox_indexes, refresh_motox_cache  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("motox_ebihr_cleanup_dupes")

# Bihr-handles = eerste cijfers PartNumber; -1/-2/-3 = Shopify collision suffix
_HANDLE_DUPE = re.compile(r"^(\d{6,})-([1-3])$")


def _candidates(products_index: dict) -> list[dict]:
    handles = set(products_index.keys())
    out: list[dict] = []
    for handle, meta in products_index.items():
        m = _HANDLE_DUPE.match(handle or "")
        if not m:
            continue
        base = m.group(1)
        if base not in handles:
            continue
        base_meta = products_index.get(base) or {}
        status = (meta.get("status") or "").upper()
        out.append(
            {
                "handle": handle,
                "id": (meta.get("id") or "").strip(),
                "title": (meta.get("title") or "").strip(),
                "status": status,
                "has_image": bool(meta.get("has_image")),
                "base_handle": base,
                "base_id": (base_meta.get("id") or "").strip(),
                "base_title": (base_meta.get("title") or "").strip(),
                "same_title": (meta.get("title") or "").strip().lower()
                == (base_meta.get("title") or "").strip().lower(),
            }
        )
    out.sort(key=lambda r: r["handle"])
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Draft Motox numeric handle -1/-2/-3 duplicates")
    ap.add_argument("--apply", action="store_true", help="Schrijf naar Shopify")
    ap.add_argument("--dry-run", action="store_true", help="Alleen rapporteren")
    ap.add_argument("--limit", type=int, default=0, help="Max producten (0=all)")
    ap.add_argument(
        "--start-after",
        default="",
        help="Hervat na deze handle (exclusief), lexicografisch in gesorteerde kandidatenlijst",
    )
    ap.add_argument("--include-already-draft", action="store_true")
    ap.add_argument("--no-refresh-cache", action="store_true")
    ap.add_argument(
        "--require-same-title",
        action="store_true",
        help="Alleen als titel gelijk is aan basis-product",
    )
    args = ap.parse_args()

    dry_run = not args.apply or args.dry_run
    if args.apply and args.dry_run:
        dry_run = True

    if not args.no_refresh_cache:
        refresh_motox_cache(force=True)
    idx = load_motox_indexes()
    products_index = idx.get("products_index") or {}
    cands = _candidates(products_index)
    if args.require_same_title:
        cands = [c for c in cands if c["same_title"]]
    if not args.include_already_draft:
        cands = [c for c in cands if c["status"] != "DRAFT"]
    if args.start_after:
        cands = [c for c in cands if c["handle"] > args.start_after]

    if args.limit and args.limit > 0:
        cands = cands[: args.limit]

    log.info(
        "Kandidaten: %s (dry_run=%s). Voorbeeld: %s",
        len(cands),
        dry_run,
        cands[0]["handle"] if cands else "-",
    )

    admin = MotoxAdmin()
    if not dry_run:
        admin.list_publication_ids()

    report_rows: list[dict] = []
    ok_n = 0
    err_n = 0

    def _with_retries(fn, *, label: str, attempts: int = 5) -> str:
        last = ""
        for attempt in range(1, attempts + 1):
            try:
                return fn()
            except Exception as exc:  # network blips
                last = f"{type(exc).__name__}: {exc}"
                wait = min(2**attempt, 30)
                log.warning("%s attempt %s/%s failed: %s; sleep %ss", label, attempt, attempts, last[:160], wait)
                time.sleep(wait)
        return last or "retry_exhausted"

    for i, row in enumerate(cands, start=1):
        pid = row["id"]
        handle = row["handle"]
        if not pid:
            err_n += 1
            report_rows.append({**row, "ok": "0", "detail": "missing id"})
            continue

        details: list[str] = []
        serr = _with_retries(
            lambda pid=pid: admin.set_status(pid, "DRAFT", dry_run=dry_run),
            label=f"draft:{handle}",
        )
        if serr:
            details.append(f"draft:{serr}")
        uerr = _with_retries(
            lambda pid=pid: admin.unpublish_from_all_channels(pid, dry_run=dry_run),
            label=f"unpublish:{handle}",
        )
        if uerr:
            details.append(f"unpublish:{uerr}")

        if details:
            err_n += 1
            report_rows.append({**row, "ok": "0", "detail": "; ".join(details)})
            log.warning("[%s/%s] FAIL %s: %s", i, len(cands), handle, details)
        else:
            ok_n += 1
            report_rows.append({**row, "ok": "1", "detail": "draft+unpublish"})
            if i == 1 or i % 100 == 0 or i == len(cands):
                log.info("[%s/%s] ok %s → DRAFT (base %s)", i, len(cands), handle, row["base_handle"])

        if not dry_run:
            time.sleep(0.25)

    out_dir = OUTPUT_ROOT / "sync"
    out_dir.mkdir(parents=True, exist_ok=True)
    ts = int(time.time())
    report_path = out_dir / f"cleanup_handle_dupes_{ts}.csv"
    fields = [
        "ok",
        "handle",
        "id",
        "status",
        "has_image",
        "base_handle",
        "base_id",
        "same_title",
        "title",
        "detail",
    ]
    with report_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
        w.writeheader()
        w.writerows(report_rows)

    summary = {
        "dry_run": dry_run,
        "candidates": len(cands),
        "ok": ok_n,
        "errors": err_n,
        "report": str(report_path),
    }
    summary_path = out_dir / f"cleanup_handle_dupes_{ts}.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    log.info("Summary: %s", summary)
    print(json.dumps(summary, indent=2))
    return 2 if err_n and not dry_run else 0


if __name__ == "__main__":
    raise SystemExit(main())
