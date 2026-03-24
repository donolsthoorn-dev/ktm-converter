#!/usr/bin/env python3
"""
Build delta-only YMM + Metafields imports based on the latest Shopify delta export.

Flow:
1) Read latest output/shopify/shopify_export_delta_*.csv
2) Collect unique Handles from that delta
3) Map handles -> Product Id via output/reports/product_ids_from_xml.csv
4) Filter:
   - YMM (output/reports/ymm_APP_import_ALL*.csv) by Product Ids
   - Metafields (output/reports/product_metafields_metafields_manager.csv) by handle
5) Write compact delta files in output/reports/
"""

from __future__ import annotations

import csv
import glob
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)

REPORTS_DIR = Path("output/reports")
SHOPIFY_DIR = Path("output/shopify")


def _latest(path_glob: str) -> str:
    files = glob.glob(path_glob)
    if not files:
        return ""
    files.sort(key=lambda p: os.path.getmtime(p))
    return files[-1]


def _read_delta_handles(delta_csv: str) -> set[str]:
    out: set[str] = set()
    with open(delta_csv, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            h = (row.get("Handle") or "").strip()
            if h:
                out.add(h)
    return out


def _read_handle_to_product_id(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            h = (row.get("Product SKU") or "").strip()
            pid = (row.get("Product Id") or "").replace("~", "").strip()
            if h and pid:
                out[h] = pid
    return out


def _write_filtered_metafields(
    src: str, dst: str, handles: set[str]
) -> tuple[int, int]:
    total = 0
    kept = 0
    with open(src, newline="", encoding="utf-8-sig") as f_in, open(
        dst, "w", newline="", encoding="utf-8"
    ) as f_out:
        r = csv.DictReader(f_in)
        if not r.fieldnames:
            return 0, 0
        w = csv.DictWriter(f_out, fieldnames=r.fieldnames)
        w.writeheader()
        for row in r:
            total += 1
            h = (row.get("handle") or "").strip()
            if h in handles:
                w.writerow(row)
                kept += 1
    return total, kept


def _iter_ymm_sources() -> list[str]:
    """
    Prefer split files if present; otherwise use single ALL file.
    """
    part_files = sorted(glob.glob(str(REPORTS_DIR / "ymm_APP_import_ALL_part_*.csv")))
    if part_files:
        return part_files
    single = REPORTS_DIR / "ymm_APP_import_ALL.csv"
    return [str(single)] if single.exists() else []


def _write_filtered_ymm(
    src_files: list[str], dst: str, product_ids: set[str]
) -> tuple[int, int]:
    total = 0
    kept = 0
    header = None
    with open(dst, "w", newline="", encoding="utf-8") as f_out:
        w = None
        for src in src_files:
            with open(src, newline="", encoding="utf-8-sig") as f_in:
                r = csv.DictReader(f_in)
                if not r.fieldnames:
                    continue
                if header is None:
                    header = r.fieldnames
                    w = csv.DictWriter(f_out, fieldnames=header)
                    w.writeheader()
                for row in r:
                    total += 1
                    pid = (row.get("Product Ids") or "").strip()
                    if pid and pid in product_ids:
                        w.writerow(row)
                        kept += 1
    return total, kept


def main() -> int:
    delta_csv = _latest(str(SHOPIFY_DIR / "shopify_export_delta_*.csv"))
    if not delta_csv:
        print("Geen delta CSV gevonden in output/shopify.")
        return 1

    product_ids_csv = str(REPORTS_DIR / "product_ids_from_xml.csv")
    metafields_csv = str(REPORTS_DIR / "product_metafields_metafields_manager.csv")
    ymm_sources = _iter_ymm_sources()

    missing = [
        p
        for p in [product_ids_csv, metafields_csv]
        if not os.path.exists(p)
    ]
    if missing:
        print("Ontbrekende bronbestanden:", ", ".join(missing))
        print("Run eerst export scripts opnieuw na Shopify import.")
        return 1
    if not ymm_sources:
        print("Geen YMM bronbestand gevonden (ymm_APP_import_ALL.csv of part-files).")
        return 1

    delta_handles = _read_delta_handles(delta_csv)
    handle_to_pid = _read_handle_to_product_id(product_ids_csv)
    delta_product_ids = {handle_to_pid[h] for h in delta_handles if h in handle_to_pid}

    out_meta = str(REPORTS_DIR / "product_metafields_delta_latest.csv")
    out_ymm = str(REPORTS_DIR / "ymm_APP_import_delta_latest.csv")

    _m_total, m_kept = _write_filtered_metafields(
        metafields_csv, out_meta, delta_handles
    )
    _y_total, y_kept = _write_filtered_ymm(
        ymm_sources, out_ymm, delta_product_ids
    )

    unresolved_handles = sorted(h for h in delta_handles if h not in handle_to_pid)

    print(f"Delta bron: {delta_csv}")
    print(f"Unieke delta handles: {len(delta_handles)}")
    print(f"Mapped delta Product IDs: {len(delta_product_ids)}")
    print(f"Metafields delta: {out_meta} ({m_kept} rows)")
    print(f"YMM delta: {out_ymm} ({y_kept} rows)")
    if unresolved_handles:
        print(
            f"Waarschuwing: {len(unresolved_handles)} handles zonder Product Id mapping "
            "(nog niet in product_ids_from_xml?):"
        )
        print(", ".join(unresolved_handles[:20]))
        if len(unresolved_handles) > 20:
            print("...")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

