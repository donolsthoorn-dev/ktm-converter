#!/usr/bin/env python3
"""
Build delta-only YMM + Metafields imports based on the latest Shopify delta export.

Flow:
1) Read latest output/products/shopify_export_delta_*.csv
2) Collect unique Handles from that delta
3) Map handles -> Product Id via output/ids/product_ids_from_xml.csv
4) Filter:
   - YMM (output/ymm/ymm_APP_import_ALL*.csv) by Product Ids
   - Metafields (output/metafields/product_metafields_metafields_manager.csv) by handle
5) Write compact delta files under output/ymm/ and output/metafields/
"""

from __future__ import annotations

import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402
from modules.delta_handles import load_handles_from_shopify_export_csv  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

PRODUCTS_DIR = Path(config.PRODUCTS_OUTPUT_DIR)
IDS_DIR = Path(config.IDS_OUTPUT_DIR)
YMM_DIR = Path(config.YMM_OUTPUT_DIR)
METAFIELDS_DIR = Path(config.METAFIELDS_OUTPUT_DIR)


def _latest(path_glob: str) -> str:
    files = glob.glob(path_glob)
    if not files:
        return ""
    files.sort(key=lambda p: os.path.getmtime(p))
    return files[-1]


def _read_handle_to_product_id(path: str) -> dict[str, str]:
    out: dict[str, str] = {}
    with open(path, newline="", encoding="utf-8-sig") as f:
        r = csv.DictReader(f)
        for row in r:
            h = normalize_shopify_product_handle(row.get("Product SKU") or "")
            pid = (row.get("Product Id") or "").replace("~", "").strip()
            if h and pid:
                out[h] = pid
    return out


def _write_filtered_metafields(src: str, dst: str, handles: set[str]) -> tuple[int, int]:
    total = 0
    kept = 0
    with (
        open(src, newline="", encoding="utf-8-sig") as f_in,
        open(dst, "w", newline="", encoding="utf-8") as f_out,
    ):
        r = csv.DictReader(f_in, delimiter=",")
        if not r.fieldnames:
            return 0, 0
        w = csv.DictWriter(
            f_out,
            fieldnames=r.fieldnames,
            delimiter=",",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )
        w.writeheader()
        for row in r:
            total += 1
            h = normalize_shopify_product_handle(row.get("handle") or "")
            if h in handles:
                row_out = dict(row)
                row_out["handle"] = h
                w.writerow(row_out)
                kept += 1
    return total, kept


def _iter_ymm_sources() -> list[str]:
    """
    Prefer split files if present; otherwise use single ALL file.
    """
    part_files = sorted(glob.glob(str(YMM_DIR / "ymm_APP_import_ALL_part_*.csv")))
    if part_files:
        return part_files
    single = YMM_DIR / "ymm_APP_import_ALL.csv"
    return [str(single)] if single.exists() else []


def _write_filtered_ymm(src_files: list[str], dst: str, product_ids: set[str]) -> tuple[int, int]:
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
    delta_csv = _latest(str(PRODUCTS_DIR / "shopify_export_delta_*.csv"))
    if not delta_csv:
        print("Geen delta CSV gevonden in output/products.")
        return 1

    product_ids_csv = str(IDS_DIR / "product_ids_from_xml.csv")
    metafields_csv = str(METAFIELDS_DIR / "product_metafields_metafields_manager.csv")
    ymm_sources = _iter_ymm_sources()

    missing = [p for p in [product_ids_csv, metafields_csv] if not os.path.exists(p)]
    if missing:
        print("Ontbrekende bronbestanden:", ", ".join(missing))
        print("Run eerst export scripts opnieuw na Shopify import.")
        return 1
    if not ymm_sources:
        print("Geen YMM bronbestand gevonden (ymm_APP_import_ALL.csv of part-files).")
        return 1

    delta_handles = load_handles_from_shopify_export_csv(delta_csv)
    handle_to_pid = _read_handle_to_product_id(product_ids_csv)
    delta_product_ids = {handle_to_pid[h] for h in delta_handles if h in handle_to_pid}

    METAFIELDS_DIR.mkdir(parents=True, exist_ok=True)
    YMM_DIR.mkdir(parents=True, exist_ok=True)

    out_meta = str(METAFIELDS_DIR / "product_metafields_delta_latest.csv")
    out_ymm = str(YMM_DIR / "ymm_APP_import_delta_latest.csv")

    _m_total, m_kept = _write_filtered_metafields(metafields_csv, out_meta, delta_handles)
    _y_total, y_kept = _write_filtered_ymm(ymm_sources, out_ymm, delta_product_ids)

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
