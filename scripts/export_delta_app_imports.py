#!/usr/bin/env python3
"""
Build delta-only YMM + Metafields imports based on the latest Shopify delta export.

Flow:
1) Read latest output/<merk>/products/shopify_export_delta_*.csv (or --delta-handles-csv)
2) Collect unique Handles from that delta
3) Map handles -> Product Id via output/<merk>/ids/product_ids_from_xml.csv
4) Filter:
   - YMM (output/<merk>/ymm/ymm_APP_import_ALL*.csv) by Product Ids
   - Metafields (output/<merk>/metafields/product_metafields_metafields_manager.csv) by handle
5) Write compact delta files under output/<merk>/ymm/ and …/metafields/
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.brand_cli import (  # noqa: E402
    add_brand_argument,
    apply_parsed_brand,
    bootstrap_brand_from_argv,
)

bootstrap_brand_from_argv()

import config  # noqa: E402
from modules.delta_handles import load_handles_from_shopify_export_csv  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402


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


def _iter_ymm_sources(ymm_dir: Path) -> list[str]:
    """Prefer split files if present; otherwise use single ALL file."""
    part_files = sorted(glob.glob(str(ymm_dir / "ymm_APP_import_ALL_part_*.csv")))
    if part_files:
        return part_files
    single = ymm_dir / "ymm_APP_import_ALL.csv"
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
    ap = argparse.ArgumentParser(
        description="Filter YMM + metafields naar delta op basis van product-export-delta.",
    )
    add_brand_argument(ap)
    ap.add_argument(
        "--delta-handles-csv",
        metavar="PATH",
        default=None,
        help=(
            "Product-delta CSV (kolom Handle). "
            f"Default: nieuwste {config.PRODUCTS_OUTPUT_DIR}/shopify_export_delta_*.csv"
        ),
    )
    args = ap.parse_args()
    apply_parsed_brand(args.brand)

    products_dir = Path(config.PRODUCTS_OUTPUT_DIR)
    ids_dir = Path(config.IDS_OUTPUT_DIR)
    ymm_dir = Path(config.YMM_OUTPUT_DIR)
    metafields_dir = Path(config.METAFIELDS_OUTPUT_DIR)

    delta_csv = (args.delta_handles_csv or "").strip()
    if not delta_csv:
        delta_csv = _latest(str(products_dir / "shopify_export_delta_*.csv"))
    if not delta_csv:
        print(f"Geen delta CSV gevonden in {products_dir}.")
        return 1

    product_ids_csv = str(ids_dir / "product_ids_from_xml.csv")
    metafields_csv = str(metafields_dir / "product_metafields_metafields_manager.csv")
    ymm_sources = _iter_ymm_sources(ymm_dir)

    missing = [p for p in [product_ids_csv, metafields_csv] if not os.path.exists(p)]
    if missing:
        print("Ontbrekende bronbestanden:", ", ".join(missing))
        print(
            f"Run eerst: python3 scripts/export_product_ids_and_ymm.py --brand {config.BRAND_ID}"
        )
        return 1
    if not ymm_sources:
        print(
            f"Geen YMM bronbestand in {ymm_dir} "
            "(ymm_APP_import_ALL.csv of part-files)."
        )
        return 1

    delta_handles = load_handles_from_shopify_export_csv(delta_csv)
    handle_to_pid = _read_handle_to_product_id(product_ids_csv)
    delta_product_ids = {handle_to_pid[h] for h in delta_handles if h in handle_to_pid}

    metafields_dir.mkdir(parents=True, exist_ok=True)
    ymm_dir.mkdir(parents=True, exist_ok=True)

    out_meta = str(metafields_dir / "product_metafields_delta_latest.csv")
    out_ymm = str(ymm_dir / "ymm_APP_import_delta_latest.csv")

    _m_total, m_kept = _write_filtered_metafields(metafields_csv, out_meta, delta_handles)
    _y_total, y_kept = _write_filtered_ymm(ymm_sources, out_ymm, delta_product_ids)

    unresolved_handles = sorted(h for h in delta_handles if h not in handle_to_pid)

    print(f"Merk: {config.BRAND_ID}")
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
