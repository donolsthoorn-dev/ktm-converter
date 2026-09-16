#!/usr/bin/env python3
"""
Na Motox-import van new-only producten: YMM-app + Metafields Manager CSV's
met echte Shopify product-IDs, alleen voor de new-only set.

  # 1) producten geïmporteerd in Motox
  # 2) cache verversen (nieuwe product-IDs)
  python3 scripts/motox_ebihr_refresh_cache.py
  # 3) YMM + metafields
  python3 scripts/motox_ebihr_prepare_ymm_metafields.py

Input:
  motox/e-bihr/output/products/new_only_handles.json  (van filter-script)
  motox/e-bihr/ymm-filter-data-ebihr-*.csv
  motox/e-bihr/ymm-metafields-data-ebihr-*_chunks/

Output:
  motox/e-bihr/output/ymm/ymm_APP_import_new_only*.csv
  motox/e-bihr/output/metafields/product_metafields_new_only*.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.metafields_manager_export import (  # noqa: E402
    METAFIELDS_HEADER,
    _pipe_join_sorted,
    _upper_plain_metafield_cell,
    _ymm_summary,
    _ymm_tuples_to_fits_on_json,
)
from modules.motox_shopify_cache import (  # noqa: E402
    load_motox_indexes,
    refresh_motox_cache,
)
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402
from modules.ymm_export import (  # noqa: E402
    YMM_MAX_FILE_SIZE_BYTES,
    split_csv_max_bytes_with_header,
)

EBIHR = ROOT / "motox" / "e-bihr"
PRODUCTS_OUT = EBIHR / "output" / "products"
YMM_OUT = EBIHR / "output" / "ymm"
META_OUT = EBIHR / "output" / "metafields"


def _load_new_only_tokens(
    handles_path: Path, skus_path: Path | None
) -> tuple[set[str], set[str]]:
    handles_raw = json.loads(handles_path.read_text(encoding="utf-8"))
    handles = {
        normalize_shopify_product_handle(h) or str(h).strip()
        for h in handles_raw
        if str(h).strip()
    }
    skus: set[str] = set()
    if skus_path and skus_path.is_file():
        for s in json.loads(skus_path.read_text(encoding="utf-8")):
            t = str(s).strip().upper()
            if t:
                skus.add(t)
    return handles, skus


def _find_ymm_filter() -> Path:
    paths = sorted(EBIHR.glob("ymm-filter-data-ebihr-*.csv"))
    # exclude parts folder files if they match somehow
    paths = [p for p in paths if p.is_file()]
    if not paths:
        # parts only?
        parts = sorted((EBIHR / "ymm-filter-data-ebihr-parts").glob("*.csv"))
        if parts:
            return parts[0].parent  # signal directory
        raise SystemExit(f"Geen ymm-filter-data-ebihr-*.csv in {EBIHR}")
    # prefer single large file over parts
    full = [p for p in paths if "_part" not in p.name]
    return full[-1] if full else paths[-1]


def _iter_ymm_filter_rows(path_or_dir: Path):
    if path_or_dir.is_dir():
        files = sorted(path_or_dir.glob("*.csv"))
    else:
        files = [path_or_dir]
    for path in files:
        with path.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                yield row


def _load_metafields_fits_on() -> dict[str, str]:
    """handle/sku token -> fits_on JSON"""
    out: dict[str, str] = {}
    dirs = sorted(EBIHR.glob("ymm-metafields-data-ebihr-*_chunks"))
    files: list[Path] = []
    if dirs:
        files = sorted(dirs[-1].glob("*.csv"))
    else:
        files = sorted(EBIHR.glob("ymm-metafields-data-ebihr-*.csv"))
    for path in files:
        with path.open(newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                h = (row.get("handle") or "").strip()
                fo = (row.get("fits_on") or "").strip()
                if h and fo:
                    out[h] = fo
                    nh = normalize_shopify_product_handle(h)
                    if nh:
                        out[nh] = fo
    return out


def _json_to_tuples(raw: str) -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    raw = (raw or "").strip()
    if not raw:
        return out
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return out
    if not isinstance(data, dict):
        return out
    for make, models in data.items():
        if not isinstance(models, dict):
            continue
        for model, years in models.items():
            if isinstance(years, dict):
                years = years.keys()
            if not isinstance(years, (list, tuple, set)):
                continue
            for y in years:
                ys = str(y).strip()
                if make and model and ys:
                    out.add((str(make).strip(), str(model).strip(), ys))
    return out


def _resolve_product_id(
    token: str,
    handle_to_pid: dict[str, str],
    sku_to_pid: dict[str, str],
    products_index: dict,
) -> tuple[str, str, str] | None:
    """
    Return (product_id, handle, title) for a Bihr handle/SKU token, or None.
    """
    t = (token or "").strip()
    if not t:
        return None
    nh = normalize_shopify_product_handle(t) or t.lower()
    pid = handle_to_pid.get(nh) or handle_to_pid.get(t)
    handle = nh if pid else ""
    if not pid:
        pid = sku_to_pid.get(t) or sku_to_pid.get(t.upper())
        if pid:
            # reverse-lookup handle from products_index
            for h, meta in products_index.items():
                if str(meta.get("id") or "") == str(pid):
                    handle = h
                    break
    if not pid:
        return None
    title = ""
    if handle and handle in products_index:
        title = products_index[handle].get("title") or ""
    elif nh in products_index:
        title = products_index[nh].get("title") or ""
        handle = nh
    return str(pid), handle or nh or t, title


def main() -> int:
    ap = argparse.ArgumentParser(
        description="YMM + metafields voor Motox new-only met Shopify product-IDs."
    )
    ap.add_argument(
        "--handles-json",
        type=Path,
        default=PRODUCTS_OUT / "new_only_handles.json",
        help="JSON-lijst new-only handles (van filter-script).",
    )
    ap.add_argument(
        "--skus-json",
        type=Path,
        default=PRODUCTS_OUT / "new_only_skus.json",
        help="JSON-lijst new-only SKUs (optioneel).",
    )
    ap.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Ververs Motox-cache vóór mapping (aanbevolen na productimport).",
    )
    ap.add_argument(
        "--max-mb",
        type=float,
        default=10.0,
        help="Max MB per YMM/metafields chunk (default: 10).",
    )
    args = ap.parse_args()

    if not args.handles_json.is_file():
        raise SystemExit(
            f"Ontbreekt {args.handles_json}. Draai eerst "
            "scripts/motox_ebihr_filter_new_products.py"
        )

    if args.refresh_cache:
        refresh_motox_cache(force=True)

    idx = load_motox_indexes()
    if not idx["summary"]["present"]:
        raise SystemExit(
            "Motox-cache ontbreekt. Draai:\n"
            "  python3 scripts/motox_ebihr_refresh_cache.py"
        )

    new_handles, new_skus = _load_new_only_tokens(args.handles_json, args.skus_json)
    allowed_tokens = set(new_handles) | set(new_skus)
    # also allow raw handle strings without normalize variants
    allowed_tokens |= {h for h in new_handles}
    print(
        f"New-only: {len(new_handles)} handles, {len(new_skus)} SKUs",
        flush=True,
    )

    handle_to_pid = idx["handle_to_product_id"]
    sku_to_pid = idx["sku_to_product_id"]
    products_index = idx["products_index"]

    # Collect YMM tuples per Shopify product id
    pid_tuples: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    pid_meta: dict[str, tuple[str, str]] = {}  # pid -> (handle, title)
    unmatched_tokens: set[str] = set()

    # From vehicle-centric filter CSV
    ymm_src = _find_ymm_filter()
    print(f"YMM-filter bron: {ymm_src}", flush=True)
    filter_rows = 0
    matched_tokens = 0
    for row in _iter_ymm_filter_rows(ymm_src):
        filter_rows += 1
        make = (row.get("Make") or "").strip()
        model = (row.get("Model") or "").strip()
        year = (row.get("Year") or "").strip()
        ids_raw = (row.get("Product Ids") or row.get("Product Id") or "").strip()
        if not (make and model and year and ids_raw):
            continue
        for tok in ids_raw.split("~"):
            tok = tok.strip()
            if not tok:
                continue
            nh = normalize_shopify_product_handle(tok) or tok
            if (
                tok not in allowed_tokens
                and tok.upper() not in allowed_tokens
                and nh not in allowed_tokens
            ):
                continue
            matched_tokens += 1
            resolved = _resolve_product_id(
                tok, handle_to_pid, sku_to_pid, products_index
            )
            if not resolved:
                unmatched_tokens.add(tok)
                continue
            pid, handle, title = resolved
            pid_tuples[pid].add((make, model, year))
            pid_meta[pid] = (handle, title)

    # Enrich / fill from metafields fits_on for new-only handles
    fits_map = _load_metafields_fits_on()
    for handle in new_handles:
        fo = fits_map.get(handle) or fits_map.get(
            normalize_shopify_product_handle(handle) or ""
        )
        if not fo:
            continue
        resolved = _resolve_product_id(
            handle, handle_to_pid, sku_to_pid, products_index
        )
        if not resolved:
            unmatched_tokens.add(handle)
            continue
        pid, h, title = resolved
        pid_meta[pid] = (h, title)
        pid_tuples[pid] |= _json_to_tuples(fo)

    print(
        f"Filter-rijen gelezen: {filter_rows}; token-hits: {matched_tokens}; "
        f"producten met YMM: {len(pid_tuples)}; "
        f"niet op Motox gemapt: {len(unmatched_tokens)}",
        flush=True,
    )

    YMM_OUT.mkdir(parents=True, exist_ok=True)
    META_OUT.mkdir(parents=True, exist_ok=True)
    for old in YMM_OUT.glob("ymm_APP_import_new_only*"):
        old.unlink()
    for old in META_OUT.glob("product_metafields_new_only*"):
        old.unlink()

    # YMM app: one row per product_id + make + model + year
    ymm_path = YMM_OUT / "ymm_APP_import_new_only.csv"
    ymm_rows = 0
    with ymm_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["Product Ids", "Make", "Model", "Year"])
        for pid in sorted(pid_tuples.keys(), key=lambda x: int(x) if x.isdigit() else x):
            for make, model, year in sorted(pid_tuples[pid]):
                w.writerow([pid, make, model, year])
                ymm_rows += 1

    max_bytes = int(args.max_mb * 1024 * 1024)
    ymm_parts = split_csv_max_bytes_with_header(str(ymm_path), max_bytes=max_bytes)
    print(f"YMM-app: {ymm_rows} rijen → {len(ymm_parts)} bestand(en)", flush=True)
    for p in ymm_parts:
        print(f"  {p}", flush=True)

    # Metafields Manager full header
    meta_path = META_OUT / "product_metafields_new_only.csv"
    meta_rows = 0
    with meta_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(METAFIELDS_HEADER)
        for pid in sorted(pid_tuples.keys(), key=lambda x: int(x) if x.isdigit() else x):
            tuples = pid_tuples[pid]
            if not tuples:
                continue
            handle, title = pid_meta.get(pid, ("", ""))
            fits_on = _ymm_tuples_to_fits_on_json(tuples)
            years = {y for _, _, y in tuples}
            makes = {m for m, _, _ in tuples}
            models = {m for _, m, _ in tuples}
            row = [
                pid,
                handle,
                title,
                fits_on,
                _upper_plain_metafield_cell(_pipe_join_sorted(years)),
                _upper_plain_metafield_cell(_pipe_join_sorted(makes)),
                _upper_plain_metafield_cell(_pipe_join_sorted(models)),
                "",
                "",
                "",
                _upper_plain_metafield_cell(_ymm_summary(tuples)),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ]
            w.writerow(row)
            meta_rows += 1

    meta_parts = split_csv_max_bytes_with_header(str(meta_path), max_bytes=max_bytes)
    print(
        f"Metafields: {meta_rows} producten → {len(meta_parts)} bestand(en)",
        flush=True,
    )
    for p in meta_parts:
        print(f"  {p}", flush=True)

    unmatched_path = META_OUT / "report_unmatched_tokens.csv"
    with unmatched_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["token"])
        for t in sorted(unmatched_tokens):
            w.writerow([t])
    print(f"Unmatched tokens: {unmatched_path} ({len(unmatched_tokens)})", flush=True)

    if unmatched_tokens and not pid_tuples:
        print(
            "Geen YMM-rijen: producten staan mogelijk nog niet in Motox-cache. "
            "Importeer eerst de new-only CSV's en draai met --refresh-cache.",
            flush=True,
        )
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
