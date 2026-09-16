#!/usr/bin/env python3
"""
Filter e-bihr valid-products CSV's tot producten die nog niet op Motox staan.

Matchregel (uitsluiten = al aanwezig):
  - Handle staat al op Motox, OF
  - minstens één Variant SKU staat al op Motox

  python3 scripts/motox_ebihr_refresh_cache.py   # eerst cache
  python3 scripts/motox_ebihr_filter_new_products.py

Output:
  motox/e-bihr/output/products/new_only_part_NNN.csv
  motox/e-bihr/output/products/new_only_handles.json
  motox/e-bihr/output/products/new_only_skus.json
  motox/e-bihr/output/products/report_new_only.csv
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.motox_shopify_cache import load_motox_indexes  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

EBIHR = ROOT / "motox" / "e-bihr"
OUT_DIR = EBIHR / "output" / "products"
# Shopify import limiet ~15 MB; marge zoals eerder
MAX_CHUNK_BYTES = 14 * 1024 * 1024


def _find_product_csvs(products_dir: Path | None) -> list[Path]:
    if products_dir is not None:
        paths = sorted(products_dir.glob("*.csv"))
        if not paths:
            raise SystemExit(f"Geen CSV's in {products_dir}")
        return paths
    chunks = sorted(EBIHR.glob("valid-products-ebihr-*_chunks"))
    if not chunks:
        # flat files
        paths = sorted(EBIHR.glob("valid-products-ebihr-*.csv"))
        if paths:
            return paths
        raise SystemExit(
            f"Geen valid-products chunks gevonden onder {EBIHR} "
            "(verwacht valid-products-ebihr-*_chunks/)."
        )
    latest = chunks[-1]
    paths = sorted(latest.glob("*.csv"))
    if not paths:
        raise SystemExit(f"Geen CSV's in {latest}")
    return paths


def _load_products_by_handle(paths: list[Path]) -> tuple[list[str], dict[str, list[list[str]]]]:
    """
    Returns (header, handle -> list of CSV rows for that product).
    Multi-row products stay grouped on Handle.
    """
    header: list[str] | None = None
    by_handle: dict[str, list[list[str]]] = {}
    handle_order: list[str] = []

    for path in paths:
        with path.open(newline="", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            file_header = next(reader, None)
            if not file_header:
                continue
            if header is None:
                header = file_header
            elif file_header != header:
                raise SystemExit(
                    f"Header-mismatch in {path.name}: verwacht zelfde kolommen als eerste deel."
                )
            try:
                handle_idx = header.index("Handle")
                sku_idx = header.index("Variant SKU")
            except ValueError as e:
                raise SystemExit(f"Kolom Handle/Variant SKU ontbreekt in {path}") from e

            current_handle = None
            current_rows: list[list[str]] = []

            def flush():
                nonlocal current_handle, current_rows
                if current_handle is None or not current_rows:
                    return
                key = normalize_shopify_product_handle(current_handle) or current_handle
                if key not in by_handle:
                    handle_order.append(key)
                    by_handle[key] = current_rows
                else:
                    by_handle[key].extend(current_rows)
                current_rows = []

            for row in reader:
                if not row:
                    continue
                h = (row[handle_idx] if len(row) > handle_idx else "").strip()
                if not h:
                    # continuation edge-case: keep with current
                    if current_rows:
                        current_rows.append(row)
                    continue
                nh = normalize_shopify_product_handle(h) or h
                if current_handle is None:
                    current_handle = nh
                    current_rows = [row]
                elif nh == current_handle:
                    current_rows.append(row)
                else:
                    flush()
                    current_handle = nh
                    current_rows = [row]
            flush()

    if header is None:
        raise SystemExit("Geen productrijen gelezen.")
    # rebuild ordered dict
    ordered = {h: by_handle[h] for h in handle_order if h in by_handle}
    return header, ordered


def _product_skus(header: list[str], rows: list[list[str]]) -> set[str]:
    sku_idx = header.index("Variant SKU")
    out: set[str] = set()
    for row in rows:
        if len(row) <= sku_idx:
            continue
        s = (row[sku_idx] or "").strip().upper()
        if s:
            out.add(s)
    return out


def _title_from_rows(header: list[str], rows: list[list[str]]) -> str:
    try:
        t_idx = header.index("Title")
    except ValueError:
        return ""
    for row in rows:
        if len(row) > t_idx and (row[t_idx] or "").strip():
            return row[t_idx].strip()
    return ""


def _cell(header: list[str], row: list[str], col: str) -> str:
    try:
        idx = header.index(col)
    except ValueError:
        return ""
    if len(row) <= idx:
        return ""
    return (row[idx] or "").strip()


def _fallback_title(header: list[str], rows: list[list[str]], handle: str) -> str:
    """Shopify eist Title op de eerste productrij; bron mist die soms."""
    vendor = ""
    ptype = ""
    for row in rows:
        if not vendor:
            vendor = _cell(header, row, "Vendor")
        if not ptype:
            ptype = _cell(header, row, "Type")
        if vendor and ptype:
            break
    parts = [p for p in (vendor, ptype) if p]
    if parts:
        return f"{' '.join(parts)} ({handle})"
    return f"Product {handle}"


def _ensure_title_on_first_row(
    header: list[str], rows: list[list[str]], handle: str
) -> tuple[list[list[str]], str, bool]:
    """
    Zorg dat de eerste CSV-rij een Title heeft (Shopify-import vereiste).
    Returns (rows, title, synthesized).
    """
    if not rows:
        return rows, "", False
    try:
        t_idx = header.index("Title")
    except ValueError:
        return rows, "", False

    title = _title_from_rows(header, rows)
    synthesized = False
    if not title:
        title = _fallback_title(header, rows, handle)
        synthesized = True

    first = list(rows[0])
    while len(first) <= t_idx:
        first.append("")
    if not (first[t_idx] or "").strip():
        first[t_idx] = title
        synthesized = synthesized or True
    return [first] + [list(r) for r in rows[1:]], title, synthesized


def _product_has_image(header: list[str], rows: list[list[str]]) -> bool:
    try:
        idx = header.index("Image Src")
    except ValueError:
        return False
    for row in rows:
        if len(row) > idx and (row[idx] or "").strip():
            return True
    return False


def _estimate_csv_bytes(header: list[str], products: list[list[list[str]]]) -> int:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(header)
    for rows in products:
        w.writerows(rows)
    return len(buf.getvalue().encode("utf-8"))


def _write_chunked(
    header: list[str],
    products: list[tuple[str, list[list[str]]]],
    out_dir: Path,
    max_bytes: int,
) -> list[Path]:
    """Write products in handle-safe chunks under max_bytes."""
    out_dir.mkdir(parents=True, exist_ok=True)
    # write one combined then split with existing helper (row-safe, not handle-safe)
    # Prefer handle-safe chunking ourselves.
    parts: list[Path] = []
    part_idx = 1
    batch: list[list[list[str]]] = []
    batch_handles: list[str] = []

    def flush_batch():
        nonlocal part_idx, batch, batch_handles
        if not batch:
            return
        path = out_dir / f"new_only_part_{part_idx:03d}.csv"
        with path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(header)
            for rows in batch:
                w.writerows(rows)
        parts.append(path)
        print(
            f"  Geschreven {path.name}: {len(batch_handles)} handles, "
            f"{path.stat().st_size / 1e6:.1f} MB",
            flush=True,
        )
        part_idx += 1
        batch = []
        batch_handles = []

    for handle, rows in products:
        candidate = batch + [rows]
        if batch and _estimate_csv_bytes(header, candidate) > max_bytes:
            flush_batch()
        batch.append(rows)
        batch_handles.append(handle)
        # single product larger than max: still write alone
        if len(batch) == 1 and _estimate_csv_bytes(header, batch) > max_bytes:
            print(
                f"  Waarschuwing: handle {handle} alleen al > {max_bytes} bytes",
                flush=True,
            )
            flush_batch()

    flush_batch()
    return parts


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Filter e-bihr producten tot Motox new-only chunks."
    )
    ap.add_argument(
        "--products-dir",
        type=Path,
        default=None,
        help="Map met valid-products CSV's (default: nieuwste valid-products-ebihr-*_chunks).",
    )
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=OUT_DIR,
        help=f"Outputmap (default: {OUT_DIR})",
    )
    ap.add_argument(
        "--max-mb",
        type=float,
        default=14.0,
        help="Max MB per chunk (default: 14).",
    )
    ap.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Eerst Motox-cache verversen via API.",
    )
    args = ap.parse_args()

    if args.refresh_cache:
        from modules.motox_shopify_cache import refresh_motox_cache

        refresh_motox_cache(force=True)

    idx = load_motox_indexes()
    if not idx["summary"]["present"]:
        raise SystemExit(
            "Motox-cache ontbreekt. Draai eerst:\n"
            "  python3 scripts/motox_ebihr_refresh_cache.py"
        )

    motox_skus: set[str] = idx["skus"]
    motox_handles: set[str] = idx["handles"]
    print(
        f"Motox-cache: {len(motox_handles)} handles, {len(motox_skus)} SKUs",
        flush=True,
    )

    paths = _find_product_csvs(args.products_dir)
    print(f"Bronbestanden ({len(paths)}):", flush=True)
    for p in paths:
        print(f"  {p}", flush=True)

    header, by_handle = _load_products_by_handle(paths)
    print(f"Unieke e-bihr handles: {len(by_handle)}", flush=True)

    new_products: list[tuple[str, list[list[str]]]] = []
    report_rows: list[dict] = []
    new_handles: list[str] = []
    new_skus: set[str] = set()
    skipped_handle = 0
    skipped_sku = 0
    skipped_no_image = 0
    titles_filled = 0

    for handle, rows in by_handle.items():
        skus = _product_skus(header, rows)
        title = _title_from_rows(header, rows)
        reason = ""
        if handle in motox_handles:
            reason = "handle_exists"
            skipped_handle += 1
        elif skus & motox_skus:
            reason = "sku_exists"
            skipped_sku += 1
        elif not _product_has_image(header, rows):
            reason = "no_image"
            skipped_no_image += 1
        if reason:
            report_rows.append(
                {
                    "handle": handle,
                    "title": title,
                    "status": "skip",
                    "reason": reason,
                    "skus": "|".join(sorted(skus)),
                }
            )
            continue
        fixed_rows, title, synthesized = _ensure_title_on_first_row(
            header, rows, handle
        )
        if synthesized:
            titles_filled += 1
        new_products.append((handle, fixed_rows))
        new_handles.append(handle)
        new_skus |= skus
        report_rows.append(
            {
                "handle": handle,
                "title": title,
                "status": "new_only",
                "reason": "title_filled" if synthesized else "",
                "skus": "|".join(sorted(skus)),
            }
        )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    # clear previous new_only parts
    for old in out_dir.glob("new_only_part_*.csv"):
        old.unlink()

    max_bytes = int(args.max_mb * 1024 * 1024)
    parts = _write_chunked(header, new_products, out_dir, max_bytes)

    handles_path = out_dir / "new_only_handles.json"
    skus_path = out_dir / "new_only_skus.json"
    handles_path.write_text(
        json.dumps(new_handles, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    skus_path.write_text(
        json.dumps(sorted(new_skus), ensure_ascii=False, indent=2), encoding="utf-8"
    )

    report_path = out_dir / "report_new_only.csv"
    with report_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(
            f,
            fieldnames=["handle", "title", "status", "reason", "skus"],
            lineterminator="\n",
        )
        w.writeheader()
        w.writerows(report_rows)

    print(
        f"\nSamenvatting:\n"
        f"  e-bihr handles totaal : {len(by_handle)}\n"
        f"  new-only              : {len(new_handles)}\n"
        f"  title aangevuld       : {titles_filled}\n"
        f"  skip handle bestaat   : {skipped_handle}\n"
        f"  skip SKU bestaat      : {skipped_sku}\n"
        f"  skip geen image      : {skipped_no_image}\n"
        f"  chunks                : {len(parts)}\n"
        f"  handles-lijst         : {handles_path}\n"
        f"  rapport               : {report_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
