#!/usr/bin/env python3
"""
Ontdubbel YMM update-export (Shopify app CSV) op:
  (Product Ids, Make, Model, Year)

Input verwacht kolommen:
  Id, Product Ids, Make, Model, Year

Output:
  1) Schone update-CSV met unieke combinaties
  2) CSV met duplicate Id's (voor delete in de app)
  3) Optioneel: volledige duplicate rijen voor audit

Voorbeeld:
  python3 scripts/dedupe_ymm_update_csv.py \
    --input input/YMM-ktm-shop-nl.myshopify.com_1776690428-update_csv.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _default_input_path() -> str:
    candidates = sorted((PROJECT_ROOT / "input").glob("YMM-*-update_csv.csv"))
    if not candidates:
        return ""
    return str(candidates[-1])


def _norm_text(v: str) -> str:
    return re.sub(r"\s+", " ", (v or "").strip()).lower()


def _norm_year(v: str) -> str:
    return re.sub(r"\s+", "", (v or "").strip())


def _norm_product_id(v: str) -> str:
    return (v or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Ontdubbel YMM update-CSV op Product Ids + Make + Model + Year."
    )
    parser.add_argument(
        "--input",
        default=_default_input_path(),
        help="Pad naar YMM update-CSV (default: nieuwste input/YMM-*-update_csv.csv).",
    )
    parser.add_argument(
        "--output-clean",
        default="output/ymm/ymm_existing_set_dedup_update.csv",
        help="Uitvoer: unieke update-CSV.",
    )
    parser.add_argument(
        "--output-delete-ids",
        default="output/ymm/ymm_existing_set_duplicate_ids.csv",
        help="Uitvoer: duplicate Id-lijst (kolom 'Id').",
    )
    parser.add_argument(
        "--output-duplicate-rows",
        default="output/ymm/ymm_existing_set_duplicate_rows.csv",
        help="Uitvoer: volledige duplicate rijen (audit).",
    )
    parser.add_argument(
        "--drop-empty-product-ids",
        action="store_true",
        help="Rijen zonder Product Ids overslaan (en Id opnemen in delete-lijst).",
    )
    args = parser.parse_args()

    in_path = Path(args.input)
    if not in_path.exists():
        print(f"Input niet gevonden: {in_path}")
        return 1

    out_clean = Path(args.output_clean)
    out_delete = Path(args.output_delete_ids)
    out_dups = Path(args.output_duplicate_rows)
    out_clean.parent.mkdir(parents=True, exist_ok=True)
    out_delete.parent.mkdir(parents=True, exist_ok=True)
    out_dups.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open(newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        fieldnames = reader.fieldnames or []
        required = {"Id", "Product Ids", "Make", "Model", "Year"}
        missing = sorted(required - set(fieldnames))
        if missing:
            print(f"Input mist verplichte kolommen: {', '.join(missing)}")
            return 1

        seen_keys: dict[tuple[str, str, str, str], dict] = {}
        keep_rows: list[dict] = []
        duplicate_rows: list[dict] = []
        duplicate_ids: list[str] = []

        total_rows = 0
        dropped_empty_pid = 0
        duplicate_count = 0

        for row in reader:
            total_rows += 1
            pid = _norm_product_id(row.get("Product Ids") or "")
            make = _norm_text(row.get("Make") or "")
            model = _norm_text(row.get("Model") or "")
            year = _norm_year(row.get("Year") or "")
            row_id = (row.get("Id") or "").strip()

            if args.drop_empty_product_ids and not pid:
                dropped_empty_pid += 1
                if row_id:
                    duplicate_ids.append(row_id)
                duplicate_rows.append(row)
                continue

            key = (pid, make, model, year)
            if key not in seen_keys:
                seen_keys[key] = row
                keep_rows.append(row)
                continue

            duplicate_count += 1
            duplicate_rows.append(row)
            if row_id:
                duplicate_ids.append(row_id)

    # Unieke update-set
    with out_clean.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(keep_rows)

    # Alleen Id-kolom (geschikt voor delete-acties in apps die op Id werken)
    with out_delete.open("w", newline="", encoding="utf-8") as f_del:
        writer = csv.writer(f_del)
        writer.writerow(["Id"])
        for rid in duplicate_ids:
            writer.writerow([rid])

    # Volledige duplicate rijen (audit / controle)
    with out_dups.open("w", newline="", encoding="utf-8") as f_dup:
        writer = csv.DictWriter(f_dup, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(duplicate_rows)

    print(f"Input: {in_path}")
    print(f"Totaal data-rijen: {total_rows}")
    print(f"Unieke update-rijen: {len(keep_rows)}")
    print(f"Duplicates (zelfde Product Ids/Make/Model/Year): {duplicate_count}")
    print(f"Rijen zonder Product Ids gedropt: {dropped_empty_pid}")
    print(f"Delete Id's: {len(duplicate_ids)}")
    print(f"Schone update-CSV: {out_clean}")
    print(f"Delete Id-CSV: {out_delete}")
    print(f"Duplicate rows (audit): {out_dups}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
