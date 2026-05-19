#!/usr/bin/env python3
"""
Bouw een CSV voor **Update rows** in de YMM-app (kolom Id verplicht).

Vergelijkt:
- huidige app-export (YMM-*-update_csv.csv met Id)
- gewenste set (ymm_APP_import_ALL_part_*.csv uit export_product_ids_and_ymm.py)

Output: Id, Product Ids, Make, Model, Year — alleen regels die al in de app bestaan
(zelfde fitment-key). Geen append → geen explosie aan nieuwe rijen.

Nieuwe fitment (wel in XML, niet in app) zit niet in dit bestand; die vereisen
Append rows of bewust build_ymm_add_delete_delta.py (add + delete).

Voorbeeld:

  # 1) In YMM-app: Import/Export → export (update_csv) → input/
  # 2) Gewenste ALL staat in output/<merk>/ymm/
  python3 scripts/build_ymm_update_rows.py --brand hsq
  python3 scripts/build_ymm_update_rows.py --brand wp --only-changed

  # Alleen product-id's uit product_ids_from_xml (merk-scope):
  python3 scripts/build_ymm_update_rows.py --brand hsq \\
    --product-ids output/hsq/ids/product_ids_from_xml.csv

Upload in app: **Update rows** → output/.../ymm/ymm_update_rows.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.brand_cli import (  # noqa: E402
    add_brand_argument,
    apply_parsed_brand,
    bootstrap_brand_from_argv,
)

bootstrap_brand_from_argv()

import config  # noqa: E402
from modules.ymm_csv_delta import (  # noqa: E402
    UPDATE_ROW_FIELDS,
    desired_ymm_all_paths,
    latest_ymm_update_csv,
    norm_product_id,
    read_allowed_product_ids,
    read_app_export_with_ids,
    read_desired_ymm_rows,
    row_changed,
)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="YMM Update-rows CSV (met Id) t.o.v. app-export + gewenste ALL."
    )
    add_brand_argument(ap)
    ap.add_argument(
        "--existing",
        default=None,
        help="Pad app-export (update_csv met Id). Default: nieuwste input/.../YMM-*-update_csv.csv",
    )
    ap.add_argument(
        "--desired",
        action="append",
        metavar="PATH",
        help="Gewenste ymm_APP_import_ALL (herhaalbaar). Default: output/<merk>/ymm/ALL_part_*",
    )
    ap.add_argument(
        "--product-ids",
        default=None,
        help="Alleen gewenste rijen waarvan Product Id in dit bestand staat (bv. product_ids_from_xml.csv).",
    )
    ap.add_argument(
        "--out",
        default=None,
        help="Update CSV (default: output/<merk>/ymm/ymm_update_rows.csv).",
    )
    ap.add_argument(
        "--include-unchanged",
        action="store_true",
        help="Alle overlappende keys (ook zonder wijziging). Default: alleen gewijzigde rijen.",
    )
    args = ap.parse_args()
    apply_parsed_brand(args.brand)

    ymm_dir = Path(config.YMM_OUTPUT_DIR)
    input_dir = Path(config.INPUT_DIR)
    out_path = Path(
        args.out or str(ymm_dir / "ymm_update_rows.csv")
    )

    existing = (args.existing or "").strip() or latest_ymm_update_csv(input_dir)
    if not existing or not os.path.exists(existing):
        print(
            f"Geen YMM app-export gevonden. Exporteer eerst in de YMM-app naar {input_dir}/",
            flush=True,
        )
        return 1

    desired_paths = list(args.desired or []) or desired_ymm_all_paths(ymm_dir)
    if not desired_paths:
        print(
            f"Geen ymm_APP_import_ALL* in {ymm_dir}. "
            "Draai eerst: python3 scripts/export_product_ids_and_ymm.py --brand …",
            flush=True,
        )
        return 1

    allowed_pids: set[str] | None = None
    product_ids_path = (args.product_ids or "").strip()
    if not product_ids_path:
        default_ids = ymm_dir.parent / "ids" / "product_ids_from_xml.csv"
        if default_ids.is_file():
            product_ids_path = str(default_ids)
    if product_ids_path and os.path.isfile(product_ids_path):
        allowed_pids = read_allowed_product_ids(product_ids_path)
        print(f"Filter op {len(allowed_pids)} product-id's uit {product_ids_path}", flush=True)

    app_by_key, app_rows = read_app_export_with_ids(existing)
    desired_by_key, desired_rows = read_desired_ymm_rows(desired_paths)

    if allowed_pids is not None:
        desired_by_key = {
            k: v
            for k, v in desired_by_key.items()
            if norm_product_id(v.get("Product Ids", "")) in allowed_pids
        }

    app_keys = set(app_by_key)
    desired_keys = set(desired_by_key)
    overlap = app_keys & desired_keys
    only_desired = desired_keys - app_keys
    only_app = app_keys - desired_keys

    only_changed = not args.include_unchanged
    update_rows: list[dict] = []
    unchanged = 0
    for k in sorted(overlap):
        app_row = app_by_key[k]
        des_row = desired_by_key[k]
        if only_changed and not row_changed(app_row, des_row):
            unchanged += 1
            continue
        update_rows.append(
            {
                "Id": app_row["Id"],
                "Product Ids": des_row["Product Ids"],
                "Make": des_row["Make"],
                "Model": des_row["Model"],
                "Year": des_row["Year"],
            }
        )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(UPDATE_ROW_FIELDS))
        w.writeheader()
        w.writerows(update_rows)

    print(f"Merk: {config.BRAND_ID}", flush=True)
    print(f"App-export: {existing}", flush=True)
    print(f"Gewenste bron(nen): {len(desired_paths)} bestand(en)", flush=True)
    for p in desired_paths[:5]:
        print(f"  - {p}", flush=True)
    if len(desired_paths) > 5:
        print(f"  … en {len(desired_paths) - 5} meer", flush=True)
    print(f"App-regels: {app_rows}, unieke keys: {len(app_keys)}", flush=True)
    print(f"Gewenste regels: {desired_rows}, unieke keys: {len(desired_keys)}", flush=True)
    print(f"Overlap (update-kandidaat): {len(overlap)}", flush=True)
    if only_changed:
        print(f"  waarvan ongewijzigd (overgeslagen): {unchanged}", flush=True)
    print(f"→ Update rows CSV: {len(update_rows)} regels → {out_path}", flush=True)
    print(
        f"Alleen in gewenste (geen Id in app → geen Update): {len(only_desired)}",
        flush=True,
    )
    print(
        f"Alleen in app (niet in gewenste → overweeg Delete): {len(only_app)}",
        flush=True,
    )
    if only_desired:
        print(
            "Tip: nieuwe fitment vereist Append rows of ymm_delta_add_rows.csv "
            "(build_ymm_add_delete_delta.py).",
            flush=True,
        )
    if only_app:
        print(
            "Tip: verwijderen via ymm_delta_delete_ids.csv "
            "(build_ymm_add_delete_delta.py).",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
