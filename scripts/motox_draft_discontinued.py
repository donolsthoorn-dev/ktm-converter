#!/usr/bin/env python3
"""Zet Motox-producten op draft als Bihr ze als discontinued markeert.

Alleen producten waarvan élke variant-SKU discontinued is. Een product met
nog een leverbare variant blijft actief.

Standaard een proefrun, zonder Shopify-schrijfacties:

  python3 scripts/motox_draft_discontinued.py

Echt uitzetten (draft + van de verkoopkanalen af):

  python3 scripts/motox_draft_discontinued.py --apply

De Bihr-catalogus van vandaag staat in cache/motox/bihr_probe.
"""

from __future__ import annotations

import argparse
import csv
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from modules.ebihr.discontinued import (  # noqa: E402
    discontinued_part_numbers,
    select_fully_discontinued,
)
from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.motox_shopify_cache import load_motox_indexes  # noqa: E402

REPORT = ROOT / "output" / "motox_draft_discontinued.csv"


def _catalog_dir(explicit: Path | None) -> Path:
    if explicit:
        if not explicit.is_dir():
            raise SystemExit(f"Catalogusmap niet gevonden: {explicit}")
        return explicit
    candidates = [
        path
        for path in (
            ROOT / "cache" / "motox" / "bihr_probe" / "v3_Products",
            *sorted((ROOT / "motox" / "e-bihr" / "raw").glob("*/v3_Products")),
        )
        if path.is_dir() and any(path.rglob("*.json"))
    ]
    if not candidates:
        raise SystemExit(
            "Geen Bihr-productcatalogus. Haal die eerst op, of geef --catalog."
        )
    return max(candidates, key=lambda path: max(p.stat().st_mtime for p in path.rglob("*.json")))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Schrijf naar Motox. Zonder dit vlaggetje alleen een telling.",
    )
    parser.add_argument("--catalog", type=Path, help="Map met de V3 Products JSON")
    args = parser.parse_args()

    catalog = _catalog_dir(args.catalog)
    print(f"Bihr-catalogus: {catalog}", flush=True)
    discontinued = discontinued_part_numbers(catalog)
    print(f"Discontinued bij Bihr: {len(discontinued)}", flush=True)

    indexes = load_motox_indexes()
    targets = select_fully_discontinued(
        discontinued,
        indexes["sku_to_product_id"],
        indexes["products_index"],
    )
    print(f"Actief in de shop en helemaal discontinued: {len(targets)}", flush=True)
    for item in targets[:12]:
        print(f"  {item['handle']}  {item['title']}", flush=True)
    if len(targets) > 12:
        print(f"  … en nog {len(targets) - 12}", flush=True)

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    with REPORT.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["id", "handle", "title", "skus", "result"],
            lineterminator="\n",
        )
        writer.writeheader()
        if not args.apply:
            for item in targets:
                writer.writerow(
                    {
                        "id": item["id"],
                        "handle": item["handle"],
                        "title": item["title"],
                        "skus": " ".join(item["skus"]),
                        "result": "dry-run",
                    }
                )
            print(f"Proefrun. Niets gewijzigd. Lijst: {REPORT}", flush=True)
            print("Uitzetten: python3 scripts/motox_draft_discontinued.py --apply", flush=True)
            return 0

        admin = MotoxAdmin()
        ok = 0
        failed = 0
        for index, item in enumerate(targets, start=1):
            err = admin.set_status(item["id"], "DRAFT")
            unpublish_err = ""
            if not err:
                unpublish_err = admin.unpublish_from_all_channels(item["id"])
            detail = "; ".join(part for part in (err, unpublish_err) if part)
            if detail:
                failed += 1
                result = detail
                print(f"FOUT {item['handle']}: {detail}", flush=True)
            else:
                ok += 1
                result = "draft"
            writer.writerow(
                {
                    "id": item["id"],
                    "handle": item["handle"],
                    "title": item["title"],
                    "skus": " ".join(item["skus"]),
                    "result": result,
                }
            )
            if index % 50 == 0 or index == len(targets):
                print(f"  {index}/{len(targets)}  draft={ok}  fout={failed}", flush=True)
            time.sleep(0.15)
        print(f"Klaar. Op draft: {ok}. Fouten: {failed}. Lijst: {REPORT}", flush=True)
        return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
