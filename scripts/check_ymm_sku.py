#!/usr/bin/env python3
"""Snel controleren hoeveel YMM-tuples de merge (MODELL + ZBH2BIKE) voor één SKU geeft."""

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import XML_FILE  # noqa: E402
from modules.ymm_export import (  # noqa: E402
    build_merged_sku_to_ymm,
    collect_sku_to_ymm_from_structure,
    stream_xml_for_export,
)


def main():
    p = argparse.ArgumentParser(description="YMM-tuple count voor één variant-SKU (XML).")
    p.add_argument("sku", help="Bijv. 00010000318")
    p.add_argument(
        "--xml",
        default=None,
        help=f"KTM XML (default: {XML_FILE})",
    )
    args = p.parse_args()
    sku = args.sku.strip()
    xml_path = args.xml or XML_FILE
    print("Structuur-pass…", flush=True)
    structure_index, relations = stream_xml_for_export()
    st = collect_sku_to_ymm_from_structure(structure_index, relations)
    print(f"  Alleen Bikes MODELL: {len(st.get(sku, set()))} tuples", flush=True)
    print("ZBH2BIKE-merge…", flush=True)
    merged = build_merged_sku_to_ymm(structure_index, relations, xml_path)
    t = merged.get(sku, set())
    print(f"  Totaal na merge: {len(t)} tuples", flush=True)
    for row in sorted(t)[:40]:
        print("   ", row)
    if len(t) > 40:
        print(f"   … en {len(t) - 40} meer", flush=True)


if __name__ == "__main__":
    main()
