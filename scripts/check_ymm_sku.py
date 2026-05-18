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

from modules.brand_cli import (  # noqa: E402
    add_brand_argument,
    apply_parsed_brand,
    bootstrap_brand_from_argv,
)

bootstrap_brand_from_argv()

from config import XML_FILE  # noqa: E402
from modules.ymm_export import (  # noqa: E402
    build_merged_sku_to_ymm,
    collect_sku_to_ymm_from_structure,
    stream_xml_for_export,
)


def _lookup_sku_map(m: dict[str, set], sku: str) -> set:
    """XML gebruikt meestal hoofdletters voor artikelnummers; CLI vaak lowercase."""
    s = (sku or "").strip()
    if not s:
        return set()
    for k in (s, s.upper(), s.lower()):
        if k in m:
            return m[k]
    return set()


def main():
    p = argparse.ArgumentParser(description="YMM-tuple count voor één variant-SKU (XML).")
    add_brand_argument(p)
    p.add_argument("sku", help="Bijv. 00010000318")
    p.add_argument(
        "--xml",
        default=None,
        help="XML-pad (default: merk-XML uit config).",
    )
    args = p.parse_args()
    apply_parsed_brand(args.brand)

    sku = args.sku.strip()
    xml_path = args.xml or XML_FILE
    print(f"Merk-XML: {xml_path}", flush=True)
    print("Structuur-pass…", flush=True)
    structure_index, relations = stream_xml_for_export()
    st = collect_sku_to_ymm_from_structure(structure_index, relations)
    st_set = _lookup_sku_map(st, sku)
    print(f"  Alleen Bikes MODELL: {len(st_set)} tuples", flush=True)
    print("ZBH2BIKE-merge…", flush=True)
    merged = build_merged_sku_to_ymm(structure_index, relations, xml_path)
    m_set = _lookup_sku_map(merged, sku)
    print(f"  Na merge: {len(m_set)} tuples", flush=True)
    if m_set:
        for t in sorted(m_set)[:20]:
            print(f"    {t}")
        if len(m_set) > 20:
            print(f"    … en {len(m_set) - 20} meer")


if __name__ == "__main__":
    main()
