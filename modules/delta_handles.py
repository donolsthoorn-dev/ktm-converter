"""Laden van delta-producthandles voor gefilterde YMM/metafields-exports."""

from __future__ import annotations

import csv

from modules.xml_loader import normalize_shopify_product_handle


def load_handles_from_shopify_export_csv(path: str) -> set[str]:
    """
    Unieke waarden uit kolom 'Handle' (Shopify product CSV / delta-export).
    Genormaliseerd naar lowercase (zelfde als Shopify URL-handle).
    """
    out: set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            h = normalize_shopify_product_handle(row.get("Handle") or "")
            if h:
                out.add(h)
    return out


def load_handles_from_text_file(path: str) -> set[str]:
    """
    Eén handle per regel; lege regels en regels die met # beginnen worden overgeslagen.
    Handles worden genormaliseerd naar lowercase.
    """
    out: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            h = normalize_shopify_product_handle(line)
            if h:
                out.add(h)
    return out
