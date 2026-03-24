"""Laden van delta-producthandles voor gefilterde YMM/metafields-exports."""

from __future__ import annotations

import csv


def load_handles_from_shopify_export_csv(path: str) -> set[str]:
    """
    Unieke waarden uit kolom 'Handle' (Shopify product CSV / delta-export).
    """
    out: set[str] = set()
    with open(path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            h = (row.get("Handle") or "").strip()
            if h:
                out.add(h)
    return out


def load_handles_from_text_file(path: str) -> set[str]:
    """
    Eén handle per regel; lege regels en regels die met # beginnen worden overgeslagen.
    """
    out: set[str] = set()
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            out.add(line)
    return out

