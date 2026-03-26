#!/usr/bin/env python3
"""
Haalt alle collecties uit de Shopify Admin API (GraphQL) en toont per collectie
de automatische voorwaarden (smart collection rules).

- **Smart collection**: `ruleSet` met regels; elke regel heeft `column` (bijv. TYPE, TAG,
  TITLE, VENDOR, …), `relation` (equals, contains, …) en `condition` (waarde).
- **Handmatige collectie**: geen `ruleSet` — producten zijn handmatig gekoppeld.

Vereist: `SHOPIFY_ACCESS_TOKEN` in `.env` (zie `.env.example`).

  python3 scripts/shopify_list_collections_rules.py
  python3 scripts/shopify_list_collections_rules.py --json > collections_rules.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402 — laadt .env via config
from modules.shopify_collections import fetch_all_collections


def _summarize_collection(c: dict) -> dict:
    rs = c.get("ruleSet")
    rules = (rs or {}).get("rules") or []
    kind = "smart" if rs and rules else "manual"
    columns = sorted({str(r.get("column", "")) for r in rules if r.get("column")})
    return {
        "id": c.get("id"),
        "title": c.get("title"),
        "handle": c.get("handle"),
        "updatedAt": c.get("updatedAt"),
        "kind": kind,
        "appliedDisjunctively": (rs or {}).get("appliedDisjunctively"),
        "rule_columns_used": columns,
        "rules": [
            {
                "column": r.get("column"),
                "relation": r.get("relation"),
                "condition": r.get("condition"),
            }
            for r in rules
        ],
    }


def main() -> int:
    p = argparse.ArgumentParser(
        description="Shopify-collecties met smart-collection regels (GraphQL)"
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Alleen JSON naar stdout (lijst met samenvatting per collectie)",
    )
    args = p.parse_args()

    token = (config.SHOPIFY_ACCESS_TOKEN or "").strip()
    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt — zie .env / .env.example", file=sys.stderr)
        return 1

    shop = config.SHOPIFY_SHOP_DOMAIN.strip()
    ver = config.SHOPIFY_ADMIN_API_VERSION.strip()

    try:
        raw = fetch_all_collections(shop, token, ver)
    except Exception as e:
        print(e, file=sys.stderr)
        return 1

    summarized = [_summarize_collection(c) for c in raw]

    if args.json:
        print(json.dumps(summarized, ensure_ascii=False, indent=2))
        return 0

    print(f"Shop: {shop}  (API {ver})\n")
    print(f"Totaal collecties: {len(summarized)}\n")

    for s in summarized:
        title = s["title"] or "(zonder titel)"
        kind_nl = (
            "Smart (automatische regels)"
            if s["kind"] == "smart"
            else "Handmatig (geen API-regels)"
        )
        print(f"— {title}")
        print(f"    handle: {s['handle']}")
        print(f"    type:   {kind_nl}")
        if s["kind"] == "smart":
            adj = s.get("appliedDisjunctively")
            mode = (
                "match ANY regel (OR)"
                if adj
                else "match ALL regels (AND)"
            )
            print(f"    logica: {mode}")
            cols = s.get("rule_columns_used") or []
            if cols:
                print(f"    regel-kolommen (types): {', '.join(cols)}")
            for i, r in enumerate(s.get("rules") or [], 1):
                col = r.get("column") or "?"
                rel = r.get("relation") or "?"
                cond = r.get("condition") or ""
                print(f"    regel {i}: [{col}] {rel} {json.dumps(cond, ensure_ascii=False)}")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
