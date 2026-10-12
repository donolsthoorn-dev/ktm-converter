#!/usr/bin/env python3
"""Koppel ongemapte Bihr-types als TYPE EQUALS aan Motox-collecties.

Stap 1 is altijd collectie Banden (MX/enduro/binnenband-whitelist). Daarna
de overige voorstellen uit output/motox_unmapped_types_proposals.json.

  python3 scripts/motox_link_bihr_types.py
  python3 scripts/motox_link_bihr_types.py --yes --banden-only
  python3 scripts/motox_link_bihr_types.py --yes
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.motox_shopify_cache import _gql, _session, load_motox_env  # noqa: E402
from modules.shopify_collections import (  # noqa: E402
    COLLECTION_UPDATE,
    MAX_SMART_RULES,
    fetch_all_collections,
    merge_type_equals,
)

PROPOSALS_PATH = PROJECT_ROOT / "output" / "motox_unmapped_types_proposals.json"
OUT_PATH = PROJECT_ROOT / "output" / "motox_link_bihr_types.json"

# Alleen deze bandentypes — rest van de Bihr-bandenboom niet.
BANDEN_TYPES = [
    "Cross / MX / SX",
    "Binnenbanden",
    "Enduro / Rallye-Raid",
    "Cross / MX / SX > Off-Road amateur",
    "Cross / MX / SX > > Strand Races",
    "Off-Road amateur",
    "Cross / MX / SX > Banden selectie",
    "Cross / MX / SX > Pakketdeals Banden",
    "Cross / MX / SX > Opruiming Banden",
    "Cross / MX / SX > Schroeven en moeren > > Strand Races",
    "Cross / MX / SX > Banden selectie > > Strand Races",
    "Cross / MX / SX > Banden selectie > Off-Road amateur",
]

BANDEN_COLLECTION = "Banden"

PROPOSAL_TO_COLLECTION = {
    "Banden (nieuw)": BANDEN_COLLECTION,
    "Plastics": "Plastic delen",
}

SKIP_PROPOSALS = {
    "eerst Type opschonen",
    "geen vaste productcollectie",
    "handmatig beoordelen",
    "niet Motox (KTM/HSQ/WP)",
}

SKIP_CONFIDENCE = {"laag"}


def _by_title(collections: list[dict]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for c in collections:
        title = (c.get("title") or "").strip()
        if title and title not in out:
            out[title] = c
    return out


def _rule_columns(col: dict) -> set[str]:
    rules = ((col.get("ruleSet") or {}).get("rules")) or []
    return {str(r.get("column") or "") for r in rules}


def _existing_type_equals(col: dict) -> set[str]:
    rules = ((col.get("ruleSet") or {}).get("rules")) or []
    return {
        str(r.get("condition") or "")
        for r in rules
        if r.get("column") == "TYPE" and (r.get("relation") or "").upper() == "EQUALS"
    }


def _useful_segs(type_name: str) -> list[str]:
    skip = {"", ".", "Strand Races"}
    return [s.strip() for s in type_name.split(" > ") if s.strip() not in skip]


def resolve_collection(type_name: str, proposal: str, titles: dict[str, dict]) -> str:
    """Als een padsegment een echte collectienaam is, gebruik die i.p.v. een
    collectie die toevallig hetzelfde TYPE EQUALS heeft (bijv. Motorblokgereedschap
    met condition Distributie)."""
    fold_titles = {t.casefold(): t for t in titles}
    for seg in reversed(_useful_segs(type_name)):
        hit = fold_titles.get(seg.casefold())
        if hit:
            return hit
    return proposal


def planned_groups(
    banden_only: bool,
    titles: dict[str, dict] | None = None,
) -> list[tuple[str, list[str], str]]:
    """[(collection_title, types, source)] — Banden altijd eerst."""
    groups: list[tuple[str, list[str], str]] = [
        (BANDEN_COLLECTION, list(BANDEN_TYPES), "banden-whitelist")
    ]
    if banden_only:
        return groups

    if not PROPOSALS_PATH.is_file():
        raise SystemExit(f"Voorstellen ontbreken: {PROPOSALS_PATH}")
    payload = json.loads(PROPOSALS_PATH.read_text(encoding="utf-8"))
    by_coll: dict[str, list[tuple[int, str]]] = defaultdict(list)
    titles = titles or {}
    for row in payload.get("rows") or []:
        raw = (row.get("proposal") or "").strip()
        if raw in SKIP_PROPOSALS:
            continue
        if (row.get("confidence") or "") in SKIP_CONFIDENCE:
            continue
        t = (row.get("type") or "").strip()
        if not t or t in BANDEN_TYPES:
            continue
        title = PROPOSAL_TO_COLLECTION.get(raw, raw)
        title = resolve_collection(t, title, titles)
        if title == BANDEN_COLLECTION:
            continue
        n = int(row.get("bihr") or 0) or int(row.get("motox") or 0)
        by_coll[title].append((-n, t))

    for title, items in sorted(by_coll.items(), key=lambda x: min(v[0] for v in x[1])):
        types = [t for _, t in sorted(set(items))]
        groups.append((title, types, "voorstel"))
    return groups


def apply_group(
    sess,
    url: str,
    token: str,
    col: dict,
    types: list[str],
    *,
    yes: bool,
) -> dict:
    existing = ((col.get("ruleSet") or {}).get("rules")) or []
    already = _existing_type_equals(col)
    wanted = [t for t in types if t not in already]
    if not wanted:
        return {
            "title": col.get("title"),
            "id": col.get("id"),
            "status": "noop",
            "added": [],
            "already": [t for t in types if t in already],
            "skipped_limit": [],
        }

    disj = True
    rs = col.get("ruleSet")
    if rs and existing:
        disj = bool(rs.get("appliedDisjunctively"))
        if not disj:
            # Enkele TYPE-regel AND → OR, anders extra EQUALS nooit matcht.
            cols = {str(r.get("column") or "") for r in existing}
            if cols <= {"TYPE"}:
                disj = True
            else:
                return {
                    "title": col.get("title"),
                    "id": col.get("id"),
                    "status": "skipped-and-mixed",
                    "added": [],
                    "wanted": wanted,
                    "skipped_limit": [],
                }

    merged, added, skipped = merge_type_equals(existing, wanted)
    result = {
        "title": col.get("title"),
        "id": col.get("id"),
        "status": "dry-run" if not yes else "applied",
        "before": len(existing),
        "after": len(merged),
        "disjunctive": disj,
        "added": added,
        "already": [t for t in types if t in already],
        "skipped_limit": skipped,
    }
    if not yes or not added:
        if not added:
            result["status"] = "noop"
        return result

    body = _gql(
        sess,
        url,
        token,
        COLLECTION_UPDATE,
        {
            "input": {
                "id": col["id"],
                "ruleSet": {
                    "appliedDisjunctively": disj,
                    "rules": merged,
                },
            }
        },
    )
    payload = ((body.get("data") or {}).get("collectionUpdate")) or {}
    errs = payload.get("userErrors") or []
    if body.get("errors"):
        result["status"] = "error"
        result["errors"] = body.get("errors")
        return result
    if errs:
        result["status"] = "error"
        result["errors"] = errs
        return result
    collection = payload.get("collection") or {}
    result["productsCount"] = ((collection.get("productsCount") or {}).get("count"))
    result["rulesSaved"] = len(((collection.get("ruleSet") or {}).get("rules")) or [])
    return result


def main() -> int:
    p = argparse.ArgumentParser(description="Bihr-types als TYPE EQUALS aan Motox-collecties")
    p.add_argument("--yes", action="store_true", help="Schrijf naar Shopify (anders dry-run)")
    p.add_argument(
        "--banden-only",
        action="store_true",
        help="Alleen collectie Banden bijwerken",
    )
    args = p.parse_args()

    domain, token, api = load_motox_env()
    print(f"Shop: {domain}  API {api}  {'APPLY' if args.yes else 'dry-run'}", flush=True)

    collections = fetch_all_collections(domain, token, api)
    by_title = _by_title(collections)
    groups = planned_groups(args.banden_only, by_title)

    sess = _session()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    report: list[dict] = []

    for title, types, source in groups:
        col = by_title.get(title)
        if not col:
            report.append(
                {
                    "title": title,
                    "status": "missing-collection",
                    "source": source,
                    "wanted": types,
                }
            )
            print(f"SKIP  {title}: collectie bestaat niet ({len(types)} types)", flush=True)
            continue
        if "PRODUCT_METAFIELD_DEFINITION" in _rule_columns(col):
            report.append(
                {
                    "title": title,
                    "status": "skipped-ymm",
                    "source": source,
                    "wanted": types,
                }
            )
            print(f"SKIP  {title}: YMM-collectie", flush=True)
            continue
        result = apply_group(sess, url, token, col, types, yes=args.yes)
        result["source"] = source
        report.append(result)
        added = result.get("added") or []
        skipped = result.get("skipped_limit") or []
        print(
            f"{result['status'].upper():16s} {title}: "
            f"+{len(added)} types  "
            f"rules {result.get('before', '?')}→{result.get('after', '?')}  "
            f"limit-skip {len(skipped)}",
            flush=True,
        )
        if args.yes and result.get("status") == "applied":
            time.sleep(0.4)

    OUT_PATH.write_text(
        json.dumps(
            {
                "shop": domain,
                "apply": args.yes,
                "banden_only": args.banden_only,
                "groups": report,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    applied = sum(1 for r in report if r.get("status") == "applied")
    dry = sum(1 for r in report if r.get("status") == "dry-run")
    errors = [r for r in report if r.get("status") == "error"]
    print(f"\nRapport: {OUT_PATH}", flush=True)
    print(f"applied={applied} dry-run={dry} errors={len(errors)}", flush=True)
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
