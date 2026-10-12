#!/usr/bin/env python3
"""Zet resterende Bihr-types in collectie Overig en publiceer unpublished producten.

De Banden-collectie had 512 ACTIVE producten zonder Online Store-publicatie
(daardoor 0 op motocrosshop.nl). Dit script:

1. Maakt slimme collectie(s) Overig met TYPE EQUALS voor types die nog nergens hangen
2. Publiceert die collectie(s) op alle sales channels
3. Zet unpublished ACTIVE producten in Banden (en Overig) op Online Store

  python3 scripts/motox_overig_collection.py
  python3 scripts/motox_overig_collection.py --yes
  python3 scripts/motox_overig_collection.py --yes --skip-overig
  python3 scripts/motox_overig_collection.py --yes --skip-publish
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.motox_shopify_cache import _gql, _session, load_motox_env  # noqa: E402
from modules.shopify_collections import (  # noqa: E402
    COLLECTION_CREATE,
    MAX_SMART_RULES,
    PRODUCT_PUBLISH_ONLINE,
    PUBLISHABLE_PUBLISH,
    fetch_all_collections,
    type_equals_rule,
)

PROPOSALS_PATH = PROJECT_ROOT / "output" / "motox_unmapped_types_proposals.json"
OUT_PATH = PROJECT_ROOT / "output" / "motox_overig_collection.json"
BANDEN_HANDLE = "banden"
OVERIG_TITLE = "Overig"

_PUBS_QUERY = """
query MotoxPublications {
  publications(first: 50) { nodes { id name } }
}
"""

_UNPUBLISHED_IN_COLLECTION = """
query UnpublishedInCollection($q: String!, $c: String) {
  products(first: 100, after: $c, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes { id title status publishedAt }
  }
}
"""


def _type_equals(collections: list[dict]) -> set[str]:
    out: set[str] = set()
    for c in collections:
        for r in ((c.get("ruleSet") or {}).get("rules")) or []:
            if r.get("column") == "TYPE" and (r.get("relation") or "").upper() == "EQUALS":
                cond = (r.get("condition") or "").strip()
                if cond:
                    out.add(cond)
    return out


def remaining_types(collections: list[dict]) -> list[str]:
    mapped = _type_equals(collections)
    payload = json.loads(PROPOSALS_PATH.read_text(encoding="utf-8"))
    types = {str(r.get("type") or "").strip() for r in payload.get("rows") or []}
    types |= {str(t).strip() for t in (payload.get("ignored_tire_types") or [])}
    types.discard("")
    return sorted(t for t in types if t not in mapped)


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def publication_ids(sess, url: str, token: str) -> list[tuple[str, str]]:
    body = _gql(sess, url, token, _PUBS_QUERY)
    nodes = (((body.get("data") or {}).get("publications") or {}).get("nodes")) or []
    return [(n["id"], n.get("name") or "") for n in nodes if n.get("id")]


def publish_resource(sess, url: str, token: str, gid: str, pub_ids: list[str]) -> list:
    if not pub_ids:
        return [{"message": "geen publications"}]
    body = _gql(
        sess,
        url,
        token,
        PUBLISHABLE_PUBLISH,
        {"id": gid, "input": [{"publicationId": pid} for pid in pub_ids]},
    )
    if body.get("errors"):
        return body.get("errors") or []
    return (((body.get("data") or {}).get("publishablePublish")) or {}).get("userErrors") or []


def create_overig(
    sess,
    url: str,
    token: str,
    types: list[str],
    existing: dict[str, dict],
    *,
    yes: bool,
) -> list[dict]:
    report = []
    chunks = _chunks(types, MAX_SMART_RULES)
    for i, chunk in enumerate(chunks):
        title = OVERIG_TITLE if i == 0 else f"{OVERIG_TITLE} {i + 1}"
        handle = "overig" if i == 0 else f"overig-{i + 1}"
        col = existing.get(title)
        item = {"title": title, "handle": handle, "types": chunk}
        if col:
            item["id"] = col.get("id")
            item["status"] = "exists"
            report.append(item)
            print(f"EXISTS {title} ({len(((col.get('ruleSet') or {}).get('rules')) or [])} rules)", flush=True)
            continue
        item["status"] = "dry-run" if not yes else "created"
        if not yes:
            report.append(item)
            print(f"DRY-RUN {title}: {len(chunk)} types", flush=True)
            continue
        body = _gql(
            sess,
            url,
            token,
            COLLECTION_CREATE,
            {
                "input": {
                    "title": title,
                    "handle": handle,
                    "ruleSet": {
                        "appliedDisjunctively": True,
                        "rules": [type_equals_rule(t) for t in chunk],
                    },
                }
            },
        )
        payload = ((body.get("data") or {}).get("collectionCreate")) or {}
        errs = (payload.get("userErrors") or []) or (body.get("errors") or [])
        if errs:
            item["status"] = "error"
            item["errors"] = errs
            report.append(item)
            print(f"ERROR {title}: {errs}", flush=True)
            continue
        created = payload.get("collection") or {}
        item["id"] = created.get("id")
        item["productsCount"] = ((created.get("productsCount") or {}).get("count"))
        report.append(item)
        print(
            f"CREATED {title} id={item['id']} types={len(chunk)} "
            f"products≈{item.get('productsCount')}",
            flush=True,
        )
        time.sleep(0.3)
    return report


def unpublished_product_ids(sess, url: str, token: str, collection_numeric_id: str) -> list[dict]:
    q = f"collection_id:{collection_numeric_id} published_status:unpublished status:active"
    out: list[dict] = []
    cursor = None
    while True:
        body = _gql(
            sess,
            url,
            token,
            _UNPUBLISHED_IN_COLLECTION,
            {"q": q, "c": cursor},
        )
        conn = ((body.get("data") or {}).get("products")) or {}
        for n in conn.get("nodes") or []:
            if not n.get("publishedAt"):
                out.append(n)
        page = conn.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
    return out


def publish_products(sess, url: str, token: str, products: list[dict], *, yes: bool) -> dict:
    ok = 0
    fail = 0
    errors = []
    if not yes:
        return {"status": "dry-run", "count": len(products)}
    for i, p in enumerate(products, 1):
        body = _gql(
            sess,
            url,
            token,
            PRODUCT_PUBLISH_ONLINE,
            {"input": {"id": p["id"], "published": True}},
        )
        payload = ((body.get("data") or {}).get("productUpdate")) or {}
        uerr = payload.get("userErrors") or []
        if body.get("errors") or uerr:
            fail += 1
            errors.append({"id": p["id"], "errors": body.get("errors") or uerr})
        else:
            ok += 1
        if i % 50 == 0:
            print(f"  published {i}/{len(products)} (ok={ok} fail={fail})", flush=True)
        time.sleep(0.12)
    return {"status": "applied", "ok": ok, "fail": fail, "errors": errors[:20]}


def _numeric_id(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def main() -> int:
    p = argparse.ArgumentParser(description="Overig-collectie + Online Store publicatie")
    p.add_argument("--yes", action="store_true", help="Schrijf naar Shopify")
    p.add_argument("--skip-overig", action="store_true")
    p.add_argument("--skip-publish", action="store_true")
    args = p.parse_args()

    domain, token, api = load_motox_env()
    print(f"Shop: {domain}  API {api}  {'APPLY' if args.yes else 'dry-run'}", flush=True)
    sess = _session()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    collections = fetch_all_collections(domain, token, api)
    by_title = {(c.get("title") or "").strip(): c for c in collections}
    leftover = remaining_types(collections)
    print(f"Types zonder TYPE EQUALS: {len(leftover)}", flush=True)

    report: dict = {"leftover_types": leftover, "overig": [], "publish": {}}

    if not args.skip_overig:
        report["overig"] = create_overig(
            sess, url, token, leftover, by_title, yes=args.yes
        )
        if args.yes:
            pubs = publication_ids(sess, url, token)
            pub_ids = [pid for pid, _ in pubs]
            for item in report["overig"]:
                gid = item.get("id")
                if not gid or item.get("status") == "error":
                    continue
                errs = publish_resource(sess, url, token, gid, pub_ids)
                item["publish_errors"] = errs
                print(
                    f"PUBLISH collection {item['title']}: "
                    f"{'ok' if not errs else errs}",
                    flush=True,
                )

    if not args.skip_publish:
        if args.yes:
            collections = fetch_all_collections(domain, token, api)
        handles = {BANDEN_HANDLE}
        for item in report.get("overig") or []:
            if item.get("handle"):
                handles.add(item["handle"])
        by_handle = {(c.get("handle") or ""): c for c in collections}
        for handle in sorted(handles):
            col = by_handle.get(handle)
            if not col:
                print(f"SKIP publish: collectie {handle} bestaat niet", flush=True)
                continue
            products = unpublished_product_ids(sess, url, token, _numeric_id(col["id"]))
            print(
                f"Unpublished in {col.get('title')}: {len(products)}",
                flush=True,
            )
            result = publish_products(sess, url, token, products, yes=args.yes)
            report["publish"][handle] = result
            print(f"  {handle}: {result}", flush=True)

    OUT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Rapport: {OUT_PATH}", flush=True)
    errs = [
        x
        for x in (report.get("overig") or [])
        if x.get("status") == "error"
    ]
    pub_fail = sum(int((v or {}).get("fail") or 0) for v in (report.get("publish") or {}).values())
    return 1 if errs or pub_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
