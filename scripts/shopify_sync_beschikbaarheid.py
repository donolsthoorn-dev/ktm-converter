#!/usr/bin/env python3
"""Zet productmetafield custom.beschikbaarheid voor het collectiefilter.

Waarde volgt de productkaart:
  beschikbare variant zonder leverdatum  → Direct beschikbaar
  variant met leverdatum                  → Levertijd van toepassing
  uitverkocht zonder leverdatum           → metafield leeg

Gebruik:
  python3 scripts/shopify_sync_beschikbaarheid.py
  python3 scripts/shopify_sync_beschikbaarheid.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import defaultdict
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402

STATUS_DIRECT = "Direct beschikbaar"
STATUS_ETA = "Levertijd van toepassing"
NAMESPACE = "custom"
KEY = "beschikbaarheid"
CHOICES = json.dumps([STATUS_DIRECT, STATUS_ETA], ensure_ascii=False)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        statusValue: metafield(namespace: "custom", key: "beschikbaarheid") { value }
        variants {
          edges {
            node {
              availableForSale
              eta: metafield(namespace: "global", key: "inventory_policy_eta_date") { value }
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation BeschikbaarheidBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_POLL = """
query BeschikbaarheidPoll($id: ID!) {
  node(id: $id) {
    ... on BulkOperation { status errorCode objectCount url }
  }
}
"""

_GQL_CURRENT = """
query { currentBulkOperation { id status } }
"""

_GQL_DEF = """
query BeschikbaarheidDef {
  metafieldDefinitions(first: 1, ownerType: PRODUCT, namespace: "custom", key: "beschikbaarheid") {
    nodes { id name access { storefront } }
  }
}
"""

_GQL_DEF_CREATE = """
mutation BeschikbaarheidDefCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition { id }
    userErrors { field message code }
  }
}
"""

_GQL_SET = """
mutation BeschikbaarheidSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    userErrors { field message }
  }
}
"""

_GQL_DELETE = """
mutation BeschikbaarheidDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    userErrors { field message }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def gql(sess: requests.Session, query: str, variables: dict | None = None) -> dict:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
    )
    last: dict = {}
    for attempt in range(20):
        try:
            r = sess.post(
                url,
                headers={
                    "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
                    "Content-Type": "application/json",
                },
                json={"query": query, "variables": variables or {}},
                timeout=(15, 180),
                proxies={"http": None, "https": None},
            )
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            wait = min(2.0 * (attempt + 1), 30.0)
            print(f"  Netwerk/timeout, opnieuw over {wait:.0f}s ({exc.__class__.__name__})", flush=True)
            time.sleep(wait)
            continue
        if r.status_code == 429:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs):
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    return last


def ensure_definition(sess: requests.Session) -> None:
    body = gql(sess, _GQL_DEF)
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    nodes = (((body.get("data") or {}).get("metafieldDefinitions")) or {}).get("nodes") or []
    if nodes:
        print(f"Metafield bestaat al: {nodes[0].get('id')}", flush=True)
        return
    created = gql(
        sess,
        _GQL_DEF_CREATE,
        {
            "definition": {
                "name": "Beschikbaarheid",
                "namespace": NAMESPACE,
                "key": KEY,
                "description": "Direct beschikbaar of levertijd van toepassing, voor het collectiefilter.",
                "type": "single_line_text_field",
                "ownerType": "PRODUCT",
                "access": {"storefront": "PUBLIC_READ"},
                "capabilities": {"adminFilterable": {"enabled": True}},
                "validations": [{"name": "choices", "value": CHOICES}],
            }
        },
    )
    if created.get("errors"):
        raise RuntimeError(created["errors"])
    result = ((created.get("data") or {}).get("metafieldDefinitionCreate")) or {}
    errors = result.get("userErrors") or []
    if errors:
        raise RuntimeError(errors)
    print(f"Metafield aangemaakt: {(result.get('createdDefinition') or {}).get('id')}", flush=True)


def _wait_bulk(sess: requests.Session) -> None:
    for _ in range(40):
        body = gql(sess, _GQL_CURRENT)
        current = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (current.get("status") or "").upper()
        if status in ("", "COMPLETED", "FAILED", "CANCELED", "CANCELLED", "EXPIRED"):
            return
        print(f"  Wacht op lopende bulk ({status})", flush=True)
        time.sleep(8)


def download_catalog(sess: requests.Session, dest: Path) -> None:
    _wait_bulk(sess)
    mutation = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY))
    body = gql(sess, mutation)
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    if run.get("userErrors"):
        raise RuntimeError(run["userErrors"])
    op_id = (run.get("bulkOperation") or {}).get("id") or ""
    if not op_id:
        raise RuntimeError("Geen bulk operation id.")
    print(f"Bulk gestart ({op_id})", flush=True)
    url = ""
    interval = 3.0
    while True:
        poll = gql(sess, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url") or ""
            break
        if status in ("FAILED", "CANCELED", "CANCELLED", "EXPIRED"):
            raise RuntimeError(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(interval)
        interval = min(interval + 0.5, 15.0)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if not url:
        dest.write_text("", encoding="utf-8")
        return
    with sess.get(url, stream=True, timeout=(15, 300), proxies={"http": None, "https": None}) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(1024 * 256):
                if chunk:
                    fh.write(chunk)
    print(f"Bulk gedownload ({dest.stat().st_size / (1024 * 1024):.1f} MB)", flush=True)


def desired_status(variants: list[dict]) -> str | None:
    """Zelfde regel als de productkaart: eerste beschikbare variant, anders de eerste."""
    if not variants:
        return None
    chosen = next((v for v in variants if v.get("availableForSale")), variants[0])
    eta = ((chosen.get("eta") or {}).get("value") or "").strip()
    if chosen.get("availableForSale") and not eta:
        return STATUS_DIRECT
    if eta:
        return STATUS_ETA
    return None


def load_products(path: Path) -> list[tuple[str, str | None, str | None]]:
    variants: dict[str, list[dict]] = defaultdict(list)
    products: dict[str, str | None] = {}
    order: list[str] = []
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            parent = obj.get("__parentId")
            if parent:
                variants[parent].append(obj)
                continue
            gid = str(obj.get("id") or "")
            if not gid.startswith("gid://shopify/Product/"):
                continue
            current = ((obj.get("statusValue") or {}).get("value") or "").strip() or None
            products[gid] = current
            order.append(gid)
    rows = []
    for gid in order:
        rows.append((gid, products[gid], desired_status(variants.get(gid) or [])))
    return rows


def apply_changes(sess: requests.Session, rows: list[tuple[str, str | None, str | None]], dry_run: bool) -> None:
    sets: list[dict] = []
    deletes: list[dict] = []
    for gid, current, desired in rows:
        if desired == current:
            continue
        if desired:
            sets.append(
                {
                    "ownerId": gid,
                    "namespace": NAMESPACE,
                    "key": KEY,
                    "type": "single_line_text_field",
                    "value": desired,
                }
            )
        elif current:
            deletes.append({"ownerId": gid, "namespace": NAMESPACE, "key": KEY})
    print(f"Bijwerken: {len(sets)} zetten, {len(deletes)} legen", flush=True)
    if dry_run:
        return
    for i in range(0, len(sets), 25):
        batch = sets[i : i + 25]
        body = gql(sess, _GQL_SET, {"metafields": batch})
        errors = (((body.get("data") or {}).get("metafieldsSet")) or {}).get("userErrors") or []
        if body.get("errors") or errors:
            raise RuntimeError(body.get("errors") or errors)
        if (i // 25) % 20 == 0:
            print(f"  gezet {min(i + 25, len(sets))}/{len(sets)}", flush=True)
    for i in range(0, len(deletes), 25):
        batch = deletes[i : i + 25]
        body = gql(sess, _GQL_DELETE, {"metafields": batch})
        errors = (((body.get("data") or {}).get("metafieldsDelete")) or {}).get("userErrors") or []
        if body.get("errors") or errors:
            raise RuntimeError(body.get("errors") or errors)
        if (i // 25) % 20 == 0:
            print(f"  geleegd {min(i + 25, len(deletes))}/{len(deletes)}", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description="Sync custom.beschikbaarheid op ktm-shop.nl")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--jsonl",
        type=Path,
        default=ROOT / "cache" / "beschikbaarheid_bulk.jsonl",
    )
    parser.add_argument(
        "--reuse-jsonl",
        action="store_true",
        help="Gebruik bestaande bulk-export i.p.v. opnieuw te downloaden",
    )
    args = parser.parse_args()
    if not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1
    sess = _session()
    ensure_definition(sess)
    if args.reuse_jsonl and args.jsonl.is_file() and args.jsonl.stat().st_size > 0:
        print(f"Bestaande bulk hergebruiken: {args.jsonl}", flush=True)
    else:
        download_catalog(sess, args.jsonl)
    rows = load_products(args.jsonl)
    counts: dict[str, int] = defaultdict(int)
    for _, _, desired in rows:
        counts[desired or "(leeg)"] += 1
    print(f"Producten: {len(rows)} — " + ", ".join(f"{k}: {v}" for k, v in sorted(counts.items())), flush=True)
    apply_changes(sess, rows, args.dry_run)
    print("Klaar.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
