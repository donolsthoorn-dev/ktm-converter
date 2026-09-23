#!/usr/bin/env python3
"""
Vul Motox ymm_summary en de platte fits_on-velden vanuit global.fits_on.

De e-Bihr-sync schrijft die velden alleen als de fits_on-JSON zelf wijzigt.
Producten die al een fits_on hebben, maar nog geen samenvatting, blijven dan leeg.
Deze job loopt de catalogus na en schrijft de afgeleide velden opnieuw zodra
ze ontbreken of niet meer kloppen bij de huidige fits_on.

  python3 scripts/motox_ymm_derived_fill.py
  python3 scripts/motox_ymm_derived_fill.py --handle 1006458
  python3 scripts/motox_ymm_derived_fill.py --apply --limit 50

Vereist: SHOPIFY_MOTOX_ACCESS_TOKEN en SHOPIFY_MOTOX_SHOP_DOMAIN in .env.motox.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402
from modules.ebihr.ymm_derived import derived_ymm_fields, needs_write  # noqa: E402
from modules.motox_shopify_cache import _gql, load_motox_env  # noqa: E402

_TIMEOUT = (12, 600)
_BULK_PATH = ROOT / "cache" / "motox" / "ymm_derived_bulk.jsonl"

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        fitsOn: metafield(namespace: "global", key: "fits_on") { value }
        ymmSummary: metafield(namespace: "global", key: "ymm_summary") { value }
        fitsMake: metafield(namespace: "global", key: "fits_on_make") { value }
        fitsModel: metafield(namespace: "global", key: "fits_on_model") { value }
        fitsYear: metafield(namespace: "global", key: "fits_on_year") { value }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation MotoxYmmDerivedBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_CURRENT = """
query {
  currentBulkOperation { id status errorCode objectCount }
}
"""

_GQL_POLL = """
query MotoxYmmDerivedPoll($id: ID!) {
  node(id: $id) {
    ... on BulkOperation { status errorCode objectCount url }
  }
}
"""

_GQL_ONE = """
query MotoxYmmDerivedOne($q: String!) {
  products(first: 5, query: $q) {
    nodes {
      id
      handle
      title
      status
      fitsOn: metafield(namespace: "global", key: "fits_on") { value }
      ymmSummary: metafield(namespace: "global", key: "ymm_summary") { value }
      fitsMake: metafield(namespace: "global", key: "fits_on_make") { value }
      fitsModel: metafield(namespace: "global", key: "fits_on_model") { value }
      fitsYear: metafield(namespace: "global", key: "fits_on_year") { value }
    }
  }
}
"""


def _metafield_value(node: dict | None) -> str:
    if not isinstance(node, dict):
        return ""
    return str(node.get("value") or "")


def _product_row(obj: dict) -> dict[str, str]:
    return {
        "id": (obj.get("id") or "").rsplit("/", 1)[-1],
        "gid": obj.get("id") or "",
        "handle": (obj.get("handle") or "").strip(),
        "title": (obj.get("title") or "").strip(),
        "status": (obj.get("status") or "").strip().upper(),
        "fits_on": _metafield_value(obj.get("fitsOn")),
        "ymm_summary": _metafield_value(obj.get("ymmSummary")),
        "fits_on_make": _metafield_value(obj.get("fitsMake")),
        "fits_on_model": _metafield_value(obj.get("fitsModel")),
        "fits_on_year": _metafield_value(obj.get("fitsYear")),
    }


def _wait_bulk(sess, url: str, token: str) -> None:
    while True:
        body = _gql(sess, url, token, _GQL_CURRENT)
        cur = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (cur.get("status") or "").upper()
        if not status or status in ("COMPLETED", "FAILED", "CANCELED"):
            return
        print(f"  Wacht op lopende bulk: {status} ({cur.get('objectCount')})", flush=True)
        time.sleep(5)


def _run_bulk(admin: MotoxAdmin) -> Path:
    sess = admin.sess
    url = admin.url
    token = admin.token
    _wait_bulk(sess, url, token)
    query = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY))
    body = _gql(sess, url, token, query)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    uerr = run.get("userErrors") or []
    if uerr:
        raise SystemExit(f"Bulk geweigerd: {uerr}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen bulk operation id.")
    print("Bulk-export gestart…", flush=True)
    poll_interval = 3.0
    while True:
        poll = _gql(sess, url, token, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            download = node.get("url")
            if not download:
                raise SystemExit("Bulk klaar zonder URL.")
            _BULK_PATH.parent.mkdir(parents=True, exist_ok=True)
            with sess.get(download, stream=True, timeout=_TIMEOUT, proxies={"http": None, "https": None}) as response:
                response.raise_for_status()
                with _BULK_PATH.open("wb") as handle:
                    for chunk in response.iter_content(1024 * 256):
                        if chunk:
                            handle.write(chunk)
            return _BULK_PATH
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _parse_bulk(path: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            gid = obj.get("id") or ""
            if gid.startswith("gid://shopify/Product/") and not obj.get("__parentId"):
                rows.append(_product_row(obj))
    return rows


def _load_handles(admin: MotoxAdmin, handles: list[str]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for handle in handles:
        body = admin.gql(_GQL_ONE, {"q": f"handle:{handle}"})
        if body.get("errors"):
            raise SystemExit(str(body.get("errors"))[:500])
        nodes = (((body.get("data") or {}).get("products") or {}).get("nodes")) or []
        matched = [node for node in nodes if (node.get("handle") or "") == handle]
        if not matched:
            print(f"Handle niet gevonden: {handle}", flush=True)
            continue
        rows.append(_product_row(matched[0]))
    return rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="Schrijf naar Motox")
    parser.add_argument("--limit", type=int, default=0, help="Max te schrijven producten (0 = alle)")
    parser.add_argument("--sleep", type=float, default=0.2, help="Pauze tussen writes")
    parser.add_argument("--handle", action="append", default=[], help="Alleen deze handle (herhaalbaar)")
    args = parser.parse_args()

    # load_motox_env faalt hard als de creds ontbreken; daarna leest MotoxAdmin ze.
    load_motox_env()
    admin = MotoxAdmin()
    print(f"Shop: {admin.domain}  apply={args.apply}", flush=True)

    if args.handle:
        products = _load_handles(admin, args.handle)
    else:
        path = _run_bulk(admin)
        products = _parse_bulk(path)
        print(f"Bulk gelezen: {len(products)} producten uit {path}", flush=True)

    scanned = with_fits = unchanged = todo_n = skipped_archived = 0
    todo: list[tuple[dict[str, str], dict[str, str]]] = []
    for product in products:
        scanned += 1
        if product["status"] == "ARCHIVED":
            skipped_archived += 1
            continue
        desired = derived_ymm_fields(product["fits_on"])
        if not desired:
            continue
        with_fits += 1
        if not needs_write(product, desired):
            unchanged += 1
            continue
        todo.append((product, desired))
        if args.limit and len(todo) >= args.limit:
            break

    print(
        f"Gescand: {scanned}. Archief overgeslagen: {skipped_archived}. "
        f"Met fits_on: {with_fits}. Al gelijk: {unchanged}. Te schrijven: {len(todo)}.",
        flush=True,
    )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"motox_ymm_derived_fill_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = ["product_id", "handle", "title", "ymm_summary", "result"]
    ok = fail = 0
    with out.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        for index, (product, desired) in enumerate(todo, start=1):
            result = "dry-run"
            if args.apply:
                result = admin.set_ymm_metafields(
                    product["id"],
                    fits_on_json=product["fits_on"],
                    ymm_summary=desired["ymm_summary"],
                    fits_on_year=desired["fits_on_year"],
                    fits_on_make=desired["fits_on_make"],
                    fits_on_model=desired["fits_on_model"],
                ) or "ok"
                time.sleep(max(0.0, args.sleep))
            if result == "ok" or result == "dry-run":
                ok += 1
            else:
                fail += 1
                print(f"Fout {product['handle']}: {result[:300]}", flush=True)
            writer.writerow(
                {
                    "product_id": product["id"],
                    "handle": product["handle"],
                    "title": product["title"],
                    "ymm_summary": desired["ymm_summary"],
                    "result": result,
                }
            )
            if index % 50 == 0:
                print(f"  {index}/{len(todo)}  ok={ok} fail={fail}", flush=True)

    mode = "Dry-run" if not args.apply else "Klaar"
    print(f"{mode}. ok={ok} fail={fail}  CSV: {out}", flush=True)
    return 1 if fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
