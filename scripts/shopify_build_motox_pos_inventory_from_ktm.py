#!/usr/bin/env python3
"""
Eenmalige baseline: KTM-shop.nl voorraad → Motox locatie Winkelvoorraad POS.

1. Haalt alle tracked variant-SKU's + on_hand op locatie KTM-shop.nl (bron).
2. Haalt alle Motox-varianten (handle + options + SKU).
3. Join op SKU → Shopify inventory-CSV alleen voor Winkelvoorraad POS.

  python3 scripts/shopify_build_motox_pos_inventory_from_ktm.py

Vereist: `.env` (ktm-shop.nl) + `.env.motox` (ktm-shop-nederland).
"""

from __future__ import annotations

import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

KTM_LOCATION_NAME = "KTM-shop.nl"
MOTOX_LOCATION_NAME = "Winkelvoorraad POS"
_REQUEST_TIMEOUT = (15, 120)
_PAGE = 100


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        raise SystemExit(f"Missing env file: {path}")
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, shop: str, token: str, api: str, query: str, variables: dict | None = None) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(8):
        r = sess.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": query, "variables": variables or {}},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(1.5 + attempt)
            continue
        if r.status_code >= 500:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            # throttle / cost
            msg = str(body["errors"])
            if "Throttled" in msg or "exceeds" in msg.lower():
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(body["errors"])
        return body
    raise RuntimeError("GraphQL failed after retries")


def _rest_get(sess: requests.Session, url: str, token: str) -> tuple[dict, str | None]:
    for attempt in range(8):
        r = sess.get(
            url,
            headers={"X-Shopify-Access-Token": token},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(1.5 + attempt)
            continue
        if r.status_code >= 500:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        link = r.headers.get("Link") or ""
        next_url = None
        if 'rel="next"' in link:
            for part in link.split(","):
                if 'rel="next"' in part:
                    next_url = part[part.find("<") + 1 : part.find(">")]
                    break
        return r.json(), next_url
    raise RuntimeError(f"REST failed: {url}")


def resolve_ktm_location_id(sess: requests.Session, shop: str, token: str, api: str) -> str:
    body = _gql(
        sess,
        shop,
        token,
        api,
        "{ locations(first: 25, includeLegacy: true) { nodes { id name isActive } } }",
    )
    nodes = body["data"]["locations"]["nodes"]
    for n in nodes:
        if n.get("name") == KTM_LOCATION_NAME and n.get("isActive"):
            return n["id"]
    names = [n.get("name") for n in nodes]
    raise SystemExit(f"Locatie {KTM_LOCATION_NAME!r} niet gevonden. Wel: {names}")


def fetch_ktm_sku_on_hand(
    sess: requests.Session, shop: str, token: str, api: str, location_id: str
) -> dict[str, int]:
    """SKU → max on_hand op KTM-locatie (meerdere listings metzelfde SKU: max)."""
    q = """
    query ($c: String, $loc: ID!) {
      productVariants(first: 100, after: $c) {
        pageInfo { hasNextPage endCursor }
        nodes {
          sku
          inventoryItem {
            tracked
            inventoryLevel(locationId: $loc) {
              quantities(names: ["on_hand", "available"]) { name quantity }
            }
          }
        }
      }
    }
    """
    sku_qty: dict[str, int] = {}
    cursor = None
    page = 0
    variants = 0
    while True:
        body = _gql(
            sess,
            shop,
            token,
            api,
            q,
            {"c": cursor, "loc": location_id},
        )
        conn = body["data"]["productVariants"]
        for node in conn["nodes"]:
            variants += 1
            sku = (node.get("sku") or "").strip()
            if not sku:
                continue
            item = node.get("inventoryItem") or {}
            if not item.get("tracked"):
                continue
            level = item.get("inventoryLevel") or {}
            qty_map = {x["name"]: int(x["quantity"]) for x in (level.get("quantities") or [])}
            # Prefer on_hand; fall back to available
            if "on_hand" in qty_map:
                qty = qty_map["on_hand"]
            else:
                qty = qty_map.get("available", 0)
            prev = sku_qty.get(sku)
            if prev is None or qty > prev:
                sku_qty[sku] = qty
        page += 1
        if page % 20 == 0 or not conn["pageInfo"]["hasNextPage"]:
            print(
                f"  KTM page {page}: variants_seen={variants} unique_skus={len(sku_qty)}",
                flush=True,
            )
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
        time.sleep(0.05)
    return sku_qty


def fetch_motox_variants(
    sess: requests.Session, shop: str, token: str, api: str
) -> list[dict]:
    """Alle Motox-varianten met SKU (voor inventory CSV-rijen)."""
    url = (
        f"https://{shop}/admin/api/{api}/products.json"
        f"?limit=250&fields=id,handle,title,options,variants"
    )
    rows: list[dict] = []
    page = 0
    while url:
        data, next_url = _rest_get(sess, url, token)
        for p in data.get("products") or []:
            handle = (p.get("handle") or "").strip()
            title = p.get("title") or ""
            options = p.get("options") or []
            opt_names = [(o.get("name") or "") for o in options]
            while len(opt_names) < 3:
                opt_names.append("")
            for v in p.get("variants") or []:
                sku = (v.get("sku") or "").strip()
                if not sku:
                    continue
                rows.append(
                    {
                        "Handle": handle,
                        "Title": title,
                        "Option1 Name": opt_names[0] or "Title",
                        "Option1 Value": v.get("option1") or "Default Title",
                        "Option2 Name": opt_names[1] if v.get("option2") is not None else "",
                        "Option2 Value": v.get("option2") or "",
                        "Option3 Name": opt_names[2] if v.get("option3") is not None else "",
                        "Option3 Value": v.get("option3") or "",
                        "SKU": sku,
                    }
                )
        page += 1
        if page % 20 == 0 or not next_url:
            print(f"  Motox page {page}: variant_rows_with_sku={len(rows)}", flush=True)
        url = next_url
        time.sleep(0.05)
    return rows


def write_inventory_csv(path: Path, rows: list[dict]) -> None:
    fields = [
        "Handle",
        "Title",
        "Option1 Name",
        "Option1 Value",
        "Option2 Name",
        "Option2 Value",
        "Option3 Name",
        "Option3 Value",
        "SKU",
        "HS Code",
        "COO",
        "Location",
        "Bin name",
        "Incoming (not editable)",
        "Unavailable (not editable)",
        "Committed (not editable)",
        "Available (not editable)",
        "On hand (current)",
        "On hand (new)",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "Handle": r["Handle"],
                    "Title": r["Title"],
                    "Option1 Name": r["Option1 Name"],
                    "Option1 Value": r["Option1 Value"],
                    "Option2 Name": r["Option2 Name"],
                    "Option2 Value": r["Option2 Value"],
                    "Option3 Name": r["Option3 Name"],
                    "Option3 Value": r["Option3 Value"],
                    "SKU": r["SKU"],
                    "HS Code": "",
                    "COO": "",
                    "Location": MOTOX_LOCATION_NAME,
                    "Bin name": "",
                    "Incoming (not editable)": "0",
                    "Unavailable (not editable)": "0",
                    "Committed (not editable)": "0",
                    "Available (not editable)": "0",
                    # Geen safety-lock: current leeg laten faalt soms;
                    # we zetten current niet — alleen On hand (new).
                    "On hand (current)": "",
                    "On hand (new)": str(r["on_hand_new"]),
                }
            )


def main() -> None:
    ktm = _load_dotenv(ROOT / ".env")
    motox = _load_dotenv(ROOT / ".env.motox")
    sess = _session()

    ktm_shop = ktm["SHOPIFY_SHOP_DOMAIN"]
    ktm_token = ktm["SHOPIFY_ACCESS_TOKEN"]
    ktm_api = ktm.get("SHOPIFY_ADMIN_API_VERSION", "2024-10")

    motox_shop = motox["SHOPIFY_SHOP_DOMAIN"]
    motox_token = motox["SHOPIFY_ACCESS_TOKEN"]
    motox_api = motox.get("SHOPIFY_ADMIN_API_VERSION", "2024-10")

    print(f"KTM source: {ktm_shop} @ {KTM_LOCATION_NAME}", flush=True)
    loc_id = resolve_ktm_location_id(sess, ktm_shop, ktm_token, ktm_api)
    print(f"  location id: {loc_id}", flush=True)
    print("Fetching KTM SKU on_hand…", flush=True)
    ktm_sku_qty = fetch_ktm_sku_on_hand(sess, ktm_shop, ktm_token, ktm_api, loc_id)
    print(f"KTM unique tracked SKUs with level: {len(ktm_sku_qty)}", flush=True)

    print(f"Motox dest: {motox_shop} → {MOTOX_LOCATION_NAME}", flush=True)
    print("Fetching Motox variants…", flush=True)
    motox_rows = fetch_motox_variants(sess, motox_shop, motox_token, motox_api)
    print(f"Motox variant rows with SKU: {len(motox_rows)}", flush=True)

    matched: list[dict] = []
    missing = 0
    qty_hist: dict[str, int] = defaultdict(int)
    for row in motox_rows:
        sku = row["SKU"]
        if sku not in ktm_sku_qty:
            missing += 1
            continue
        qty = ktm_sku_qty[sku]
        out = dict(row)
        out["on_hand_new"] = qty
        matched.append(out)
        if qty == 0:
            qty_hist["zero"] += 1
        else:
            qty_hist["positive"] += 1

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = ROOT / "output" / f"motox_pos_inventory_from_ktm_full_{stamp}.csv"
    write_inventory_csv(out_path, matched)

    # Small sample check file
    sample_path = ROOT / "output" / f"motox_pos_inventory_from_ktm_sample20_{stamp}.csv"
    write_inventory_csv(sample_path, matched[:20])

    print(flush=True)
    print(f"Wrote {out_path}", flush=True)
    print(f"Sample {sample_path}", flush=True)
    print(f"Matched Motox variants (SKU in KTM): {len(matched)}", flush=True)
    print(f"Motox variants without KTM SKU (skipped): {missing}", flush=True)
    print(
        f"On hand (new)>0: {qty_hist['positive']}  ==0: {qty_hist['zero']}  "
        f"sum={sum(r['on_hand_new'] for r in matched)}",
        flush=True,
    )


if __name__ == "__main__":
    main()
