#!/usr/bin/env python3
"""Check-examples from Excel + Motox reserved/negative inventory snapshot."""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from modules.motox_shopify_cache import load_motox_env, _gql, _session  # noqa: E402
from modules.env_loader import load_dotenv  # noqa: E402

SRC = ROOT / "motox" / "Voorraad winkel - WERKDOC EJIS.csv"
OUT = ROOT / "output" / "winkelvoorraad_check_examples.json"

LOC = {
    "bak": "Bak (product.metafields.global.parts_box)",
    "vak": "Vak (product.metafields.global.parts_compartment)",
    "verdieping": "Verdieping (product.metafields.global.parts_floor)",
    "rij": "Rij (product.metafields.global.parts_row)",
    "plank": "Plank (product.metafields.global.parts_shelve)",
}


def sku_key(s: str) -> str:
    return re.sub(r"[\s\-\.]+", "", str(s)).upper()


def loc_label(r) -> str:
    parts = []
    v = str(r.get(LOC["verdieping"], "")).strip()
    k = str(r.get(LOC["vak"], "")).strip()
    b = str(r.get(LOC["bak"], "")).strip()
    p = str(r.get(LOC["plank"], "")).strip()
    rij = str(r.get(LOC["rij"], "")).strip()
    if v:
        parts.append(v)
    if k:
        parts.append(f"kast {k}" if k not in {"A", "a"} else "kast A")
    if rij:
        parts.append(f"rij {rij}")
    if b:
        parts.append(f"bak {b}")
    if p:
        parts.append(f"plank {p}")
    return " · ".join(parts) if parts else "(geen locatie in extract)"


def load_excel() -> pd.DataFrame:
    df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    if cols[0].startswith("Unnamed") or cols[0].strip() == "":
        df = df.rename(columns={cols[0]: "Handle"})
    df.columns = [c.strip() for c in df.columns]
    df["tag"] = df["Tags"].astype(str).str.strip().replace("", "(leeg)")
    df["sku"] = df["Variant SKU"].astype(str).str.strip()
    df["sku_key"] = df["sku"].map(lambda s: sku_key(s) if s else "")
    df["title"] = df["titel"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["Variant Inventory Qty"], errors="coerce").fillna(0).astype(int)
    return df


def pick_examples(df: pd.DataFrame) -> dict:
    with_sku = df[df["sku_key"] != ""]
    groups = []
    for key, g in with_sku.groupby("sku_key"):
        tags = sorted(set(g["tag"]))
        if len(tags) < 2:
            continue
        by_tag = {}
        for t, tg in g.groupby("tag"):
            title = next((x for x in tg["title"] if x and sku_key(x) != key), "") or tg["title"].iloc[0]
            by_tag[t] = {
                "qty": int(tg["qty"].sum()),
                "title": title[:80],
                "sku": tg["sku"].iloc[0],
                "loc": loc_label(tg.iloc[0]),
                "n_rows": int(len(tg)),
            }
        qtys = {t: by_tag[t]["qty"] for t in by_tag}
        if len(set(qtys.values())) <= 1:
            continue
        # prefer partstream with a real shelf location
        loc = ""
        if "Partstream, stock" in by_tag:
            loc = by_tag["Partstream, stock"]["loc"]
        groups.append(
            {
                "sku_key": key,
                "sku": g["sku"].iloc[0],
                "has_shelf": "bak " in loc,
                "has_partstream": "Partstream, stock" in by_tag,
                "qty_spread": max(qtys.values()) - min(qtys.values()),
                "by_tag": by_tag,
                "best_title": next(
                    (
                        by_tag[t]["title"]
                        for t in ["ejis", "Winkelvoorraad, stock", "(leeg)", "Partstream, stock", "stock"]
                        if t in by_tag and by_tag[t]["title"] and sku_key(by_tag[t]["title"]) != key
                    ),
                    g["title"].iloc[0],
                ),
                "loc": loc,
            }
        )

    shelf = [x for x in groups if x["has_shelf"]]
    shelf.sort(key=lambda x: -x["qty_spread"])
    wv = [
        x
        for x in groups
        if "Winkelvoorraad, stock" in x["by_tag"] and "ejis" in x["by_tag"] and not x["has_shelf"]
    ]
    wv.sort(key=lambda x: -x["qty_spread"])
    ejis_pos_other_zero = [
        x
        for x in groups
        if x["by_tag"].get("ejis", {}).get("qty", 0) > 0
        and all(v["qty"] == 0 for k, v in x["by_tag"].items() if k != "ejis")
        and x["has_partstream"]
    ]
    equal = []
    for key, g in with_sku.groupby("sku_key"):
        tags = sorted(set(g["tag"]))
        if "ejis" not in tags or "Partstream, stock" not in tags:
            continue
        eq = int(g.loc[g["tag"] == "ejis", "qty"].sum())
        pq = int(g.loc[g["tag"] == "Partstream, stock", "qty"].sum())
        if eq == pq and eq > 0:
            ps = g[g["tag"] == "Partstream, stock"].iloc[0]
            loc = loc_label(ps)
            if "bak " not in loc:
                continue
            title = g.loc[g["tag"] == "ejis", "title"].iloc[0]
            equal.append(
                {
                    "sku": g["sku"].iloc[0],
                    "qty": eq,
                    "title": title[:80],
                    "loc": loc,
                }
            )
            if len(equal) >= 5:
                break

    def slim(x, n_tags=None):
        return {
            "sku": x["sku"],
            "title": x["best_title"][:80],
            "locatie": x["loc"],
            "bronnen": {
                t: {"aantal": v["qty"], "titel": v["title"][:50]}
                for t, v in x["by_tag"].items()
            },
        }

    return {
        "stelling_verschil": [slim(x) for x in shelf[:8]],
        "winkelvoorraad_vs_ejis": [slim(x) for x in wv[:6]],
        "ejis_heeft_winkel_nul": [slim(x) for x in ejis_pos_other_zero[:5]],
        "stelling_eens": equal[:5],
    }


def gql_ok(body: dict, key: str):
    if body.get("errors"):
        raise RuntimeError(json.dumps(body["errors"])[:1500])
    return (body.get("data") or {}).get(key)


def fetch_locations(sess, url, token) -> list[dict]:
    q = """
    { locations(first: 50, includeLegacy: true) {
        nodes { id name isActive fulfillsOnlineOrders }
    } }
    """
    data = _gql(sess, url, token, q)
    return gql_ok(data, "locations")["nodes"]


def fetch_negative_variants(sess, url, token, limit: int = 250) -> list[dict]:
    q = """
    query Neg($c: String) {
      productVariants(first: 50, after: $c, query: "inventory_quantity:<0") {
        pageInfo { hasNextPage endCursor }
        nodes {
          sku
          displayName
          inventoryQuantity
          product { title handle status }
          inventoryItem {
            tracked
            inventoryLevels(first: 10) {
              nodes {
                location { name }
                quantities(names: ["on_hand", "available", "committed", "incoming", "reserved"]) {
                  name
                  quantity
                }
              }
            }
          }
        }
      }
    }
    """
    rows = []
    c = None
    while True:
        body = _gql(sess, url, token, q, {"c": c})
        conn = gql_ok(body, "productVariants")
        for n in conn["nodes"]:
            levels = []
            for lv in (((n.get("inventoryItem") or {}).get("inventoryLevels") or {}).get("nodes") or []):
                qm = {x["name"]: x["quantity"] for x in (lv.get("quantities") or [])}
                levels.append({"location": (lv.get("location") or {}).get("name"), **qm})
            rows.append(
                {
                    "sku": n.get("sku") or "",
                    "title": ((n.get("product") or {}).get("title") or n.get("displayName") or "")[:80],
                    "handle": (n.get("product") or {}).get("handle"),
                    "status": (n.get("product") or {}).get("status"),
                    "available_total": n.get("inventoryQuantity"),
                    "levels": levels,
                }
            )
            if len(rows) >= limit:
                return rows
        if not conn["pageInfo"]["hasNextPage"]:
            break
        c = conn["pageInfo"]["endCursor"]
    return rows


def fetch_committed_at_location(sess, url, token, location_id: str, limit: int = 400) -> list[dict]:
    """Paginate inventoryLevels; keep committed>0 or available<0."""
    q = """
    query Levels($id: ID!, $c: String) {
      location(id: $id) {
        name
        inventoryLevels(first: 80, after: $c) {
          pageInfo { hasNextPage endCursor }
          nodes {
            quantities(names: ["on_hand", "available", "committed", "incoming"]) {
              name quantity
            }
            item {
              sku
              variant { sku displayName product { title handle } }
            }
          }
        }
      }
    }
    """
    hits = []
    scanned = 0
    c = None
    while True:
        body = _gql(sess, url, token, q, {"id": location_id, "c": c})
        loc = gql_ok(body, "location")
        if not loc:
            raise RuntimeError(json.dumps(body)[:1500])
        conn = loc["inventoryLevels"]
        for n in conn["nodes"]:
            scanned += 1
            qm = {x["name"]: x["quantity"] for x in (n.get("quantities") or [])}
            comm = int(qm.get("committed") or 0)
            avail = int(qm.get("available") or 0)
            if comm <= 0 and avail >= 0:
                continue
            item = n.get("item") or {}
            var = item.get("variant") or {}
            prod = var.get("product") or {}
            hits.append(
                {
                    "sku": (var.get("sku") or item.get("sku") or ""),
                    "title": (prod.get("title") or var.get("displayName") or "")[:80],
                    "handle": prod.get("handle"),
                    "on_hand": int(qm.get("on_hand") or 0),
                    "available": avail,
                    "committed": comm,
                    "incoming": int(qm.get("incoming") or 0),
                }
            )
            if len(hits) >= limit:
                return hits, scanned
        if not conn["pageInfo"]["hasNextPage"]:
            break
        c = conn["pageInfo"]["endCursor"]
        if scanned >= 8000:
            # enough signal without scanning entire catalog
            break
    return hits, scanned


def main() -> None:
    df = load_excel()
    examples = pick_examples(df)
    print("EXAMPLES stelling", len(examples["stelling_verschil"]))
    for x in examples["stelling_verschil"][:5]:
        print(" ", x["sku"], x["title"][:40], x["locatie"], x["bronnen"])

    domain, token, api = load_motox_env()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    sess = _session()
    locs = fetch_locations(sess, url, token)
    print("MOTOX LOCATIONS")
    for L in locs:
        print(" ", L["name"], "active=" + str(L["isActive"]), L["id"])

    pos = next((L for L in locs if L["name"] == "Winkelvoorraad POS"), None)
    mx = next((L for L in locs if "MotoX" in (L["name"] or "") or L["name"] == "MotoXshop"), None)

    negs = fetch_negative_variants(sess, url, token, limit=200)
    print(f"NEGATIVE variants {len(negs)}")
    for n in negs[:8]:
        print(" ", n["sku"], n["available_total"], n["levels"])

    comm_hits, scanned = ([], 0)
    if pos:
        comm_hits, scanned = fetch_committed_at_location(sess, url, token, pos["id"])
        print(f"POS committed/neg scanned={scanned} hits={len(comm_hits)}")
        for h in comm_hits[:8]:
            print(" ", h)

    # KTM negatives too? lighter
    load_dotenv(ROOT / ".env", override=True)
    import os
    import requests

    ktm_token = os.environ["SHOPIFY_ACCESS_TOKEN"]
    ktm_domain = os.environ["SHOPIFY_SHOP_DOMAIN"]
    ktm_api = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "2024-10")
    ktm_url = f"https://{ktm_domain}/admin/api/{ktm_api}/graphql.json"
    ksess = requests.Session()
    ksess.trust_env = False

    def kgql(query, variables=None):
        r = ksess.post(
            ktm_url,
            headers={"Content-Type": "application/json", "X-Shopify-Access-Token": ktm_token},
            json={"query": query, "variables": variables or {}},
            timeout=60,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        return r.json()

    ktm_locs = gql_ok(kgql("{ locations(first: 50, includeLegacy: true) { nodes { id name isActive } } }"), "locations")["nodes"]
    print("KTM LOCATIONS")
    for L in ktm_locs:
        print(" ", L["name"], L["id"])

    ktm_negs = []
    c = None
    qn = """
    query Neg($c: String) {
      productVariants(first: 50, after: $c, query: "inventory_quantity:<0") {
        pageInfo { hasNextPage endCursor }
        nodes {
          sku inventoryQuantity
          product { title handle }
          inventoryItem {
            inventoryLevels(first: 8) {
              nodes {
                location { name }
                quantities(names: ["on_hand","available","committed"]) { name quantity }
              }
            }
          }
        }
      }
    }
    """
    while len(ktm_negs) < 80:
        body = kgql(qn, {"c": c})
        conn = gql_ok(body, "productVariants")
        for n in conn["nodes"]:
            levels = []
            for lv in (((n.get("inventoryItem") or {}).get("inventoryLevels") or {}).get("nodes") or []):
                qm = {x["name"]: x["quantity"] for x in (lv.get("quantities") or [])}
                levels.append({"location": (lv.get("location") or {}).get("name"), **qm})
            ktm_negs.append(
                {
                    "sku": n.get("sku") or "",
                    "title": ((n.get("product") or {}).get("title") or "")[:80],
                    "available_total": n.get("inventoryQuantity"),
                    "levels": levels,
                }
            )
        if not conn["pageInfo"]["hasNextPage"]:
            break
        c = conn["pageInfo"]["endCursor"]
    print(f"KTM NEGATIVE {len(ktm_negs)}")

    report = {
        "examples": examples,
        "motox_locations": locs,
        "ktm_locations": ktm_locs,
        "motox_negative_variants": {
            "count_fetched": len(negs),
            "sample": negs[:15],
        },
        "motox_pos_committed_or_neg": {
            "scanned_levels": scanned,
            "hits": len(comm_hits),
            "sample": comm_hits[:20],
        },
        "ktm_negative_variants": {
            "count_fetched": len(ktm_negs),
            "sample": ktm_negs[:10],
        },
    }
    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    print("Wrote", OUT)


if __name__ == "__main__":
    main()
