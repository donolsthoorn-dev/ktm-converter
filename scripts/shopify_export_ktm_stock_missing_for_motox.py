#!/usr/bin/env python3
"""
Exporteer KTM-producten (met voorraad) die nog ontbreken in MotoXshop
als Shopify product-CSV (inclusief Image Src-URL's).

  python3 scripts/shopify_export_ktm_stock_missing_for_motox.py

Input:  output/ktm_stock_missing_in_motox.txt
Output: output/motox_import_ktm_winkelvoorraad_<timestamp>.csv

Vereist: SHOPIFY_ACCESS_TOKEN + SHOPIFY_SHOP_DOMAIN voor ktm-shop.nl (.env).
"""

from __future__ import annotations

import csv
import os
import sys
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

import config  # noqa: E402
import requests

SKU_FILE = ROOT / "output" / "ktm_stock_missing_in_motox.txt"
OUT_DIR = ROOT / "output"

CSV_FIELDS = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Product Category",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "Variant Barcode",
    "Image Src",
    "Image Position",
    "Image Alt Text",
    "Gift Card",
    "SEO Title",
    "SEO Description",
    "Status",
    "Variant Weight Unit",
    "Cost per item",
]

QUERY = """
query ($c: String) {
  productVariants(first: 50, after: $c, query: "inventory_quantity:>0") {
    pageInfo { hasNextPage endCursor }
    nodes {
      sku
      barcode
      price
      compareAtPrice
      inventoryQuantity
      inventoryPolicy
      taxable
      inventoryItem {
        measurement {
          weight { value unit }
        }
      }
      selectedOptions { name value }
      image { url altText }
      product {
        handle
        title
        descriptionHtml
        vendor
        productType
        tags
        status
        category { fullName }
        seo { title description }
        featuredImage { url altText }
        images(first: 20) {
          nodes { url altText }
        }
      }
    }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def gql(sess: requests.Session, query: str, variables: dict[str, Any] | None = None) -> dict[str, Any]:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
    )
    token = config.SHOPIFY_ACCESS_TOKEN
    for attempt in range(10):
        r = sess.post(
            url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=120,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        payload = r.json()
        if payload.get("errors"):
            if "throttl" in str(payload["errors"]).lower():
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(payload["errors"][:3])
        cost = ((payload.get("extensions") or {}).get("cost") or {}).get(
            "throttleStatus"
        ) or {}
        if (cost.get("currentlyAvailable") or 2000) < 50:
            time.sleep(2)
        return payload["data"]
    raise RuntimeError("GraphQL throttle")


def weight_to_grams(weight: float | None, unit: str | None) -> int:
    if weight is None:
        return 0
    u = (unit or "GRAMS").upper()
    if u == "KILOGRAMS":
        return int(round(weight * 1000))
    if u == "OUNCES":
        return int(round(weight * 28.3495))
    if u == "POUNDS":
        return int(round(weight * 453.592))
    return int(round(weight))


def main() -> int:
    if not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", flush=True)
        return 1
    if not SKU_FILE.exists():
        print(f"SKU-lijst ontbreekt: {SKU_FILE}", flush=True)
        return 1

    missing = {
        line.strip().upper()
        for line in SKU_FILE.read_text(encoding="utf-8").splitlines()
        if line.strip()
    }
    print(f"Doel-SKU’s: {len(missing)}", flush=True)

    # product_handle -> {meta, variants[], images[]}
    products: dict[str, dict[str, Any]] = {}
    found: set[str] = set()
    sess = _session()
    cursor = None
    pages = 0

    while True:
        data = gql(sess, QUERY, {"c": cursor})
        conn = data["productVariants"]
        for node in conn["nodes"]:
            sku = (node.get("sku") or "").strip().upper()
            if sku not in missing:
                continue
            prod = node["product"]
            handle = prod["handle"]
            if handle not in products:
                imgs = []
                for im in (prod.get("images") or {}).get("nodes") or []:
                    url = (im.get("url") or "").strip()
                    if url:
                        imgs.append(
                            {"url": url, "alt": (im.get("altText") or "") or ""}
                        )
                if not imgs and prod.get("featuredImage"):
                    fi = prod["featuredImage"]
                    if fi.get("url"):
                        imgs.append(
                            {
                                "url": fi["url"],
                                "alt": (fi.get("altText") or "") or "",
                            }
                        )
                cat = prod.get("category") or {}
                seo = prod.get("seo") or {}
                tags = list(prod.get("tags") or [])
                if "ktm-winkelvoorraad-pos" not in tags:
                    tags.append("ktm-winkelvoorraad-pos")
                if "KTM-clone-not-published" not in tags:
                    tags.append("KTM-clone-not-published")
                products[handle] = {
                    "title": prod.get("title") or handle,
                    "body": prod.get("descriptionHtml") or "",
                    "vendor": prod.get("vendor") or "",
                    "type": prod.get("productType") or "",
                    "category": (cat.get("fullName") or "") if cat else "",
                    "tags": ", ".join(tags),
                    "status": (prod.get("status") or "ACTIVE").lower(),
                    "seo_title": seo.get("title") or "",
                    "seo_description": seo.get("description") or "",
                    "images": imgs,
                    "variants": [],
                }
            opts = node.get("selectedOptions") or []
            o1 = opts[0] if len(opts) > 0 else {"name": "Title", "value": "Default Title"}
            o2 = opts[1] if len(opts) > 1 else {"name": "", "value": ""}
            o3 = opts[2] if len(opts) > 2 else {"name": "", "value": ""}
            vimg = node.get("image") or {}
            wnode = (
                ((node.get("inventoryItem") or {}).get("measurement") or {}).get(
                    "weight"
                )
                or {}
            )
            wunit = (wnode.get("unit") or "GRAMS").upper()
            products[handle]["variants"].append(
                {
                    "sku": (node.get("sku") or "").strip(),
                    "barcode": (node.get("barcode") or "") or "",
                    "price": node.get("price") or "0.00",
                    "compare": node.get("compareAtPrice") or "",
                    "qty": int(node.get("inventoryQuantity") or 0),
                    "policy": (
                        "continue"
                        if (node.get("inventoryPolicy") or "").upper() == "CONTINUE"
                        else "deny"
                    ),
                    "taxable": "TRUE" if node.get("taxable") else "FALSE",
                    "grams": weight_to_grams(wnode.get("value"), wunit),
                    "weight_unit": {
                        "GRAMS": "g",
                        "KILOGRAMS": "kg",
                        "OUNCES": "oz",
                        "POUNDS": "lb",
                    }.get(wunit, "g"),
                    "opt1_name": o1.get("name") or "Title",
                    "opt1_value": o1.get("value") or "Default Title",
                    "opt2_name": o2.get("name") or "",
                    "opt2_value": o2.get("value") or "",
                    "opt3_name": o3.get("name") or "",
                    "opt3_value": o3.get("value") or "",
                    "variant_image": (vimg.get("url") or "") if vimg else "",
                }
            )
            found.add(sku)

        pages += 1
        if pages % 20 == 0:
            print(
                f"  pagina {pages}: producten={len(products)} SKU’s={len(found)}/{len(missing)}",
                flush=True,
            )
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]

    not_found = sorted(missing - found)
    print(
        f"Klaar: {len(found)} SKU’s in {len(products)} producten; niet gevonden: {len(not_found)}",
        flush=True,
    )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = OUT_DIR / f"motox_import_ktm_winkelvoorraad_{ts}.csv"
    missing_out = OUT_DIR / f"motox_import_ktm_winkelvoorraad_{ts}_not_found.txt"

    rows: list[dict[str, str]] = []
    for handle, meta in sorted(products.items(), key=lambda x: x[0]):
        variants = meta["variants"]
        # dedupe sku within product
        seen_sku: set[str] = set()
        uniq_vars = []
        for v in variants:
            key = v["sku"].upper()
            if key in seen_sku:
                continue
            seen_sku.add(key)
            uniq_vars.append(v)
        images = meta["images"]
        for i, v in enumerate(uniq_vars):
            row = {f: "" for f in CSV_FIELDS}
            row["Handle"] = handle
            if i == 0:
                row["Title"] = meta["title"]
                row["Body (HTML)"] = meta["body"]
                row["Vendor"] = meta["vendor"]
                row["Product Category"] = meta["category"]
                row["Type"] = meta["type"]
                row["Tags"] = meta["tags"]
                row["Published"] = "FALSE"
                row["Gift Card"] = "FALSE"
                row["SEO Title"] = meta["seo_title"]
                row["SEO Description"] = meta["seo_description"]
                row["Status"] = "active" if meta["status"] == "active" else "draft"
                if images:
                    row["Image Src"] = images[0]["url"]
                    row["Image Position"] = "1"
                    row["Image Alt Text"] = images[0]["alt"]
            row["Option1 Name"] = v["opt1_name"]
            row["Option1 Value"] = v["opt1_value"]
            row["Option2 Name"] = v["opt2_name"]
            row["Option2 Value"] = v["opt2_value"]
            row["Option3 Name"] = v["opt3_name"]
            row["Option3 Value"] = v["opt3_value"]
            row["Variant SKU"] = v["sku"]
            row["Variant Grams"] = str(v["grams"])
            row["Variant Inventory Tracker"] = "shopify"
            row["Variant Inventory Qty"] = str(v["qty"])
            row["Variant Inventory Policy"] = v["policy"]
            row["Variant Fulfillment Service"] = "manual"
            row["Variant Price"] = str(v["price"])
            row["Variant Compare At Price"] = str(v["compare"] or "")
            row["Variant Requires Shipping"] = "TRUE"
            row["Variant Taxable"] = v["taxable"]
            row["Variant Barcode"] = v["barcode"]
            row["Variant Weight Unit"] = v["weight_unit"]
            if v["variant_image"] and i > 0:
                # variant-specific image on variant row
                row["Image Src"] = v["variant_image"]
            rows.append(row)

        # Extra image rows (Shopify CSV: handle + Image Src/Position only)
        for pos, im in enumerate(images[1:], start=2):
            row = {f: "" for f in CSV_FIELDS}
            row["Handle"] = handle
            row["Image Src"] = im["url"]
            row["Image Position"] = str(pos)
            row["Image Alt Text"] = im["alt"]
            rows.append(row)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    if not_found:
        missing_out.write_text("\n".join(not_found) + "\n", encoding="utf-8")

    print(f"CSV: {out}", flush=True)
    print(f"Rijen: {len(rows)}", flush=True)
    if not_found:
        print(f"Niet gevonden SKU’s: {missing_out}", flush=True)
    print(
        "Import in MotoXshop: Producten → Importeren. "
        "Published=FALSE; daarna POS-kanaal aan + voorraadlocatie Winkelvoorraad POS checken.",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
