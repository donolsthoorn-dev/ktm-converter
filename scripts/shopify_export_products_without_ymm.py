#!/usr/bin/env python3
"""
Exporteer Shopify-producten die géén YMM-data hebben in de huidige YMM update_csv.

Vergelijking:
- YMM-bron: `input/YMM-*-update_csv.csv` (kolom `Product Ids`)
- Shopify: alle producten + varianten via REST API

Output (default): `output/ymm/shopify_producten_zonder_ymm.csv`

Voorbeelden:
  python3 scripts/shopify_export_products_without_ymm.py
  python3 scripts/shopify_export_products_without_ymm.py --refresh
  python3 scripts/shopify_export_products_without_ymm.py --out output/ymm/zonder_ymm.csv
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
API_VER = config.SHOPIFY_ADMIN_API_VERSION

INPUT_DIR = PROJECT_ROOT / "input"
YMM_DIR = PROJECT_ROOT / "output" / "ymm"
DEFAULT_OUT = YMM_DIR / "shopify_producten_zonder_ymm.csv"
_REQUEST_TIMEOUT = (12, 120)


def load_dotenv(path: Path | None = None) -> None:
    path = path or (PROJECT_ROOT / ".env")
    if not path.is_file():
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                if not key:
                    continue
                val = val.strip()
                if (val.startswith('"') and val.endswith('"')) or (
                    val.startswith("'") and val.endswith("'")
                ):
                    val = val[1:-1]
                if key not in os.environ:
                    os.environ[key] = val
    except OSError:
        return


def _http_session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def _next_page_url(link_header: str | None) -> str | None:
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' in part:
            url = part.split(";")[0].strip()
            return url.replace("<", "").replace(">", "")
    return None


def _latest_existing_update_csv() -> str:
    files = sorted(glob.glob(str(INPUT_DIR / "YMM-*-update_csv.csv")), key=os.path.getmtime)
    return files[-1] if files else ""


def _norm_product_id(value: str) -> str:
    return (value or "").strip().replace("~", "")


def _detect_csv_delimiter(path: str) -> str:
    try:
        with open(path, encoding="utf-8-sig", newline="") as f:
            sample = f.read(4096)
    except OSError:
        return ";"
    if not sample.strip():
        return ";"
    try:
        return csv.Sniffer().sniff(sample, delimiters=",;\t").delimiter
    except csv.Error:
        return ";"


def read_ymm_product_ids(path: str) -> set[str]:
    ids: set[str] = set()
    delim = _detect_csv_delimiter(path)
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=delim)
        if not reader.fieldnames:
            return ids
        lower_to_col = {c.strip().lower(): c for c in reader.fieldnames if c}
        pid_col = lower_to_col.get("product ids")
        if not pid_col:
            raise ValueError(f"YMM CSV mist kolom 'Product Ids': {path}")
        for row in reader:
            pid = _norm_product_id(row.get(pid_col, ""))
            if pid:
                ids.add(pid)
    return ids


def fetch_products(shop: str, token: str, api_version: str) -> list[dict[str, str]]:
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    url = (
        f"https://{shop}/admin/api/{api_version}/products.json"
        "?limit=250&fields=id,handle,title,product_type,status"
    )
    out: list[dict[str, str]] = []
    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("REST rate limit (products), wachten...", flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout (products), retry...", flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()
        for p in data.get("products", []):
            pid = p.get("id")
            if pid is None:
                continue
            pid_s = str(int(pid)) if isinstance(pid, (int, float)) else str(pid).strip()
            out.append(
                {
                    "product_id": pid_s,
                    "handle": (p.get("handle") or "").strip(),
                    "title": (p.get("title") or "").strip(),
                    "type": (p.get("product_type") or "").strip(),
                    "status": (p.get("status") or "").strip(),
                }
            )
        print(f"  Producten… {len(out)}", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return out


def fetch_product_id_to_skus(shop: str, token: str, api_version: str) -> dict[str, set[str]]:
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    url = (
        f"https://{shop}/admin/api/{api_version}/variants.json"
        "?limit=250&fields=product_id,sku"
    )
    out: dict[str, set[str]] = {}
    while url:
        r = sess.get(
            url,
            headers=headers,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("REST rate limit (variants), wachten...", flush=True)
            time.sleep(2)
            continue
        if r.status_code >= 500:
            print("Shopify serverfout (variants), retry...", flush=True)
            time.sleep(3)
            continue
        r.raise_for_status()
        data = r.json()
        for v in data.get("variants", []):
            pid = v.get("product_id")
            if pid is None:
                continue
            pid_s = str(int(pid)) if isinstance(pid, (int, float)) else str(pid).strip()
            sku = (v.get("sku") or "").strip().upper()
            if pid_s not in out:
                out[pid_s] = set()
            if sku:
                out[pid_s].add(sku)
        print(f"  SKU-koppelingen… {sum(len(s) for s in out.values())}", flush=True)
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return out


def build_missing_rows(
    products: list[dict[str, str]],
    product_id_to_skus: dict[str, set[str]],
    product_ids_with_ymm: set[str],
) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for p in products:
        pid = p.get("product_id", "")
        if not pid or pid in product_ids_with_ymm:
            continue
        skus = sorted(product_id_to_skus.get(pid, set()))
        if not skus:
            rows.append(
                {
                    "SKU": "",
                    "Titel": p.get("title", ""),
                    "Type": p.get("type", ""),
                    "Product_id": pid,
                    "Handle": p.get("handle", ""),
                    "Status": p.get("status", ""),
                }
            )
            continue
        for sku in skus:
            rows.append(
                {
                    "SKU": sku,
                    "Titel": p.get("title", ""),
                    "Type": p.get("type", ""),
                    "Product_id": pid,
                    "Handle": p.get("handle", ""),
                    "Status": p.get("status", ""),
                }
            )
    rows.sort(key=lambda r: (r["Titel"].casefold(), r["SKU"], r["Product_id"]))
    return rows


def write_output_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["SKU", "Titel", "Type", "Product_id", "Handle", "Status"])
        for r in rows:
            writer.writerow(
                [
                    r.get("SKU", ""),
                    r.get("Titel", ""),
                    r.get("Type", ""),
                    r.get("Product_id", ""),
                    r.get("Handle", ""),
                    r.get("Status", ""),
                ]
            )


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(
        description="Exporteer Shopify-producten zonder YMM-data (op basis van YMM update_csv)."
    )
    ap.add_argument(
        "--ymm-existing",
        default=_latest_existing_update_csv(),
        help="Pad naar huidige YMM update_csv met kolom Product Ids.",
    )
    ap.add_argument(
        "--out",
        default=str(DEFAULT_OUT),
        help=f"Output CSV (default: {DEFAULT_OUT})",
    )
    ap.add_argument(
        "--refresh",
        action="store_true",
        help="Aanwezig voor CLI-consistentie; deze run haalt altijd live data op.",
    )
    args = ap.parse_args()

    if not TOKEN.strip():
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", flush=True)
        return 1

    ymm_path = (args.ymm_existing or "").strip()
    if not ymm_path or not os.path.exists(ymm_path):
        print("Geen YMM update_csv gevonden/meegegeven.", flush=True)
        return 1

    print(f"YMM update_csv inlezen: {ymm_path}", flush=True)
    product_ids_with_ymm = read_ymm_product_ids(ymm_path)
    print(f"Producten met YMM in bron: {len(product_ids_with_ymm)}", flush=True)

    print("Shopify: producten ophalen…", flush=True)
    products = fetch_products(SHOP, TOKEN, API_VER)
    print("Shopify: varianten (SKU-koppeling) ophalen…", flush=True)
    product_id_to_skus = fetch_product_id_to_skus(SHOP, TOKEN, API_VER)

    rows = build_missing_rows(products, product_id_to_skus, product_ids_with_ymm)
    out_path = Path(args.out)
    write_output_csv(out_path, rows)

    missing_products = len(
        {
            r.get("Product_id", "")
            for r in rows
            if (r.get("Product_id", "") or "").strip()
        }
    )
    print(f"Shopify producten totaal: {len(products)}", flush=True)
    print(f"Producten zonder YMM: {missing_products}", flush=True)
    print(f"CSV-rijen geschreven: {len(rows)}", flush=True)
    print(f"Output: {out_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
