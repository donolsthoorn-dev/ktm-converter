#!/usr/bin/env python3
"""
Vervang-matrix (0150) vs Shopify-variantcache.

Leest `input/replacement_articles_0150_csv.csv` (kolommen ArticleNumber → oud,
ArticleNumberReplace → nieuw). Vergelijkt met SKU's uit de variant-cache
(`shopify_refresh_variant_cache.py`).

Schrijft een CSV met alleen rijen waar het **oude** artikelnummer nog als variant-SKU
in Shopify voorkomt:
  kolom 1 — oud artikelnummer
  kolom 2 — vervangend artikelnummer (uit de matrix)
  kolom 3 — ja / nee of het vervangende artikel **ook** als eigen SKU in de shop staat
  kolom 4 — active / draft / archived (Shopify-productstatus van het vervangende artikel;
            leeg als kolom 3 = nee; bij dezelfde SKU op meerdere producten: waarden gecombineerd)

SKU-normalisatie: strip + uppercase (zelfde als sync-scripts).

Vereist: SHOPIFY_ACCESS_TOKEN in .env (live productlijst voor status kolom 4).

Vooraf cache verversen:
  python3 scripts/shopify_refresh_variant_cache.py

  python3 scripts/shopify_report_replacement_overlap.py
  python3 scripts/shopify_report_replacement_overlap.py -o output/replacement_shopify_overlap.csv
"""

from __future__ import annotations

import argparse
import csv
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
_DEFAULT_REPLACEMENT = PROJECT_ROOT / "input" / "replacement_articles_0150_csv.csv"
_DEFAULT_OUT = PROJECT_ROOT / "output" / "replacement_shopify_overlap.csv"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(PROJECT_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

import config  # noqa: E402
import shopify_sync_from_pricelist_csv as sync  # noqa: E402

_REQUEST_TIMEOUT = (12, 120)


def load_dotenv(path: Path | None = None) -> None:
    path = path or (PROJECT_ROOT / ".env")
    if not path.is_file():
        return
    try:
        with open(path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
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


def fetch_product_id_status_map(
    shop: str,
    token: str,
    api_version: str,
    only_product_ids: frozenset[str] | None = None,
) -> dict[str, str]:
    """
    product_id (str) -> status (active|draft|archived, lowercase).

    Als only_product_ids is gezet: stopt zodra alle gevraagde id's gevonden zijn
    (sneller bij grote catalogus).
    """
    sess = _http_session()
    headers = {"X-Shopify-Access-Token": token}
    out: dict[str, str] = {}
    missing: set[str] | None = set(only_product_ids) if only_product_ids else None
    url = (
        f"https://{shop}/admin/api/{api_version}/products.json"
        "?limit=250&fields=id,status"
    )
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
            pid_s = str(int(pid)) if isinstance(pid, (int, float)) else str(pid)
            if missing is not None and pid_s not in missing:
                continue
            st = (p.get("status") or "").strip().lower()
            if st:
                out[pid_s] = st
                if missing is not None:
                    missing.discard(pid_s)
        print(f"  Producten (status): {len(out)} …", flush=True)
        if missing is not None and not missing:
            break
        url = _next_page_url(r.headers.get("Link"))
        time.sleep(0.5)
    return out


def status_for_replacement_sku(
    new_sku: str,
    sku_to_vp: dict[str, list[tuple[str, str | None]]],
    pid_to_status: dict[str, str],
) -> str:
    """Status van het product (of meerdere bij dubbele SKU) voor vervangende SKU."""
    pairs = sku_to_vp.get(new_sku, [])
    pids = [pid for _, pid in pairs if pid]
    if not pids:
        return ""
    statuses: list[str] = []
    for pid in pids:
        st = pid_to_status.get(pid)
        if st:
            statuses.append(st)
    if not statuses:
        return ""
    uniq = sorted(set(statuses))
    return uniq[0] if len(uniq) == 1 else "+".join(uniq)


def read_replacement_rows(path: Path) -> list[tuple[str, str]]:
    """(oud_sku, nieuw_sku) in volgorde van het bestand; lege waarden worden overgeslagen."""
    with open(path, encoding="utf-8", errors="replace") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"
        reader = csv.reader(f, dialect)
        header = next(reader, None)
        if not header:
            return []
        h = [c.strip().strip('"') for c in header]
        try:
            i_old = h.index("ArticleNumber")
            i_new = h.index("ArticleNumberReplace")
        except ValueError:
            if len(h) >= 2:
                i_old, i_new = 0, 1
            else:
                return []
        out: list[tuple[str, str]] = []
        for row in reader:
            if len(row) <= max(i_old, i_new):
                continue
            old = (row[i_old] or "").strip().upper()
            new = (row[i_new] or "").strip().upper()
            if not old or not new:
                continue
            out.append((old, new))
    return out


def main() -> int:
    load_dotenv()

    p = argparse.ArgumentParser(
        description="Rapport: oude vervangen SKU's nog in Shopify + of vervanger apart bestaat."
    )
    p.add_argument(
        "--replacement-csv",
        type=Path,
        default=_DEFAULT_REPLACEMENT,
        help=f"Vervang-matrix (default: {_DEFAULT_REPLACEMENT})",
    )
    p.add_argument(
        "--variant-cache",
        type=Path,
        default=sync.DEFAULT_VARIANT_CACHE,
        help=f"SKU→variant JSON (default: {sync.DEFAULT_VARIANT_CACHE})",
    )
    p.add_argument(
        "-o",
        "--output",
        type=Path,
        default=_DEFAULT_OUT,
        help=f"Uitvoer-CSV (default: {_DEFAULT_OUT})",
    )
    args = p.parse_args()

    rep_path = args.replacement_csv.resolve()
    if not rep_path.is_file():
        print(f"Bestand ontbreekt: {rep_path}", file=sys.stderr)
        return 1

    cache_path = args.variant_cache.resolve()
    if not cache_path.is_file():
        print(
            f"Variant-cache ontbreekt: {cache_path}\n"
            "  Leg aan met: python3 scripts/shopify_refresh_variant_cache.py",
            file=sys.stderr,
        )
        return 1

    token = (config.SHOPIFY_ACCESS_TOKEN or "").strip()
    shop = (config.SHOPIFY_SHOP_DOMAIN or "").strip()
    api_ver = (config.SHOPIFY_ADMIN_API_VERSION or "2024-10").strip()
    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env) — nodig voor productstatus.", file=sys.stderr)
        return 1
    if not shop:
        print("SHOPIFY_SHOP_DOMAIN ontbreekt (.env).", file=sys.stderr)
        return 1

    rows = read_replacement_rows(rep_path)
    sku_to_vp = sync.load_variant_cache(cache_path)
    shop_skus = frozenset(sku_to_vp.keys())

    # Unieke (oud, nieuw) in eerste-volgorde
    seen: set[tuple[str, str]] = set()
    pending: list[tuple[str, str, str]] = []  # old, new, ja|nee
    n_old_in_shop = 0
    n_new_ja = 0
    n_new_nee = 0

    for old_sku, new_sku in rows:
        key = (old_sku, new_sku)
        if key in seen:
            continue
        seen.add(key)
        if old_sku not in shop_skus:
            continue
        n_old_in_shop += 1
        has_new = new_sku in shop_skus
        if has_new:
            n_new_ja += 1
            flag = "ja"
        else:
            n_new_nee += 1
            flag = "nee"
        pending.append((old_sku, new_sku, flag))

    needed_pids: set[str] = set()
    for _, new_sku, flag in pending:
        if flag != "ja":
            continue
        for _, pid in sku_to_vp.get(new_sku, []):
            if pid:
                needed_pids.add(pid)

    pid_to_status: dict[str, str] = {}
    if needed_pids:
        print(
            f"Shopify: productstatus ophalen ({len(needed_pids)} unieke producten, REST)…",
            flush=True,
        )
        pid_to_status = fetch_product_id_status_map(
            shop, token, api_ver, frozenset(needed_pids)
        )

    out_rows: list[tuple[str, str, str, str]] = []
    for old_sku, new_sku, flag in pending:
        if flag == "nee":
            ps = ""
        else:
            ps = status_for_replacement_sku(new_sku, sku_to_vp, pid_to_status)
        out_rows.append((old_sku, new_sku, flag, ps))

    out_path = args.output.resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(
            [
                "Oud_artikelnummer",
                "Vervangend_artikelnummer",
                "Vervanger_staat_apart_in_shop",
                "Vervanger_productstatus",
            ]
        )
        w.writerows(out_rows)

    print(
        f"Vervang-regels in bron (unieke oude→nieuw): {len(seen)}\n"
        f"Rijen met oud nog in Shopify:              {n_old_in_shop}\n"
        f"  daarvan vervanger ook als SKU in shop:   {n_new_ja} (ja)\n"
        f"  daarvan vervanger nog niet als SKU:      {n_new_nee} (nee)\n"
        f"Geschreven: {out_path}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
