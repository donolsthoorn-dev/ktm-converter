"""Bouw de KTM fitment-index uit live global.fits_on op ktm-shop.nl.

Gebruikt Shopify bulk export (zelfde pad als Motox). Nodig wanneer
canonical_product_fits_on in Supabase achterloopt op Shopifymetafields
(bijv. na Kutvek graphics-normalisatie).
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.motox_garage.fitment_index import FitmentIndex, iter_bulk_products  # noqa: E402
from modules.motox_shopify_cache import (  # noqa: E402
    _GQL_BULK_START,
    _GQL_POLL,
    _TIMEOUT,
    _gql,
    _session,
    _wait_current_bulk,
)

BULK_FILE = ROOT / "cache" / "ktm" / "fitment_bulk.jsonl"
INDEX_FILE = ROOT / "cache" / "ktm" / "fitment_index.sqlite"

_QUERY = """{
  products {
    edges {
      node {
        id
        tags
        metafield(namespace: "global", key: "fits_on") {
          value
        }
      }
    }
  }
}"""


def load_ktm_env() -> tuple[str, str, str]:
    """Force KTM (.env) credentials; clear Motox overrides."""
    for key in (
        "SHOPIFY_MOTOX_ACCESS_TOKEN",
        "SHOPIFY_MOTOX_SHOP_DOMAIN",
        "SHOPIFY_MOTOX_ADMIN_API_VERSION",
    ):
        os.environ.pop(key, None)
    env_path = ROOT / ".env"
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k, v = k.strip(), v.strip().strip('"').strip("'")
        if k in {
            "SHOPIFY_ACCESS_TOKEN",
            "SHOPIFY_SHOP_DOMAIN",
            "SHOPIFY_SHOP_SLUG",
            "SHOPIFY_ADMIN_API_VERSION",
        }:
            os.environ[k] = v
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
    domain = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or "").strip()
    api = (os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "2024-10").strip()
    if not token or not domain:
        raise SystemExit("KTM Shopify-creds ontbreken in .env")
    return domain, token, api


def fetch_bulk(dest: Path) -> None:
    domain, token, api = load_ktm_env()
    sess = _session()
    url = f"https://{domain}/admin/api/{api}/graphql.json"
    _wait_current_bulk(sess, url, token)
    query = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_QUERY))
    body = _gql(sess, url, token, query)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    if run.get("userErrors"):
        raise SystemExit(f"Bulk geweigerd: {run.get('userErrors')}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen bulk operation id.")
    print(f"Fitment-export gestart op {domain}...", flush=True)
    while True:
        poll = _gql(sess, url, token, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            download = node.get("url")
            if not download:
                raise SystemExit("Bulk klaar zonder URL.")
            dest.parent.mkdir(parents=True, exist_ok=True)
            with sess.get(
                download, stream=True, timeout=_TIMEOUT, proxies={"http": None, "https": None}
            ) as response:
                response.raise_for_status()
                with dest.open("wb") as handle:
                    for chunk in response.iter_content(1024 * 256):
                        if chunk:
                            handle.write(chunk)
            return
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(3)


def build_index(source: Path, dest: Path) -> FitmentIndex:
    index = FitmentIndex.create(dest)
    counts = {"fitted": 0, "excluded": 0, "universal": 0}
    pending = 0
    for product_id, tags, fits_on in iter_bulk_products(source):
        kind = index.add_product(product_id, tags, fits_on)
        counts[kind] += 1
        pending += 1
        if pending >= 2000:
            index.finish()
            pending = 0
    index.finish()
    print(
        f"{counts['fitted']} met fitment, {counts['excluded']} kassakopieën zonder fitment, "
        f"{counts['universal']} altijd zichtbaar",
        flush=True,
    )
    stored = index.counts()
    print(f"{stored['pairs']} motor-koppelingen → {dest}", flush=True)
    return index


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from-file", type=Path, default=None)
    parser.add_argument("-o", type=Path, default=INDEX_FILE)
    args = parser.parse_args()
    source = args.from_file
    if source is None:
        print(f"download → {BULK_FILE}", flush=True)
        fetch_bulk(BULK_FILE)
        source = BULK_FILE
    build_index(source, args.o)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
