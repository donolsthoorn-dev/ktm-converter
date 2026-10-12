"""Bouw de fitment-index voor het collectiefilter.

Haalt van elk Motox-product het id, de tags en global.fits_on op en schrijft
cache/motox/fitment_index.sqlite. POS-kopieën zonder fits_on komen in de
uitsluitlijst. De motorenlijst voor de kiezer blijft een aparte export.
"""

from __future__ import annotations

import json
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
    load_motox_env,
)

BULK_FILE = ROOT / "cache" / "motox" / "fitment_bulk.jsonl"
INDEX_FILE = ROOT / "cache" / "motox" / "fitment_index.sqlite"

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


def fetch_bulk(dest: Path) -> None:
    domain, token, api = load_motox_env()
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
    print("Fitment-export gestart...", flush=True)
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
            with sess.get(download, stream=True, timeout=_TIMEOUT, proxies={"http": None, "https": None}) as response:
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
