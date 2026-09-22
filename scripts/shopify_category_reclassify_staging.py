#!/usr/bin/env python3
"""
Staging dry-run: huidige Shopify Category ↔ voorstel voor (bijna) alle producten.

Geen writes naar Shopify. De catalogus komt uit één Shopify bulk-export
(geen pagina-voor-pagina). Schrijft onder output/:
  - shopify_category_reclassify_staging_{shop}_{ts}.csv   (alles)
  - shopify_category_reclassify_changes_{shop}_{ts}.csv    (alleen verschillen)

  python3 scripts/shopify_category_reclassify_staging.py --shop ktm
  python3 scripts/shopify_category_reclassify_staging.py --shop ktm --limit 500
  python3 scripts/shopify_category_reclassify_staging.py --shop motox

Daarna mapper bijsturen op basis van changes-CSV; apply apart.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from collections import Counter
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

from modules.category_mapper import (  # noqa: E402
    is_missing_shopify_category,
    resolve_shopify_product_category,
)

_REQUEST_TIMEOUT = (15, 120)
_DOWNLOAD_TIMEOUT = (15, 300)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        productType
        tags
        descriptionHtml
        category { id fullName }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation CategoryStagingBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_CURRENT = """
query {
  currentBulkOperation {
    id
    status
    errorCode
    objectCount
  }
}
"""

_GQL_POLL = """
query CategoryStagingPoll($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
    }
  }
}
"""

FIELDS = [
    "product_id",
    "handle",
    "title",
    "status",
    "product_type",
    "tags",
    "current_category",
    "current_missing",
    "proposed_category",
    "proposed_bucket",
    "source",
    "action",  # keep | set | change
]


def _load_dotenv(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _shop_credentials(shop: str) -> tuple[str, str, str]:
    if shop == "motox":
        token = (
            os.environ.get("SHOPIFY_MOTOX_ACCESS_TOKEN")
            or os.environ.get("SHOPIFY_ACCESS_TOKEN")
            or ""
        ).strip()
        domain = (
            os.environ.get("SHOPIFY_MOTOX_SHOP_DOMAIN")
            or os.environ.get("SHOPIFY_SHOP_DOMAIN")
            or ""
        ).strip()
        api = (
            os.environ.get("SHOPIFY_MOTOX_ADMIN_API_VERSION")
            or os.environ.get("SHOPIFY_ADMIN_API_VERSION")
            or ""
        ).strip()
        file_env = _load_dotenv(ROOT / ".env.motox")
    else:
        token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
        domain = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or "").strip()
        api = (os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "").strip()
        file_env = _load_dotenv(ROOT / ".env")

    token = token or (file_env.get("SHOPIFY_ACCESS_TOKEN") or "").strip()
    domain = domain or (file_env.get("SHOPIFY_SHOP_DOMAIN") or "").strip()
    api = api or (file_env.get("SHOPIFY_ADMIN_API_VERSION") or "2024-10").strip()
    if not token or not domain:
        raise SystemExit(f"Shopify creds ontbreken voor shop={shop}")
    return domain, token, api


def _gql(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    query: str,
    variables: dict | None = None,
) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(10):
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
            if "Throttl" in str(body["errors"]):
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(body["errors"])
        return body
    raise RuntimeError("GraphQL failed after retries")


def _norm_cat(name: str | None) -> str:
    return (name or "").strip()


def _action(current: str, proposed: str) -> str:
    if is_missing_shopify_category(current):
        return "set"
    if _norm_cat(current) == _norm_cat(proposed):
        return "keep"
    return "change"


def _wait_current_bulk(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
) -> None:
    while True:
        body = _gql(sess, shop, token, api, _GQL_CURRENT)
        cur = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (cur.get("status") or "").upper()
        if not status or status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return
        print(
            f"  Wacht op lopende bulk: {status} ({cur.get('objectCount')})",
            flush=True,
        )
        time.sleep(5)


def _download_category_bulk(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    dest: Path,
) -> None:
    """Eén bulk-export van de catalogus. Shopify staat één bulk per shop toe."""
    _wait_current_bulk(sess, shop, token, api)
    mutation = _GQL_BULK_START.replace(
        "BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY)
    )
    op_id = ""
    for attempt in range(5):
        body = _gql(sess, shop, token, api, mutation)
        if body.get("errors"):
            raise RuntimeError(body["errors"])
        run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
        uerr = run.get("userErrors") or []
        if uerr:
            msg = str(uerr)
            if "already in progress" in msg.lower() and attempt < 4:
                print(f"  Bulk bezet, opnieuw proberen ({attempt + 1})", flush=True)
                time.sleep(8)
                _wait_current_bulk(sess, shop, token, api)
                continue
            raise RuntimeError(f"Bulk geweigerd: {uerr}")
        op_id = (run.get("bulkOperation") or {}).get("id") or ""
        if op_id:
            break
        time.sleep(3)
    if not op_id:
        raise RuntimeError("Geen bulk operation id.")

    print(f"Bulk-export gestart ({op_id})", flush=True)
    poll_interval = 3.0
    url = ""
    while True:
        poll = _gql(sess, shop, token, api, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url") or ""
            break
        if status in ("FAILED", "CANCELED", "CANCELLED", "EXPIRED"):
            raise RuntimeError(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)

    dest.parent.mkdir(parents=True, exist_ok=True)
    if not url:
        dest.write_text("", encoding="utf-8")
        print("Bulk klaar, lege catalogus", flush=True)
        return

    with sess.get(
        url,
        stream=True,
        timeout=_DOWNLOAD_TIMEOUT,
        proxies={"http": None, "https": None},
    ) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(1024 * 256):
                if chunk:
                    f.write(chunk)
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"Bulk gedownload: {dest.name} ({size_mb:.1f} MB)", flush=True)


def _iter_bulk_products(path: Path):
    """Productregels uit de JSONL. Geneste verbindingen hebben __parentId."""
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Ongeldige JSONL regel {line_no}: {exc}") from exc
            if obj.get("__parentId"):
                continue
            gid = str(obj.get("id") or "")
            if not gid.startswith("gid://shopify/Product/"):
                continue
            yield obj


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Stop met mappen na N producten (0=alle). De bulk haalt de hele catalogus op.",
    )
    args = ap.parse_args()

    shop_domain, token, api = _shop_credentials(args.shop)
    sess = requests.Session()
    sess.trust_env = False

    print(f"Staging reclassify dry-run shop={shop_domain} ({args.shop})", flush=True)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    staging_path = ROOT / "output" / f"shopify_category_reclassify_staging_{args.shop}_{stamp}.csv"
    changes_path = ROOT / "output" / f"shopify_category_reclassify_changes_{args.shop}_{stamp}.csv"
    bulk_path = ROOT / "output" / f".category_bulk_{args.shop}_{stamp}.jsonl"

    action_counts: Counter[str] = Counter()
    bucket_change: Counter[str] = Counter()
    source_change: Counter[str] = Counter()
    scanned = 0

    with staging_path.open("w", newline="", encoding="utf-8") as f_all, changes_path.open(
        "w", newline="", encoding="utf-8"
    ) as f_chg:
        w_all = csv.DictWriter(f_all, fieldnames=FIELDS)
        w_chg = csv.DictWriter(f_chg, fieldnames=FIELDS)
        w_all.writeheader()
        w_chg.writeheader()

        try:
            _download_category_bulk(sess, shop_domain, token, api, bulk_path)
            for p in _iter_bulk_products(bulk_path):
                scanned += 1
                current = _norm_cat(
                    (p.get("category") or {}).get("fullName") if p.get("category") else None
                )
                tags = p.get("tags") or []
                if isinstance(tags, str):
                    tags = [t.strip() for t in tags.split(",") if t.strip()]
                decision = resolve_shopify_product_category(
                    product_type=p.get("productType"),
                    tags=tags,
                    title=p.get("title"),
                    body_html=p.get("descriptionHtml"),
                    shop=args.shop,
                )
                action = _action(current, decision.path)
                row = {
                    "product_id": (p.get("id") or "").split("/")[-1],
                    "handle": p.get("handle") or "",
                    "title": p.get("title") or "",
                    "status": p.get("status") or "",
                    "product_type": p.get("productType") or "",
                    "tags": ", ".join(tags),
                    "current_category": current,
                    "current_missing": "1" if is_missing_shopify_category(current) else "0",
                    "proposed_category": decision.path,
                    "proposed_bucket": decision.bucket,
                    "source": decision.source,
                    "action": action,
                }
                w_all.writerow(row)
                action_counts[action] += 1
                if action in ("set", "change"):
                    w_chg.writerow(row)
                    bucket_change[decision.bucket] += 1
                    source_change[decision.source.split(":")[0]] += 1

                if scanned % 5000 == 0:
                    print(
                        f"  scanned={scanned} keep={action_counts['keep']} "
                        f"change={action_counts['change']} set={action_counts['set']}",
                        flush=True,
                    )
                if args.limit and scanned >= args.limit:
                    break
        finally:
            bulk_path.unlink(missing_ok=True)

    print(flush=True)
    print(f"Wrote {staging_path}", flush=True)
    print(f"Wrote {changes_path}", flush=True)
    print(f"Scanned: {scanned}", flush=True)
    print(
        f"Actions: keep={action_counts['keep']}  "
        f"change={action_counts['change']}  set={action_counts['set']}",
        flush=True,
    )
    print("Changes by proposed bucket (top 15):", flush=True)
    for b, n in bucket_change.most_common(15):
        print(f"  {n:6}  {b}", flush=True)
    print("Changes by source:", dict(source_change), flush=True)
    print(
        "Geen Shopify-writes. Review changes-CSV → mapper bijsturen → daarna apply.",
        flush=True,
    )


if __name__ == "__main__":
    main()
