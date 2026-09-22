#!/usr/bin/env python3
"""
Zet actieve Motox-producten zonder afbeelding op DRAFT.

Alleen status ACTIVE en featuredImage leeg. DRAFT en ARCHIVED blijven staan.
Standaard dry-run. Met --apply wordt de status echt gezet.

Voorbeelden:
  python3 scripts/motox_deactivate_products_without_image.py
  python3 scripts/motox_deactivate_products_without_image.py --apply
  python3 scripts/motox_deactivate_products_without_image.py --apply --limit 20

Vereist: SHOPIFY_MOTOX_ACCESS_TOKEN en SHOPIFY_MOTOX_SHOP_DOMAIN
(of SHOPIFY_ACCESS_TOKEN + SHOPIFY_SHOP_DOMAIN in .env.motox).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from modules.motox_shopify_cache import (  # noqa: E402
    _TIMEOUT,
    _gql,
    _session,
    _wait_current_bulk,
    load_motox_env,
)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        featuredImage {
          id
        }
        variants {
          edges {
            node {
              sku
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation MotoxDeactivateNoImageBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_POLL = """
query MotoxDeactivateNoImagePoll($id: ID!) {
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

_GQL_SET_DRAFT = """
mutation MotoxSetDraft($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id status }
    userErrors { field message }
  }
}
"""


def has_featured_image(featured: object) -> bool:
    if not featured or not isinstance(featured, dict):
        return False
    return bool(featured.get("id"))


def should_set_draft(status: str, has_image: bool) -> bool:
    """Alleen live producten zonder featured image gaan naar DRAFT."""
    return (status or "").strip().upper() == "ACTIVE" and not has_image


def parse_bulk_jsonl(path: Path) -> list[dict]:
    products: dict[str, dict] = {}
    order: list[str] = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            gid = obj.get("id") or ""
            parent = obj.get("__parentId")
            if gid.startswith("gid://shopify/Product/") and not parent:
                products[gid] = {
                    "product_gid": gid,
                    "product_id": gid.rsplit("/", 1)[-1],
                    "handle": (obj.get("handle") or "").strip(),
                    "title": (obj.get("title") or "").strip(),
                    "status": (obj.get("status") or "").strip().upper(),
                    "has_image": has_featured_image(obj.get("featuredImage")),
                    "skus": [],
                }
                order.append(gid)
                continue
            if parent in products and "sku" in obj:
                sku = (obj.get("sku") or "").strip()
                if sku:
                    products[parent]["skus"].append(sku)
    return [products[gid] for gid in order]


def _download_bulk(sess, gql_url: str, token: str, dest: Path) -> None:
    _wait_current_bulk(sess, gql_url, token)
    q = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY))
    body = _gql(sess, gql_url, token, q)
    if body.get("errors"):
        raise SystemExit(f"GraphQL start-fout: {body.get('errors')}")
    run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
    uerr = run.get("userErrors") or []
    if uerr:
        raise SystemExit(f"Bulk geweigerd: {uerr}")
    op_id = (run.get("bulkOperation") or {}).get("id")
    if not op_id:
        raise SystemExit("Geen Motox bulk operation id.")
    print("Motox bulk-export gestart...", flush=True)
    poll_interval = 3.0
    while True:
        poll = _gql(sess, gql_url, token, _GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url")
            if not url:
                dest.write_text("", encoding="utf-8")
                return
            dest.parent.mkdir(parents=True, exist_ok=True)
            with sess.get(
                url,
                stream=True,
                timeout=_TIMEOUT,
                proxies={"http": None, "https": None},
            ) as r:
                r.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in r.iter_content(1024 * 256):
                        if chunk:
                            f.write(chunk)
            return
        if status in ("FAILED", "CANCELED"):
            raise SystemExit(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _write_csv(rows: list[dict], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(
            ["product_id", "handle", "title", "status", "skus", "actie"]
        )
        for row in rows:
            w.writerow(
                [
                    row["product_id"],
                    row["handle"],
                    row["title"],
                    row["status"],
                    ",".join(row.get("skus") or []),
                    "set_draft",
                ]
            )


def _set_draft(sess, gql_url: str, token: str, rows: list[dict]) -> tuple[int, list[str]]:
    ok_count = 0
    failed: list[str] = []
    total = len(rows)
    for idx, row in enumerate(rows, start=1):
        gid = row["product_gid"]
        body = _gql(
            sess,
            gql_url,
            token,
            _GQL_SET_DRAFT,
            {"input": {"id": gid, "status": "DRAFT"}},
        )
        errs = body.get("errors") or []
        if errs:
            failed.append(f"{gid} GraphQL errors: {json.dumps(errs)}")
            continue
        upd = ((body.get("data") or {}).get("productUpdate")) or {}
        user_errors = upd.get("userErrors") or []
        if user_errors:
            failed.append(f"{gid} userErrors: {json.dumps(user_errors)}")
            continue
        status = (((upd.get("product") or {}).get("status")) or "").strip().upper()
        if status != "DRAFT":
            failed.append(f"{gid} onverwachte status: {status!r}")
            continue
        ok_count += 1
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(
                f"  [{idx}/{total}] DRAFT: {row['title'] or '(zonder titel)'} ({row['product_id']})",
                flush=True,
            )
        time.sleep(0.15)
    return ok_count, failed


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Zet actieve Motox-producten zonder afbeelding op DRAFT."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Zet de status echt op DRAFT (zonder deze vlag: alleen rapport).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max aantal producten om op DRAFT te zetten (0 = alle).",
    )
    ap.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/motox_deactivate_products_without_image.csv"),
        metavar="PAD",
        help="CSV-rapport (default: output/motox_deactivate_products_without_image.csv)",
    )
    args = ap.parse_args()
    if args.limit < 0:
        print("--limit moet 0 of hoger zijn.", file=sys.stderr)
        return 2

    domain, token, api = load_motox_env()
    gql_url = f"https://{domain}/admin/api/{api}/graphql.json"
    print(f"Shop: {domain} (API {api})", flush=True)
    sess = _session()

    with tempfile.TemporaryDirectory(prefix="motox-no-image-") as tmp:
        bulk_path = Path(tmp) / "products.jsonl"
        _download_bulk(sess, gql_url, token, bulk_path)
        products = parse_bulk_jsonl(bulk_path)

    candidates = [p for p in products if should_set_draft(p["status"], p["has_image"])]
    candidates.sort(key=lambda r: ((r.get("title") or "").lower(), r.get("product_id") or ""))
    _write_csv(candidates, args.output_csv)

    print(
        f"Gescand: {len(products)} producten; "
        f"actief zonder afbeelding: {len(candidates)}.",
        flush=True,
    )
    print(f"CSV-rapport: {args.output_csv}", flush=True)
    for row in candidates[:25]:
        print(
            f"- {row['title'] or '(zonder titel)'} ({row['product_id']}) "
            f"handle={row['handle']}",
            flush=True,
        )
    if len(candidates) > 25:
        print(f"... en {len(candidates) - 25} meer", flush=True)

    if not args.apply:
        print("Dry-run: niets aangepast. Voeg --apply toe om op DRAFT te zetten.", flush=True)
        return 0
    if not candidates:
        print("Geen producten om op DRAFT te zetten.", flush=True)
        return 0

    apply_rows = candidates if args.limit == 0 else candidates[: args.limit]
    if args.limit and len(candidates) > len(apply_rows):
        print(
            f"Apply beperkt tot {len(apply_rows)} van {len(candidates)} (--limit {args.limit}).",
            flush=True,
        )
    else:
        print(f"Apply: {len(apply_rows)} product(en) op DRAFT zetten.", flush=True)

    ok_count, failed = _set_draft(sess, gql_url, token, apply_rows)
    print(f"Klaar: {ok_count}/{len(apply_rows)} op DRAFT gezet.", flush=True)
    if failed:
        print("Mislukte updates:", file=sys.stderr)
        for err in failed[:50]:
            print(f"  - {err}", file=sys.stderr)
        if len(failed) > 50:
            print(f"  ... en {len(failed) - 50} meer", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
