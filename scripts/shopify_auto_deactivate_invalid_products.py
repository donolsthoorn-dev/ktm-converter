#!/usr/bin/env python3
"""
Controleer actieve/gepubliceerde Shopify-producten en zet ze op DRAFT als:
1) minstens één variant geen geldige prijs heeft (leeg/null/<= 0), of
2) het product volledig uitverkocht is (alle varianten inventoryPolicy=DENY en quantity<=0).

Standaard is dry-run (alleen rapporteren). Voeg --apply toe om echt te deactiveren.

Voorbeelden:
  python3 scripts/shopify_auto_deactivate_invalid_products.py
  python3 scripts/shopify_auto_deactivate_invalid_products.py --apply
  python3 scripts/shopify_auto_deactivate_invalid_products.py --output-csv output/auto_deactivate_report.csv

Vereist: SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN in .env of environment.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
os.chdir(ROOT)
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (12, 120)
_REQUEST_TIMEOUT_LONG = (12, 600)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        publishedAt
        variants {
          edges {
            node {
              id
              title
              sku
              price
              inventoryPolicy
              inventoryQuantity
            }
          }
        }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation KtmBulkDeactivateScan {
  bulkOperationRunQuery(
    query: BULK_QUERY_PLACEHOLDER
  ) {
    bulkOperation {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""

_GQL_POLL = """
query KtmPollBulk($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
      partialDataUrl
    }
  }
}
"""

_GQL_SET_DRAFT = """
mutation KtmSetDraft($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      status
    }
    userErrors {
      field
      message
    }
  }
}
"""


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _graphql_post(
    sess: requests.Session,
    query: str,
    variables: dict | None = None,
    *,
    timeout: tuple[int, int] = _REQUEST_TIMEOUT_LONG,
) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    for attempt in range(25):
        r = sess.post(
            _GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": TOKEN,
            },
            json=payload,
            timeout=timeout,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    return last


def _variant_price_bad(raw: object) -> bool:
    if raw is None:
        return True
    if isinstance(raw, (int, float)):
        try:
            return float(raw) <= 0.0
        except (TypeError, ValueError):
            return True
    s = str(raw).strip()
    if not s:
        return True
    try:
        return float(s.replace(",", ".")) <= 0.0
    except ValueError:
        return True


def _product_published_active_gql(p: dict) -> bool:
    st = (p.get("status") or "").strip().upper()
    if st != "ACTIVE":
        return False
    pub = p.get("publishedAt")
    if pub is None:
        return False
    if isinstance(pub, str) and not pub.strip():
        return False
    return True


def _gid_numeric(gid: str) -> str:
    return gid.rsplit("/", 1)[-1]


def _variant_is_sold_out(v: dict) -> bool:
    policy = (v.get("inventoryPolicy") or "").strip().upper()
    if policy != "DENY":
        return False
    qty = v.get("inventoryQuantity")
    if qty is None:
        return False
    try:
        return int(qty) <= 0
    except (TypeError, ValueError):
        return False


def _extract_candidate_reasons(variants: list[dict]) -> tuple[bool, bool]:
    if not variants:
        return False, False

    has_bad_price = any(_variant_price_bad(v.get("price")) for v in variants)
    all_sold_out = all(_variant_is_sold_out(v) for v in variants)
    return has_bad_price, all_sold_out


def _run_bulk(sess: requests.Session) -> list[dict]:
    q = _GQL_BULK_START.replace("BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY))
    body = _graphql_post(sess, q)
    gerrs = body.get("errors")
    if gerrs:
        print("GraphQL-fout bij start bulk export:", file=sys.stderr)
        print(json.dumps(gerrs, indent=2), file=sys.stderr)
        raise SystemExit(2)

    data = body.get("data") or {}
    run = data.get("bulkOperationRunQuery") or {}
    uerr = run.get("userErrors") or []
    if uerr:
        print("Bulk export geweigerd:", file=sys.stderr)
        for e in uerr:
            print(f"  {e.get('field')}: {e.get('message')}", file=sys.stderr)
        raise SystemExit(2)

    bulk = run.get("bulkOperation") or {}
    op_id = bulk.get("id")
    if not op_id:
        print("Geen bulk operation id in response.", file=sys.stderr)
        raise SystemExit(2)

    print(
        "Bulk export gestart — Shopify bouwt één JSONL-bestand (kan enkele minuten duren)...",
        flush=True,
    )

    poll_interval = 3.0
    while True:
        poll = _graphql_post(sess, _GQL_POLL, {"id": op_id})
        if poll.get("errors"):
            print("Poll-fout:", poll.get("errors"), file=sys.stderr)
            time.sleep(poll_interval)
            continue

        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        count = node.get("objectCount")
        if count is not None:
            print(f"  Status: {status} — objecten verwerkt: {count}", flush=True)
        else:
            print(f"  Status: {status}", flush=True)

        if status == "COMPLETED":
            url = node.get("url")
            if not url:
                print("\nGeen resultaat-URL (lege shop?).", flush=True)
                return []
            return _download_and_scan_jsonl(sess, url)

        if status in ("FAILED", "CANCELED"):
            err = node.get("errorCode")
            print(f"Bulk export {status.lower()}: {err}", file=sys.stderr)
            purl = node.get("partialDataUrl")
            if purl:
                print("Er is gedeeltelijke data; parse die niet automatisch.", file=sys.stderr)
            raise SystemExit(2)

        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)


def _download_and_scan_jsonl(sess: requests.Session, url: str) -> list[dict]:
    print("JSONL downloaden en regel voor regel verwerken...", flush=True)
    products: dict[str, dict] = {}
    variants_by_product: dict[str, list[dict]] = {}
    prev_product_gid: str | None = None
    candidates: list[dict] = []

    r = sess.get(
        url,
        stream=True,
        timeout=_REQUEST_TIMEOUT_LONG,
        proxies={"http": None, "https": None},
    )
    r.raise_for_status()

    line_n = 0
    for raw_line in r.iter_lines(decode_unicode=True):
        if not raw_line:
            continue
        line_n += 1
        if line_n % 250_000 == 0:
            print(f"  ... {line_n} JSONL-regels", flush=True)
        try:
            obj = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        gid = (obj.get("id") or "").strip()
        if "/ProductVariant/" in gid:
            parent = obj.get("__parentId")
            if not parent or parent not in products:
                continue
            variants_by_product.setdefault(parent, []).append(obj)
            continue
        if "/Product/" in gid and "/ProductVariant/" not in gid:
            if prev_product_gid and prev_product_gid in products:
                product = products[prev_product_gid]
                variants = variants_by_product.get(prev_product_gid, [])
                has_bad_price, all_sold_out = _extract_candidate_reasons(variants)
                if has_bad_price or all_sold_out:
                    candidates.append(
                        {
                            "product_gid": prev_product_gid,
                            "product_id_numeric": _gid_numeric(prev_product_gid),
                            "handle": (product.get("handle") or "").strip(),
                            "title": (product.get("title") or "").strip(),
                            "reason_bad_price": has_bad_price,
                            "reason_sold_out": all_sold_out,
                            "variant_count": len(variants),
                        }
                    )
                del products[prev_product_gid]
                variants_by_product.pop(prev_product_gid, None)
            prev_product_gid = gid
            if _product_published_active_gql(obj):
                products[gid] = obj
            continue

    if prev_product_gid and prev_product_gid in products:
        product = products[prev_product_gid]
        variants = variants_by_product.get(prev_product_gid, [])
        has_bad_price, all_sold_out = _extract_candidate_reasons(variants)
        if has_bad_price or all_sold_out:
            candidates.append(
                {
                    "product_gid": prev_product_gid,
                    "product_id_numeric": _gid_numeric(prev_product_gid),
                    "handle": (product.get("handle") or "").strip(),
                    "title": (product.get("title") or "").strip(),
                    "reason_bad_price": has_bad_price,
                    "reason_sold_out": all_sold_out,
                    "variant_count": len(variants),
                }
            )

    return candidates


def _reason_label(item: dict) -> str:
    reasons: list[str] = []
    if item.get("reason_bad_price"):
        reasons.append("bad_price")
    if item.get("reason_sold_out"):
        reasons.append("sold_out")
    return ",".join(reasons)


def _write_csv(rows: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writerow(
            [
                "product_id_numeric",
                "product_gid",
                "handle",
                "title",
                "reason_bad_price",
                "reason_sold_out",
                "reasons",
                "variant_count",
            ]
        )
        for row in rows:
            w.writerow(
                [
                    row["product_id_numeric"],
                    row["product_gid"],
                    row["handle"],
                    row["title"],
                    "1" if row["reason_bad_price"] else "0",
                    "1" if row["reason_sold_out"] else "0",
                    _reason_label(row),
                    row["variant_count"],
                ]
            )


def _set_products_draft(sess: requests.Session, rows: list[dict]) -> tuple[int, list[str]]:
    ok_count = 0
    failed: list[str] = []
    for idx, row in enumerate(rows, start=1):
        gid = row["product_gid"]
        body = _graphql_post(
            sess,
            _GQL_SET_DRAFT,
            {"input": {"id": gid, "status": "DRAFT"}},
            timeout=_REQUEST_TIMEOUT,
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
        print(
            f"  [{idx}/{len(rows)}] DRAFT gezet: {row['title'] or '(zonder titel)'} ({row['product_id_numeric']})",
            flush=True,
        )
        time.sleep(0.15)
    return ok_count, failed


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Deactiveer automatisch producten zonder geldige prijs of volledig uitverkocht."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Voer deactivatie echt uit (zonder deze vlag: dry-run rapport).",
    )
    ap.add_argument(
        "--output-csv",
        type=Path,
        default=Path("output/auto_deactivate_invalid_products.csv"),
        metavar="PAD",
        help="CSV-rapport pad (default: output/auto_deactivate_invalid_products.csv)",
    )
    args = ap.parse_args()

    if not TOKEN or not SHOP:
        print(
            "SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN zijn verplicht (.env).",
            file=sys.stderr,
        )
        return 2

    sess = _http_session()
    rows = _run_bulk(sess)

    rows_sorted = sorted(
        rows,
        key=lambda r: ((r.get("title") or "").lower(), str(r.get("product_id_numeric") or "")),
    )
    _write_csv(rows_sorted, args.output_csv)

    if not rows_sorted:
        print("Geen producten gevonden die aan de deactivatie-regels voldoen.", flush=True)
        print(f"CSV-rapport geschreven: {args.output_csv}", flush=True)
        return 0

    print(
        f"Gevonden: {len(rows_sorted)} product(en) met reden bad_price en/of sold_out.",
        flush=True,
    )
    print(f"CSV-rapport geschreven: {args.output_csv}", flush=True)
    for row in rows_sorted[:25]:
        print(
            f"- {row['title'] or '(zonder titel)'} ({row['product_id_numeric']}) [{_reason_label(row)}]",
            flush=True,
        )
    if len(rows_sorted) > 25:
        print(f"... en {len(rows_sorted) - 25} meer", flush=True)

    if not args.apply:
        print("Dry-run: geen producten aangepast. Voeg --apply toe om op DRAFT te zetten.", flush=True)
        return 0

    print("Apply-modus: producten op DRAFT zetten...", flush=True)
    ok_count, failed = _set_products_draft(sess, rows_sorted)
    print(f"Klaar: {ok_count}/{len(rows_sorted)} succesvol op DRAFT gezet.", flush=True)

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
