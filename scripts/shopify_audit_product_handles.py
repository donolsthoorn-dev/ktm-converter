#!/usr/bin/env python3
"""
Audit Shopify-producthandles t.o.v. SKU-regels en herstel waar veilig mogelijk.

Regels:
- Single-variant product: handle moet gelijk zijn aan SKU.
- Multi-variant product: handle hoort op "x" te eindigen.

Standaard is dit script een dry-run:
- Leest alle producten via Shopify REST
- Schrijft audit-CSV + fix-voorstel-CSV
- Past niets aan

Met --apply:
- Past alleen veilige updates toe uit fix-voorstel-CSV (geen collisions)

Voorbeelden:
  python3 scripts/shopify_audit_product_handles.py
  python3 scripts/shopify_audit_product_handles.py --apply
  python3 scripts/shopify_audit_product_handles.py --limit 50 --apply
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any

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
from modules.pricing_loader import normalize_sku_key  # noqa: E402
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
ADMIN_API_VERSION = config.SHOPIFY_ADMIN_API_VERSION
_GRAPHQL_URL = f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/graphql.json"
_REQUEST_TIMEOUT = (15, 90)


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _next_url_from_link_header(link: str | None) -> str | None:
    if not link:
        return None
    for part in link.split(","):
        if 'rel="next"' not in part:
            continue
        raw = part.split(";")[0].strip()
        if raw.startswith("<") and raw.endswith(">"):
            return raw[1:-1]
        return raw
    return None


def fetch_products(sess: requests.Session) -> list[dict[str, Any]]:
    """
    Haal alle producten op met velden die nodig zijn voor de handle-audit.
    """
    out: list[dict[str, Any]] = []
    url = (
        f"https://{SHOP}/admin/api/{ADMIN_API_VERSION}/products.json"
        "?limit=250&fields=id,handle,title,status,variants"
    )
    headers = {"X-Shopify-Access-Token": TOKEN}
    page = 0
    while url:
        page += 1
        for attempt in range(25):
            r = sess.get(
                url,
                headers=headers,
                timeout=_REQUEST_TIMEOUT,
                proxies={"http": None, "https": None},
            )
            if r.status_code == 429:
                time.sleep(min(2.0 + attempt * 0.4, 45.0))
                continue
            if r.status_code >= 500:
                time.sleep(3.0)
                continue
            r.raise_for_status()
            break
        else:
            print(f"REST products: te veel retries op pagina {page}", file=sys.stderr)
            raise SystemExit(2)

        body = r.json()
        chunk = body.get("products") or []
        out.extend(chunk)
        print(f"Shopify pagina {page}: +{len(chunk)} (totaal {len(out)})", flush=True)

        url = _next_url_from_link_header(r.headers.get("Link"))
        if url:
            time.sleep(0.35)
    return out


def _variant_skus(product: dict[str, Any]) -> list[str]:
    skus: list[str] = []
    for v in product.get("variants") or []:
        s = normalize_sku_key(v.get("sku"))
        if s:
            skus.append(s)
    # Volgorde behouden, duplicaten eruit
    seen: set[str] = set()
    uniq: list[str] = []
    for s in skus:
        if s in seen:
            continue
        seen.add(s)
        uniq.append(s)
    return uniq


def _expected_handle_for_product(skus: list[str]) -> tuple[str, str]:
    """
    Bepaal verwachte handle + regeltype.
    regeltype:
      - single_variant_exact_sku
      - multi_variant_suffix_x
      - unknown
    """
    if len(skus) == 1:
        return normalize_shopify_product_handle(skus[0]), "single_variant_exact_sku"
    if len(skus) > 1:
        # Multi-regel uit vraag: handle moet op x eindigen.
        # Voor veilige autofix alleen deterministic prefix-regel.
        first = skus[0]
        if len(first) >= 3 and all(len(s) == len(first) for s in skus):
            prefix = first[:-1]
            if all(s[:-1] == prefix for s in skus):
                return normalize_shopify_product_handle(prefix + "X"), "multi_variant_suffix_x"
    return "", "unknown"


def _graphql_post(sess: requests.Session, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    payload = {"query": query, "variables": variables}
    for attempt in range(25):
        r = sess.post(
            _GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        body = r.json()
        errs = body.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return body
    return body


def _product_gid(numeric_id: str) -> str:
    return f"gid://shopify/Product/{int(numeric_id)}"


def apply_handle_update(
    sess: requests.Session, product_id: str, new_handle: str
) -> tuple[bool, str]:
    mutation = """
mutation KtmHandleFix($input: ProductInput!) {
  productUpdate(input: $input) {
    product {
      id
      handle
    }
    userErrors {
      field
      message
    }
  }
}
"""
    body = _graphql_post(
        sess,
        mutation,
        {
            "input": {
                "id": _product_gid(product_id),
                "handle": new_handle,
            }
        },
    )
    errs = body.get("errors") or []
    if errs:
        return False, json.dumps(errs, ensure_ascii=False)[:500]
    upd = (body.get("data") or {}).get("productUpdate") or {}
    uerr = upd.get("userErrors") or []
    if uerr:
        return False, json.dumps(uerr, ensure_ascii=False)[:500]
    got = normalize_shopify_product_handle(((upd.get("product") or {}).get("handle") or ""))
    if got != new_handle:
        return False, f"onverwachte handle terug: {got!r}"
    return True, ""


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Audit handles vs SKU-regels en fix veilige gevallen."
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Pas fix-voorstellen toe (zonder deze vlag: alleen audit).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Max N fixes bij --apply (0 = geen limiet).",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output/handle_audit"),
        metavar="PAD",
        help="Map voor audit/fix CSV output (default: output/handle_audit).",
    )
    args = ap.parse_args()

    if not TOKEN or not SHOP:
        print("SHOPIFY_ACCESS_TOKEN en SHOPIFY_SHOP_DOMAIN zijn verplicht (.env).", file=sys.stderr)
        return 2

    sess = _http_session()
    products = fetch_products(sess)
    if not products:
        print("Geen producten gevonden.", flush=True)
        return 0

    handle_to_product_id: dict[str, str] = {}
    for p in products:
        pid = str(int(p.get("id")))
        handle = normalize_shopify_product_handle(p.get("handle") or "")
        if handle and pid:
            handle_to_product_id[handle] = pid

    audit_rows: list[dict[str, Any]] = []
    fix_rows: list[dict[str, Any]] = []

    for p in products:
        pid = str(int(p.get("id")))
        title = str(p.get("title") or "").strip()
        status = str(p.get("status") or "").strip().upper()
        current_handle = normalize_shopify_product_handle(p.get("handle") or "")
        skus = _variant_skus(p)
        variant_count = len(skus)

        expected_handle, rule = _expected_handle_for_product(skus)
        needs_fix = bool(expected_handle and current_handle != expected_handle)

        multi_variant_non_x = variant_count > 1 and not current_handle.endswith("x")
        audit_reason = ""
        if needs_fix:
            audit_reason = "expected_handle_mismatch"
        elif multi_variant_non_x:
            audit_reason = "multi_variant_handle_not_suffix_x"
        elif variant_count == 1 and skus and current_handle != normalize_shopify_product_handle(skus[0]):
            audit_reason = "single_variant_handle_not_sku"

        if not audit_reason:
            continue

        existing_owner = handle_to_product_id.get(expected_handle, "") if expected_handle else ""
        collision = bool(expected_handle and existing_owner and existing_owner != pid)

        audit_rows.append(
            {
                "product_id": pid,
                "status": status,
                "title": title,
                "current_handle": current_handle,
                "expected_handle": expected_handle,
                "rule": rule,
                "variant_count": variant_count,
                "variant_skus": ",".join(skus),
                "audit_reason": audit_reason,
                "collision": "1" if collision else "0",
                "collision_product_id": existing_owner if collision else "",
                "fixable": "1" if (needs_fix and not collision) else "0",
            }
        )

        if needs_fix and not collision:
            fix_rows.append(
                {
                    "product_id": pid,
                    "status": status,
                    "current_handle": current_handle,
                    "new_handle": expected_handle,
                    "rule": rule,
                    "variant_count": variant_count,
                    "variant_skus": ",".join(skus),
                }
            )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    audit_csv = args.output_dir / f"shopify_handle_audit_{ts}.csv"
    fix_csv = args.output_dir / f"shopify_handle_fixable_{ts}.csv"

    _write_csv(
        audit_csv,
        audit_rows,
        [
            "product_id",
            "status",
            "title",
            "current_handle",
            "expected_handle",
            "rule",
            "variant_count",
            "variant_skus",
            "audit_reason",
            "collision",
            "collision_product_id",
            "fixable",
        ],
    )
    _write_csv(
        fix_csv,
        fix_rows,
        [
            "product_id",
            "status",
            "current_handle",
            "new_handle",
            "rule",
            "variant_count",
            "variant_skus",
        ],
    )

    print(f"Audit CSV: {audit_csv}", flush=True)
    print(f"Fix-voorstel CSV: {fix_csv}", flush=True)
    print(
        f"Samenvatting: afwijkingen={len(audit_rows)}, veilig-fixbaar={len(fix_rows)}",
        flush=True,
    )

    if not args.apply:
        print("Dry-run: geen handles aangepast. Gebruik --apply om fixbare regels toe te passen.")
        return 0

    if not fix_rows:
        print("Geen fixbare regels om toe te passen.", flush=True)
        return 0

    limit = max(0, int(args.limit or 0))
    applied = 0
    failed = 0
    for idx, row in enumerate(fix_rows, start=1):
        if limit and applied >= limit:
            print(f"Gestopt na --limit {limit}.", flush=True)
            break
        ok, err = apply_handle_update(sess, row["product_id"], row["new_handle"])
        if ok:
            applied += 1
            print(
                f"[{idx}/{len(fix_rows)}] OK {row['product_id']}: "
                f"{row['current_handle']} -> {row['new_handle']}",
                flush=True,
            )
        else:
            failed += 1
            print(
                f"[{idx}/{len(fix_rows)}] FOUT {row['product_id']} ({row['current_handle']}): {err}",
                file=sys.stderr,
                flush=True,
            )
        time.sleep(0.2)

    print(f"Klaar. toegepast={applied}, fouten={failed}", flush=True)
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
