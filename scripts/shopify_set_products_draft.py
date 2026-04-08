#!/usr/bin/env python3
"""
Zet Shopify-producten op status **DRAFT** via REST (zelfde aanpak als shopify_sync_from_0150).

Bron: CSV met **product_id_numeric**, door komma’s gescheiden **ID’s**, of **handles**
(Admin → product-URL slug; wordt met GET ``products.json?handle=`` opgezocht).

Standaard **dry-run**. Voeg **--apply** toe om echt te wijzigen.

  python3 scripts/shopify_set_products_draft.py --csv pad/naar/export.csv
  python3 scripts/shopify_set_products_draft.py --csv export.csv --only-row-kind x_single_variant
  python3 scripts/shopify_set_products_draft.py --ids 8553730539850,8553730081098 --apply
  python3 scripts/shopify_set_products_draft.py --handles-file mijn_handles.txt --apply
  python3 scripts/shopify_set_products_draft.py --handles "team-pants,replica-team-pants"

Vereist: SHOPIFY_ACCESS_TOKEN, SHOPIFY_SHOP_DOMAIN (en SHOPIFY_ADMIN_API_VERSION) in .env.
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
from modules.xml_loader import normalize_shopify_product_handle  # noqa: E402

SHOP = config.SHOPIFY_SHOP_DOMAIN
TOKEN = config.SHOPIFY_ACCESS_TOKEN
API_VER = config.SHOPIFY_ADMIN_API_VERSION


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def rest_put_json(
    shop: str,
    token: str,
    api_version: str,
    url: str,
    payload: dict,
    sess: requests.Session,
) -> tuple[bool, str]:
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    n429 = 0
    n500 = 0
    n_net = 0
    timeout = (15, 60)
    while True:
        try:
            r = sess.put(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=timeout,
                proxies={"http": None, "https": None},
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            n_net += 1
            if n_net > 25:
                return False, f"netwerk: {type(e).__name__}: {e!s}"[:500]
            w = min(2.0 + n_net * 0.4, 45.0)
            print(
                f"REST netwerkfout ({n_net}/25) {type(e).__name__}, {w:.1f}s…",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(w)
            continue
        n_net = 0
        if r.status_code == 429:
            n429 += 1
            if n429 > 30:
                return False, "429 rate limit: te veel retries"
            print(f"REST rate limit ({n429}/30), wachten…", file=sys.stderr, flush=True)
            time.sleep(min(2.0 + n429 * 0.3, 45.0))
            continue
        if r.status_code >= 500:
            n500 += 1
            if n500 > 10:
                return False, r.text[:500]
            print("Shopify serverfout, retry…", file=sys.stderr, flush=True)
            time.sleep(3)
            continue
        n500 = 0
        if r.status_code >= 400:
            return False, r.text[:500]
        return True, ""


def rest_product_draft(
    product_id: str,
    sess: requests.Session,
) -> bool:
    try:
        pid_int = int(product_id)
    except ValueError:
        print(f"Ongeldig product-id: {product_id!r}", file=sys.stderr)
        return False
    url = f"https://{SHOP}/admin/api/{API_VER}/products/{pid_int}.json"
    ok, err = rest_put_json(
        SHOP,
        TOKEN,
        API_VER,
        url,
        {"product": {"id": pid_int, "status": "draft"}},
        sess,
    )
    if not ok:
        print(f"  Product {product_id}: {err}", file=sys.stderr)
    return ok


def _norm_map(fieldnames: list[str] | None) -> dict[str, str]:
    if not fieldnames:
        return {}
    return {(k or "").strip().lower(): k for k in fieldnames}


def load_ids_from_csv(
    path: Path,
    *,
    only_row_kind: str | None,
) -> list[str]:
    with open(path, encoding="utf-8", newline="") as f:
        r = csv.DictReader(f)
        if not r.fieldnames:
            return []
        nm = _norm_map(list(r.fieldnames))
        id_col = None
        for cand in ("product_id_numeric", "product_id", "id"):
            if cand in nm:
                id_col = nm[cand]
                break
        if not id_col:
            id_col = r.fieldnames[0]
        rk_col = nm.get("row_kind")

        out: list[str] = []
        seen: set[str] = set()
        for row in r:
            if only_row_kind and rk_col:
                if (row.get(rk_col) or "").strip() != only_row_kind:
                    continue
            pid = (row.get(id_col) or "").strip()
            if not pid or not pid.isdigit():
                continue
            if pid in seen:
                continue
            seen.add(pid)
            out.append(pid)
        return out


def parse_handles_blob(raw: str) -> list[str]:
    """Komma’s en newlines als scheiding; handles naar lowercase (Shopify-slug)."""
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.replace(",", "\n").split("\n"):
        h = normalize_shopify_product_handle(part)
        if not h:
            continue
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def fetch_product_id_for_handle(sess: requests.Session, handle: str) -> str | None:
    """GET products.json?handle=… → eerste product-id of None."""
    url = f"https://{SHOP}/admin/api/{API_VER}/products.json"
    headers = {"X-Shopify-Access-Token": TOKEN}
    n429 = 0
    while True:
        try:
            r = sess.get(
                url,
                headers=headers,
                params={"handle": handle, "fields": "id,handle"},
                timeout=(15, 60),
                proxies={"http": None, "https": None},
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            print(f"GET handle {handle!r}: {e}", file=sys.stderr, flush=True)
            time.sleep(2)
            continue
        if r.status_code == 429:
            n429 += 1
            if n429 > 30:
                print(f"429 bij handle {handle!r}", file=sys.stderr)
                return None
            time.sleep(min(2.0 + n429 * 0.3, 45.0))
            continue
        if r.status_code >= 500:
            time.sleep(3)
            continue
        if r.status_code >= 400:
            print(
                f"  Handle {handle!r}: HTTP {r.status_code} {r.text[:200]}",
                file=sys.stderr,
            )
            return None
        try:
            data = r.json()
        except json.JSONDecodeError:
            return None
        products = data.get("products") or []
        if not products:
            return None
        pid = products[0].get("id")
        return str(pid) if pid is not None else None


def resolve_handles_to_ids(
    handles: list[str],
    sess: requests.Session,
    *,
    sleep: float,
) -> tuple[list[str], list[tuple[str, str]]]:
    """
    Retourneert (ids_in_volgorde, [(handle, id), ...]).
    Ontbrekende handles worden overgeslagen met melding.
    """
    ids: list[str] = []
    mapping: list[tuple[str, str]] = []
    seen_id: set[str] = set()
    for h in handles:
        pid = fetch_product_id_for_handle(sess, h)
        if sleep > 0:
            time.sleep(sleep)
        if not pid:
            print(f"  Niet gevonden (geen product met handle): {h}", file=sys.stderr)
            continue
        if pid in seen_id:
            print(f"  Dubbel id {pid} (handle {h}), één keer draft.", file=sys.stderr)
            continue
        seen_id.add(pid)
        ids.append(pid)
        mapping.append((h, pid))
    return ids, mapping


def parse_ids_arg(raw: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for part in raw.replace("\n", ",").split(","):
        p = part.strip()
        if not p:
            continue
        if not p.isdigit():
            print(f"Negeer ongeldig id: {p!r}", file=sys.stderr)
            continue
        if p not in seen:
            seen.add(p)
            out.append(p)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Zet Shopify-producten op DRAFT (REST)."
    )
    ap.add_argument(
        "--csv",
        type=Path,
        metavar="PAD",
        help="CSV met product_id_numeric (of kolom id/product_id)",
    )
    ap.add_argument(
        "--ids",
        metavar="IDS",
        help="Komma-gescheiden Shopify product-id’s (numeriek)",
    )
    ap.add_argument(
        "--handles",
        metavar="LIST",
        help="Komma- of newline-gescheiden product handles (URL-slug)",
    )
    ap.add_argument(
        "--handles-file",
        type=Path,
        metavar="PAD",
        help="Bestand met één handle per regel",
    )
    ap.add_argument(
        "--only-row-kind",
        metavar="WAARDE",
        help="Als de CSV een kolom row_kind heeft: alleen deze rijen (bv. x_single_variant)",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Echt API-calls doen (zonder deze flag: dry-run)",
    )
    ap.add_argument(
        "--sleep",
        type=float,
        default=0.2,
        metavar="SEC",
        help="Pauze tussen requests (default: 0.2)",
    )
    args = ap.parse_args()

    if not TOKEN:
        print("Geen SHOPIFY_ACCESS_TOKEN in .env.", file=sys.stderr)
        return 2

    n_sources = sum(
        1 for x in (args.csv, args.ids, args.handles, args.handles_file) if x
    )
    if n_sources != 1:
        print(
            "Geef precies één van: --csv PAD | --ids … | --handles … | --handles-file PAD",
            file=sys.stderr,
        )
        return 2

    sess = _http_session()

    if args.csv:
        if not args.csv.is_file():
            print(f"Bestand niet gevonden: {args.csv}", file=sys.stderr)
            return 2
        ids = load_ids_from_csv(args.csv, only_row_kind=args.only_row_kind)
    elif args.ids:
        ids = parse_ids_arg(args.ids)
    elif args.handles_file:
        if not args.handles_file.is_file():
            print(f"Bestand niet gevonden: {args.handles_file}", file=sys.stderr)
            return 2
        with open(args.handles_file, encoding="utf-8") as f:
            hlist = parse_handles_blob(f.read())
        print(f"Handles oplossen naar product-id’s ({len(hlist)} stuks)...", file=sys.stderr)
        ids, handle_map = resolve_handles_to_ids(
            hlist, sess, sleep=max(args.sleep, 0.15)
        )
        if not ids:
            print("Geen enkele handle gevonden in de shop.", file=sys.stderr)
            return 2
        if not args.apply:
            print("Dry-run — voeg --apply toe om op DRAFT te zetten.", file=sys.stderr)
            for h, pid in handle_map:
                print(f"  {h}  →  {pid}", file=sys.stderr)
            return 0
    else:
        hlist = parse_handles_blob(args.handles or "")
        if not hlist:
            print("Geen handles opgegeven.", file=sys.stderr)
            return 2
        print(f"Handles oplossen naar product-id’s ({len(hlist)} stuks)...", file=sys.stderr)
        ids, handle_map = resolve_handles_to_ids(
            hlist, sess, sleep=max(args.sleep, 0.15)
        )
        if not ids:
            print("Geen enkele handle gevonden in de shop.", file=sys.stderr)
            return 2
        if not args.apply:
            print("Dry-run — voeg --apply toe om op DRAFT te zetten.", file=sys.stderr)
            for h, pid in handle_map:
                print(f"  {h}  →  {pid}", file=sys.stderr)
            return 0

    if not ids:
        print("Geen product-id’s om te verwerken.", file=sys.stderr)
        return 2

    print(f"Te verwerken: {len(ids)} unieke product-id’s.", file=sys.stderr, flush=True)
    if not args.apply:
        print("Dry-run — voeg --apply toe om op DRAFT te zetten.", file=sys.stderr)
        for i, pid in enumerate(ids[:20]):
            print(f"  {pid}")
        if len(ids) > 20:
            print(f"  … en {len(ids) - 20} meer", file=sys.stderr)
        return 0

    ok_n = 0
    for pid in ids:
        if rest_product_draft(pid, sess):
            ok_n += 1
            print(f"OK draft: {pid}", file=sys.stderr, flush=True)
        if args.sleep > 0:
            time.sleep(args.sleep)

    print(f"Klaar: {ok_n}/{len(ids)} gelukt.", file=sys.stderr, flush=True)
    return 0 if ok_n == len(ids) else 1


if __name__ == "__main__":
    raise SystemExit(main())
