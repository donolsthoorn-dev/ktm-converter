#!/usr/bin/env python3
"""
Stand-alone: leest KTM 0150-CSV (kolommen ArticleNumber + hqETADate) en werkt het
variant-metafield inventory_policy_eta_date in Shopify bij (type date, ISO YYYY-MM-DD).

- Datum **vandaag of in de toekomst** → metafield zetten op die datum.
- Datum **in het verleden** → metafield **verwijderen** (veld leeg in Admin).
- **Lege** hqETADate → metafield **verwijderen** (zelfde als verleden).

**SKU → variant-id** komt uit een cachebestand (geen live variant-fetch in dit script).
Bouw of ververs die cache met:
  python3 scripts/shopify_refresh_variant_cache.py

CSV-scheidingsteken (komma vs `;`) via `pricing_loader.detect_0150_csv_delimiter`.

Configuratie (omgevingsvariabelen, o.a. via project-root .env):
  SHOPIFY_ACCESS_TOKEN   — verplicht
  SHOPIFY_SHOP_DOMAIN      — default ktm-shop-nl.myshopify.com
  SHOPIFY_ADMIN_API_VERSION — default 2024-10
  SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE — default global (moet gelijk zijn aan de definitie in
    Admin: Settings → Custom data → Variants; jullie gebruiken global.inventory_policy_eta_date)
  SHOPIFY_VARIANT_ETA_METAFIELD_KEY — default inventory_policy_eta_date

CLI:
  python3 scripts/shopify_sync_eta_from_0150.py
  python3 scripts/shopify_sync_eta_from_0150.py --csv input/0150_00_Z1_EUR_EN_csv.csv
  python3 scripts/shopify_sync_eta_from_0150.py --dry-run
  python3 scripts/shopify_sync_eta_from_0150.py --variant-cache pad/naar/sku_variant.json

Later uitbreidbaar (prijzen + cron) in hetzelfde bestand of een wrapper.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import date
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from modules.pricing_loader import detect_0150_csv_delimiter  # noqa: E402

DEFAULT_VARIANT_CACHE = PROJECT_ROOT / "cache" / "shopify_eta_sync_sku_variant.json"


def load_dotenv(path: Path | None = None) -> None:
    """Minimale KEY=value parser; overschrijft bestaande os.environ niet."""
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


def parse_hq_eta_to_iso(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    parts = raw.replace("-", "/").split("/")
    if len(parts) != 3:
        return None
    try:
        y, m, d = int(parts[0]), int(parts[1]), int(parts[2])
        date(y, m, d)
        return f"{y:04d}-{m:02d}-{d:02d}"
    except (ValueError, OverflowError):
        return None


def resolve_csv_path(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise FileNotFoundError(f"CSV niet gevonden: {p}")
        return p.resolve()
    input_dir = PROJECT_ROOT / "input"
    for name in sorted(os.listdir(input_dir)):
        if "0150" in name and name.endswith(".csv"):
            return (input_dir / name).resolve()
    raise FileNotFoundError(f"Geen *0150*.csv in {input_dir}; gebruik --csv PAD")


def variant_cache_json_to_sku_variant_ids(raw: dict) -> dict[str, str]:
    """Oude cache: SKU -> variant-id string. Nieuwe cache: SKU -> { variant_id, product_id }."""
    out: dict[str, str] = {}
    for k, v in raw.items():
        if isinstance(v, dict):
            vid = str(v.get("variant_id") or "").strip()
        else:
            vid = str(v).strip()
        if vid:
            out[k] = vid
    return out


def read_sku_eta_from_csv(csv_path: Path) -> dict[str, str | None]:
    """
    SKU (uppercase) -> YYYY-MM-DD, of None als hqETADate leeg is (alleen whitespace).
    Rijen zonder geldige datum (parse faalt) worden overgeslagen (geen wijziging in Shopify).
    """
    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin1")
    out: dict[str, str | None] = {}
    for enc in encodings:
        try:
            with open(csv_path, newline="", encoding=enc) as fh:
                first = fh.readline()
                fh.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(fh, delimiter=delim)
                header = next(reader, None)
                if not header:
                    return out
                try:
                    eta_col = header.index("hqETADate")
                    sku_col = header.index("ArticleNumber")
                except ValueError:
                    eta_col, sku_col = 22, 1
                for row in reader:
                    if len(row) <= max(eta_col, sku_col):
                        continue
                    sku = row[sku_col].strip().upper()
                    if not sku:
                        continue
                    raw_eta = (row[eta_col] or "").strip()
                    if not raw_eta:
                        out[sku] = None
                        continue
                    iso = parse_hq_eta_to_iso(raw_eta)
                    if iso:
                        out[sku] = iso
            return out
        except UnicodeDecodeError:
            continue
    raise OSError(f"CSV kon niet worden gelezen: {csv_path}")


def graphql_metafields_set(
    shop: str,
    token: str,
    api_version: str,
    metafields: list[dict],
) -> dict:
    sess = _http_session()
    q = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    while True:
        r = sess.post(
            url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            data=json.dumps({"query": q, "variables": {"metafields": metafields}}),
            timeout=(12, 120),
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("GraphQL rate limit, wachten...", flush=True)
            time.sleep(2)
            continue
        r.raise_for_status()
        body = r.json()
        if "errors" in body:
            raise RuntimeError(json.dumps(body["errors"], indent=2))
        return body.get("data") or {}


def graphql_metafields_delete(
    shop: str,
    token: str,
    api_version: str,
    identifiers: list[dict],
) -> dict:
    """identifiers: { ownerId, namespace, key } — max. 25 per call (Shopify-limiet)."""
    sess = _http_session()
    q = """
mutation metafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields { key namespace ownerId }
    userErrors { field message }
  }
}
"""
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    while True:
        r = sess.post(
            url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            data=json.dumps({"query": q, "variables": {"metafields": identifiers}}),
            timeout=(12, 120),
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            print("GraphQL rate limit (delete), wachten...", flush=True)
            time.sleep(2)
            continue
        r.raise_for_status()
        body = r.json()
        if "errors" in body:
            raise RuntimeError(json.dumps(body["errors"], indent=2))
        return body.get("data") or {}


def build_eta_ops(
    sku_to_eta: dict[str, str | None],
    sku_to_vid: dict[str, str],
    today: date,
) -> tuple[
    list[tuple[str, str, str, str]],
    int,
    int,
    int,
    int,
    int,
]:
    """
    Returns: ops, n_clear_empty, n_clear_past, n_no_variant, n_set_planned, n_clear_planned
    """
    n_no_variant = sum(1 for sku in sku_to_eta if sku not in sku_to_vid)
    ops: list[tuple[str, str, str, str]] = []
    n_clear_empty = 0
    n_clear_past = 0
    for sku, val in sku_to_eta.items():
        vid = sku_to_vid.get(sku)
        if not vid:
            continue
        if val is None:
            ops.append(("clear", sku, vid, ""))
            n_clear_empty += 1
            continue
        d = date.fromisoformat(val)
        if d < today:
            ops.append(("clear", sku, vid, ""))
            n_clear_past += 1
        else:
            ops.append(("set", sku, vid, val))
    n_set_planned = sum(1 for o in ops if o[0] == "set")
    n_clear_planned = sum(1 for o in ops if o[0] == "clear")
    return ops, n_clear_empty, n_clear_past, n_no_variant, n_set_planned, n_clear_planned


def main() -> int:
    load_dotenv()

    p = argparse.ArgumentParser(
        description="0150 hqETADate -> Shopify variant-metafield inventory_policy_eta_date"
    )
    p.add_argument(
        "--csv",
        metavar="PAD",
        help="0150-CSV (default: eerste *0150*.csv in input/)",
    )
    p.add_argument("--dry-run", action="store_true", help="Geen Shopify-API-calls")
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max. aantal mutaties (0 = alles)",
    )
    p.add_argument(
        "--variant-cache",
        type=Path,
        default=DEFAULT_VARIANT_CACHE,
        help=f"SKU→variant JSON van shopify_refresh_variant_cache.py (default: {DEFAULT_VARIANT_CACHE})",
    )
    args = p.parse_args()

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "2024-10").strip()
    ns = os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE", "global").strip()
    key = os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_KEY", "inventory_policy_eta_date").strip()

    if not args.dry_run and not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env of export).", flush=True)
        return 1
    if not ns or not key:
        print("Metafield namespace/key mogen niet leeg zijn.", flush=True)
        return 1

    csv_path = resolve_csv_path(args.csv)
    print(f"CSV: {csv_path}", flush=True)
    sku_to_eta = read_sku_eta_from_csv(csv_path)
    n_empty = sum(1 for v in sku_to_eta.values() if v is None)
    n_dated = sum(1 for v in sku_to_eta.values() if v is not None)
    print(
        f"Ingelezen: {n_dated} SKU's met datum, {n_empty} SKU's met lege hqETADate.",
        flush=True,
    )

    today = date.today()

    if args.dry_run:
        n_future = sum(
            1 for v in sku_to_eta.values() if v is not None and date.fromisoformat(v) >= today
        )
        n_past = sum(
            1 for v in sku_to_eta.values() if v is not None and date.fromisoformat(v) < today
        )
        print(
            f"  (t.o.v. vandaag {today.isoformat()}: "
            f"{n_future} vandaag/toekomst, {n_past} verleden, "
            f"{n_empty} leeg — verleden/lege ETA → metafield wissen in Shopify)",
            flush=True,
        )
        print(
            f"Dry-run: zou {ns!r}.{key!r} zetten of verwijderen na match variant-SKU in Shopify.",
            flush=True,
        )
        cache_hint = args.variant_cache.resolve()
        print(
            f"  (live-run gebruikt variant-cache: {cache_hint}; "
            f"aanmaken met: python3 scripts/shopify_refresh_variant_cache.py)",
            flush=True,
        )
        if cache_hint.is_file():
            with open(cache_hint, encoding="utf-8") as f:
                sku_to_vid_dr = variant_cache_json_to_sku_variant_ids(json.load(f))
            ops_dr, _ce, _cp, n_no_dr, n_sp, n_cp = build_eta_ops(sku_to_eta, sku_to_vid_dr, today)
            _print_summary_footer(
                csv_path=csv_path,
                ns=ns,
                key=key,
                n_csv_skus=len(sku_to_eta),
                n_no_variant=n_no_dr,
                n_set_ok=0,
                n_del_ok=0,
                n_set_planned=n_sp,
                n_clear_planned=n_cp,
                errors=0,
                limited=False,
                dry_run=True,
                planned_set_full=None,
                planned_clear_full=None,
            )
        else:
            print(
                "(Geen variant-cache: geen voorspelde aantallen voor Shopify-overlap.)",
                flush=True,
            )
        return 0

    cache_path = args.variant_cache.resolve()
    if not cache_path.is_file():
        print(
            f"Variant-cache ontbreekt: {cache_path}\n"
            "Bouw eerst de SKU→variant-lijst:\n"
            "  python3 scripts/shopify_refresh_variant_cache.py",
            flush=True,
        )
        return 1

    print(f"SKU→variant uit cache: {cache_path}", flush=True)
    with open(cache_path, encoding="utf-8") as f:
        sku_to_vid = variant_cache_json_to_sku_variant_ids(json.load(f))

    ops, n_clear_empty, n_clear_past, n_no_variant, n_set_planned_full, n_clear_planned_full = (
        build_eta_ops(sku_to_eta, sku_to_vid, today)
    )
    n_set_planned = n_set_planned_full
    n_clear_planned = n_clear_planned_full

    if not ops:
        print("Geen overlap tussen CSV-ETA en Shopify-variant-SKU's.", flush=True)
        _print_summary_footer(
            csv_path=csv_path,
            ns=ns,
            key=key,
            n_csv_skus=len(sku_to_eta),
            n_no_variant=n_no_variant,
            n_set_ok=0,
            n_del_ok=0,
            n_set_planned=0,
            n_clear_planned=0,
            errors=0,
            limited=False,
            dry_run=False,
            planned_set_full=None,
            planned_clear_full=None,
        )
        return 0

    print(
        f"Te verwerken: {n_set_planned_full} ETA zetten (vandaag/toekomst), "
        f"{n_clear_planned_full} metafield wissen (waarvan {n_clear_empty} lege ETA, "
        f"{n_clear_past} datum in verleden).",
        flush=True,
    )
    if n_no_variant:
        print(
            f"CSV-SKU's zonder variant in shop (overgeslagen): {n_no_variant}",
            flush=True,
        )

    limited = False
    if args.limit > 0:
        ops = ops[: args.limit]
        limited = True
        print(f"Beperkt tot --limit {args.limit} mutaties.", flush=True)
    n_set_planned = sum(1 for o in ops if o[0] == "set")
    n_clear_planned = sum(1 for o in ops if o[0] == "clear")

    batch_size = 25
    errors = 0
    n_set_ok = 0
    n_del_ok = 0
    batch_num = 0

    i = 0
    while i < len(ops):
        kind = ops[i][0]
        batch: list[tuple[str, str, str, str]] = []
        while i < len(ops) and len(batch) < batch_size and ops[i][0] == kind:
            batch.append(ops[i])
            i += 1
        batch_num += 1

        if kind == "clear":
            identifiers = [
                {
                    "ownerId": f"gid://shopify/ProductVariant/{op[2]}",
                    "namespace": ns,
                    "key": key,
                }
                for op in batch
            ]
            data = graphql_metafields_delete(shop, token, api_ver, identifiers)
            mdel = (data or {}).get("metafieldsDelete") or {}
            for err in mdel.get("userErrors") or []:
                print("Shopify userError (delete):", err, flush=True)
                errors += 1
            deleted = mdel.get("deletedMetafields") or []
            n_del_ok += len(deleted)
            print(
                f"Batch {batch_num} (wissen): {len(deleted)} metafields verwijderd.",
                flush=True,
            )
        else:
            mfs = [
                {
                    "ownerId": f"gid://shopify/ProductVariant/{op[2]}",
                    "namespace": ns,
                    "key": key,
                    "type": "date",
                    "value": op[3],
                }
                for op in batch
            ]
            data = graphql_metafields_set(shop, token, api_ver, mfs)
            mset = (data or {}).get("metafieldsSet") or {}
            for err in mset.get("userErrors") or []:
                print("Shopify userError (set):", err, flush=True)
                errors += 1
            mf = mset.get("metafields") or []
            n_set_ok += len(mf)
            print(
                f"Batch {batch_num} (zetten): {len(mf)} metafields gezet.",
                flush=True,
            )
        time.sleep(0.25)

    _print_summary_footer(
        csv_path=csv_path,
        ns=ns,
        key=key,
        n_csv_skus=len(sku_to_eta),
        n_no_variant=n_no_variant,
        n_set_ok=n_set_ok,
        n_del_ok=n_del_ok,
        n_set_planned=n_set_planned,
        n_clear_planned=n_clear_planned,
        errors=errors,
        limited=limited,
        dry_run=False,
        planned_set_full=n_set_planned_full if limited else None,
        planned_clear_full=n_clear_planned_full if limited else None,
    )
    return 0 if errors == 0 else 2


def _print_summary_footer(
    csv_path: Path,
    ns: str,
    key: str,
    n_csv_skus: int,
    n_no_variant: int,
    n_set_ok: int,
    n_del_ok: int,
    n_set_planned: int,
    n_clear_planned: int,
    errors: int,
    limited: bool,
    dry_run: bool = False,
    planned_set_full: int | None = None,
    planned_clear_full: int | None = None,
) -> None:
    """Vaste eindblok voor logs en automatisering."""
    print("", flush=True)
    print("=" * 60, flush=True)
    if dry_run:
        print("SAMENVATTING (dry-run — geen wijzigingen in Shopify)", flush=True)
    else:
        print("SAMENVATTING", flush=True)
    print("=" * 60, flush=True)
    print(f"  CSV:                    {csv_path}", flush=True)
    print(f"  Metafield:              {ns!r}.{key!r}", flush=True)
    print(f"  Unieke SKU's in CSV:    {n_csv_skus}", flush=True)
    if dry_run:
        print(
            f"  ETA zetten (gepland):   {n_set_planned}",
            flush=True,
        )
        print(
            f"  Metafield wissen (gepland): {n_clear_planned}",
            flush=True,
        )
    else:
        print(
            f"  ETA-datum gezet (API):  {n_set_ok}  (gepland deze run: {n_set_planned})",
            flush=True,
        )
        print(
            f"  Metafield gewist (API): {n_del_ok}  (gepland deze run: {n_clear_planned})",
            flush=True,
        )
    print(
        f"  SKU's zonder variant:   {n_no_variant}  (in CSV, niet in shop-cache)",
        flush=True,
    )
    if not dry_run:
        print(f"  Shopify userErrors:     {errors}", flush=True)
    if limited and planned_set_full is not None and planned_clear_full is not None:
        print(
            f"  Gepland (volledige CSV): {planned_set_full} ETA zetten, "
            f"{planned_clear_full} wissen (vóór --limit)",
            flush=True,
        )
    if limited:
        print("  Let op:                 run was beperkt met --limit", flush=True)
    print("=" * 60, flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
