#!/usr/bin/env python3
"""
Stand-alone: leest KTM prijs-CSV (kolommen ArticleNumber + hqETADate) en werkt het
variant-metafield inventory_policy_eta_date in Shopify bij (type date, ISO YYYY-MM-DD).

- Datum **vandaag of in de toekomst** → metafield zetten op die datum.
- Datum **in het verleden** → metafield **verwijderen** (veld leeg in Admin).
- **Lege** hqETADate → metafield **verwijderen** (zelfde als verleden).

**SKU → variant-id** komt uit een cachebestand (geen live variant-fetch in dit script).
Bouw of ververs die cache met:
  python3 scripts/shopify_refresh_variant_cache.py

Delta t.o.v. **cache/shopify_pricelist_sync_state.json** (zelfde als `shopify_sync_from_pricelist_csv.py`):
alleen mutaties waar de gewenste ETA nog niet in die state staat; na elke geslaagde batch wordt de
state bijgewerkt. `--force` = alles opnieuw pushen (state negeren).

CSV-scheidingsteken (komma vs `;`) via `pricing_loader.detect_0150_csv_delimiter`.

Configuratie (omgevingsvariabelen, o.a. via project-root .env):
  SHOPIFY_ACCESS_TOKEN   — verplicht
  SHOPIFY_SHOP_DOMAIN      — default ktm-shop-nl.myshopify.com
  SHOPIFY_ADMIN_API_VERSION — default 2024-10
  SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE — default global (moet gelijk zijn aan de definitie in
    Admin: Settings → Custom data → Variants; jullie gebruiken global.inventory_policy_eta_date)
  SHOPIFY_VARIANT_ETA_METAFIELD_KEY — default inventory_policy_eta_date

CLI:
  python3 scripts/shopify_sync_eta_from_pricelist_csv.py
  python3 scripts/shopify_sync_eta_from_pricelist_csv.py --csv input/0150_35_Z1_EUR_EN_csv.csv
  python3 scripts/shopify_sync_eta_from_pricelist_csv.py --dry-run
  python3 scripts/shopify_sync_eta_from_pricelist_csv.py --variant-cache pad/naar/sku_variant.json
  python3 scripts/shopify_sync_eta_from_pricelist_csv.py --force   # state negeren, volledige ETA-push

Later uitbreidbaar (prijzen + cron) in hetzelfde bestand of een wrapper.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
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
DEFAULT_STATE_FILE = PROJECT_ROOT / "cache" / "shopify_pricelist_sync_state.json"
LEGACY_STATE_FILE = PROJECT_ROOT / "cache" / "shopify_0150_sync_state.json"

DEFAULT_KTM_PRICE_CSV_NAMES: tuple[str, ...] = (
    "1100_35_Z1_EUR_EN_csv.csv",
    "0910_35_Z1_EUR_EN_csv.csv",
    "0150_35_Z1_EUR_EN_csv.csv",
    "0140_35_Z1_EUR_EN_csv.csv",
)


def migrate_legacy_state_file(target: Path) -> None:
    if target.is_file() or target.resolve() != DEFAULT_STATE_FILE.resolve():
        return
    if not LEGACY_STATE_FILE.is_file():
        return
    shutil.copy2(LEGACY_STATE_FILE, target)
    print(
        f"Delta-state gemigreerd: {LEGACY_STATE_FILE.name} → {target.name}",
        flush=True,
    )


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


def _graphql_post_with_retries(
    sess: requests.Session,
    url: str,
    headers: dict,
    json_body: dict,
) -> requests.Response:
    """POST met retries bij timeout/verbindingsfout (bijv. kort internet weg)."""
    payload = json.dumps(json_body)
    n_net = 0
    while True:
        try:
            return sess.post(
                url,
                headers=headers,
                data=payload,
                timeout=(25, 180),
                proxies={"http": None, "https": None},
            )
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
        ) as e:
            n_net += 1
            if n_net > 40:
                raise
            w = min(3.0 + n_net * 0.6, 120.0)
            print(
                f"GraphQL netwerk ({n_net}/40) {type(e).__name__}, {w:.1f}s…",
                flush=True,
            )
            time.sleep(w)


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
    for name in DEFAULT_KTM_PRICE_CSV_NAMES:
        p = input_dir / name
        if p.is_file():
            return p.resolve()
    for name in sorted(os.listdir(input_dir)):
        if not name.endswith(".csv"):
            continue
        if name.endswith("_Z1_EUR_EN_csv.csv"):
            return (input_dir / name).resolve()
    raise FileNotFoundError(
        f"Geen KTM prijs-CSV in {input_dir} (verwacht o.a. {', '.join(DEFAULT_KTM_PRICE_CSV_NAMES)}); "
        "gebruik --csv PAD"
    )


def load_state(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def merge_eta_state(state: dict, sku: str, vid: str, iso: str | None) -> None:
    """Zelfde velden als shopify_sync_from_pricelist_csv na geslaagde ETA-mutatie."""
    entry = dict(state.get(sku) or {})
    ev = dict(entry.get("eta_variants") or {})
    s = str(vid)
    if iso is None:
        ev.pop(s, None)
    else:
        ev[s] = iso
    entry["eta_variants"] = ev
    if iso is not None:
        entry["eta_iso"] = iso
    elif not ev:
        entry["eta_iso"] = None
    state[sku] = entry


def current_eta_from_state(
    state: dict, sku: str, vid: str, all_vids_for_sku: list[str]
) -> str | None:
    st = state.get(sku) or {}
    ev = st.get("eta_variants") or {}
    svid = str(vid)
    if svid in ev:
        return ev[svid]
    if len(all_vids_for_sku) <= 1:
        return st.get("eta_iso")
    return None


def filter_eta_ops_by_state(
    ops: list[tuple[str, str, str, str]],
    state: dict,
    sku_to_vid: dict[str, list[str]],
) -> tuple[list[tuple[str, str, str, str]], int]:
    """Laat alleen mutaties door die nog niet in state staan. Returns (ops, n_skipped)."""
    out: list[tuple[str, str, str, str]] = []
    skipped = 0
    for op in ops:
        kind, sku, vid, val = op
        vids = sku_to_vid.get(sku) or []
        cur = current_eta_from_state(state, sku, vid, vids)
        if kind == "set":
            if cur == val:
                skipped += 1
                continue
        else:
            if cur is None:
                skipped += 1
                continue
        out.append(op)
    return out, skipped


def variant_cache_json_to_sku_variant_ids(raw: dict) -> dict[str, list[str]]:
    """SKU -> alle variant-id's (lijst). Oude cache: één object of string → één id."""
    out: dict[str, list[str]] = {}
    for k, v in raw.items():
        sku = k.strip().upper()
        vids: list[str] = []
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict):
                    vid = str(item.get("variant_id") or "").strip()
                    if vid:
                        vids.append(vid)
        elif isinstance(v, dict):
            vid = str(v.get("variant_id") or "").strip()
            if vid:
                vids.append(vid)
        else:
            vid = str(v).strip()
            if vid:
                vids.append(vid)
        if vids:
            out[sku] = vids
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
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    while True:
        r = _graphql_post_with_retries(
            sess,
            url,
            headers,
            {"query": q, "variables": {"metafields": metafields}},
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
    headers = {
        "X-Shopify-Access-Token": token,
        "Content-Type": "application/json",
    }
    while True:
        r = _graphql_post_with_retries(
            sess,
            url,
            headers,
            {"query": q, "variables": {"metafields": identifiers}},
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
    sku_to_vids: dict[str, list[str]],
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
    n_no_variant = sum(
        1 for sku in sku_to_eta if sku not in sku_to_vids or not sku_to_vids[sku]
    )
    ops: list[tuple[str, str, str, str]] = []
    n_clear_empty = 0
    n_clear_past = 0
    for sku, val in sku_to_eta.items():
        vids = sku_to_vids.get(sku) or []
        if not vids:
            continue
        for vid in vids:
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
        description="KTM prijs-CSV hqETADate -> Shopify variant-metafield inventory_policy_eta_date"
    )
    p.add_argument(
        "--csv",
        metavar="PAD",
        help="KTM prijs-CSV (default: eerste bekende merk-export of *_Z1_EUR_EN_csv.csv in input/)",
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
    p.add_argument(
        "--state-file",
        type=Path,
        default=DEFAULT_STATE_FILE,
        help=f"Zelfde JSON als hoofdsync — delta op ETA (default: {DEFAULT_STATE_FILE})",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Geen delta: alle geplande ETA zetten/wissen uitvoeren (state niet gebruiken om te skippen)",
    )
    args = p.parse_args()

    migrate_legacy_state_file(args.state_file.resolve())

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = (
        (os.environ.get("SHOPIFY_ADMIN_API_VERSION") or "").strip() or "2024-10"
    )
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
            st = load_state(args.state_file.resolve())
            if args.force:
                ops_eff = ops_dr
                n_sk = 0
                print("  (--force: geen delta-filter in dry-run)", flush=True)
            else:
                ops_eff, n_sk = filter_eta_ops_by_state(ops_dr, st, sku_to_vid_dr)
                if n_sk:
                    print(
                        f"  (delta: {n_sk} variant-ETA's zouden worden overgeslagen — al in "
                        f"{args.state_file.resolve().name})",
                        flush=True,
                    )
            n_sp_eff = sum(1 for o in ops_eff if o[0] == "set")
            n_cp_eff = sum(1 for o in ops_eff if o[0] == "clear")
            _print_summary_footer(
                csv_path=csv_path,
                ns=ns,
                key=key,
                n_csv_skus=len(sku_to_eta),
                n_no_variant=n_no_dr,
                n_set_ok=0,
                n_del_ok=0,
                n_set_planned=n_sp_eff,
                n_clear_planned=n_cp_eff,
                errors=0,
                limited=False,
                dry_run=True,
                planned_set_full=n_sp,
                planned_clear_full=n_cp,
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
    state_path = args.state_file.resolve()
    state = load_state(state_path)
    n_skipped_state = 0
    if args.force:
        print("--force: volledige werklijst (geen overslaan op basis van state).", flush=True)
    else:
        ops, n_skipped_state = filter_eta_ops_by_state(ops, state, sku_to_vid)
        if n_skipped_state:
            print(
                f"Delta: {n_skipped_state} variant-ETA's overgeslagen (al in sync volgens "
                f"{state_path.name}).",
                flush=True,
            )
    n_set_after_delta = sum(1 for o in ops if o[0] == "set")
    n_clear_after_delta = sum(1 for o in ops if o[0] == "clear")

    if not ops:
        if n_set_planned_full + n_clear_planned_full > 0 and n_skipped_state:
            print(
                "Geen resterende ETA-mutaties: alles in de CSV staat al in de sync-state "
                f"({state_path.name}). Gebruik --force om alle metafields opnieuw te zetten/wissen.",
                flush=True,
            )
        else:
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
        f"Te verwerken (na delta): {n_set_after_delta} ETA zetten, "
        f"{n_clear_after_delta} metafield wissen.",
        flush=True,
    )
    print(
        f"  Bruto uit CSV+cache: {n_set_planned_full} zetten, {n_clear_planned_full} wissen "
        f"(waarvan {n_clear_empty} lege ETA, {n_clear_past} datum in verleden).",
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
            uerr = mdel.get("userErrors") or []
            for err in uerr:
                print("Shopify userError (delete):", err, flush=True)
                errors += 1
            deleted = mdel.get("deletedMetafields") or []
            n_del_ok += len(deleted)
            print(
                f"Batch {batch_num} (wissen): {len(deleted)} metafields verwijderd.",
                flush=True,
            )
            if not uerr:
                for op in batch:
                    merge_eta_state(state, op[1], op[2], None)
                save_state(state_path, state)
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
            uerr = mset.get("userErrors") or []
            for err in uerr:
                print("Shopify userError (set):", err, flush=True)
                errors += 1
            mf = mset.get("metafields") or []
            n_set_ok += len(mf)
            print(
                f"Batch {batch_num} (zetten): {len(mf)} metafields gezet.",
                flush=True,
            )
            if not uerr:
                for op in batch:
                    merge_eta_state(state, op[1], op[2], op[3])
                save_state(state_path, state)
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
