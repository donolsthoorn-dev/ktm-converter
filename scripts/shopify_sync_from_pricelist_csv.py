#!/usr/bin/env python3
"""
KTM-prijs-CSV(s) → Shopify: alleen wijzigingen doorzetten (delta) t.o.v. de vorige succesvolle run.

Prijzen, ETA en publicatiestatus (draft/active) gaan **uitsluitend** via dit script; de delta is
altijd: gewenste waarde uit de CSV-bron(zen) vergeleken met cache/shopify_pricelist_sync_state.json
(geen massale uitlees van Shopify per run).

**Standaard** worden (als ze in `input/` bestaan) vier merk-exports **samengevoegd** — **laatste
bestand wint** bij dezelfde SKU: `1100_35_Z1_EUR_EN_csv.csv`, `0910_35_Z1_EUR_EN_csv.csv`,
`0150_35_Z1_EUR_EN_csv.csv`, `0140_35_Z1_EUR_EN_csv.csv`. Met `--csv PAD` (meerdere keren) kies je
zelf bestanden en volgorde.

Leest hetzelfde KTM CSV-formaat als pricing_loader (hqETADate, SalesPrice, ArticleStatus, …):
  - hqETADate      → variant-metafield global.inventory_policy_eta_date (type date)
  - SalesPrice     → variantprijs (CSV ex-BTW × VAT_MULTIPLIER, gelijk aan Shopify incl. BTW)
  - ArticleStatus  → als waarde 80: product op draft (niet gepubliceerd); anders active

Variant-SKU → lijst (variant_id, product_id) uit cache (alle dubbele SKU’s; geen live variant-fetch per run):
  python3 scripts/shopify_refresh_variant_cache.py

Status van de laatst succesvol toegepaste waarden per SKU staat in:
  cache/shopify_pricelist_sync_state.json
Het bestand wordt tussentijds weggeschreven (o.a. na elke geslaagde ETA-batch en periodiek bij
prijs/product), zodat een onderbroken run niet opnieuw alle ETA-batches hoeft te doen.

Typische workflow:
  1) Eén keer (of na nieuwe producten): python3 scripts/shopify_refresh_variant_cache.py
  2) Meerdere keren per dag: python3 scripts/shopify_sync_from_pricelist_csv.py

Eerste keer state vullen zonder alles te uploaden:
  • Basis = **Shopify product-export** (prijs/status): scripts/bootstrap_state_from_shopify_export.py
  • Basis = **KTM prijs-CSV** (ERP): scripts/bootstrap_pricelist_sync_state_from_csv.py
  (variant-ID-cache komt alleen uit shopify_refresh_variant_cache.py, niet uit een CSV.)

Opties: --csv (herhaalbaar), --dry-run, --force, --variant-cache, --state-file, --price-workers,
  --graphql-inflight (max gelijktijdige GraphQL-requests; default env SHOPIFY_GRAPHQL_INFLIGHT=4)
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import random
import sys
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from config import VAT_MULTIPLIER  # noqa: E402
from modules.pricing_loader import detect_0150_csv_delimiter  # noqa: E402

DEFAULT_VARIANT_CACHE = PROJECT_ROOT / "cache" / "shopify_eta_sync_sku_variant.json"
DEFAULT_STATE_FILE = PROJECT_ROOT / "cache" / "shopify_pricelist_sync_state.json"
LEGACY_STATE_FILE = PROJECT_ROOT / "cache" / "shopify_0150_sync_state.json"

# Standaard merge-volgorde: later in de lijst overschrijft bij dubbele ArticleNumber tussen bestanden.
DEFAULT_KTM_PRICE_CSV_NAMES: tuple[str, ...] = (
    "1100_35_Z1_EUR_EN_csv.csv",
    "0910_35_Z1_EUR_EN_csv.csv",
    "0150_35_Z1_EUR_EN_csv.csv",
    "0140_35_Z1_EUR_EN_csv.csv",
)


def migrate_legacy_state_file(target: Path) -> None:
    """Eenmalig: oude bestandsnaam shopify_0150_sync_state.json → shopify_pricelist_sync_state.json."""
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
    """Eén CSV-pad (legacy / bootstrap)."""
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


def resolve_csv_paths(explicit: list[str] | None) -> list[Path]:
    """
    Lijst van prijs-CSV-paden. Zonder --csv: de vier standaard KTM-bestanden die in input/ bestaan
    (merge in vaste volgorde; ontbrekende namen worden overgeslagen met waarschuwing), anders
    fallback naar één automatisch prijs-CSV-bestand. Met expliciete paden: alle moeten bestaan.
    """
    input_dir = PROJECT_ROOT / "input"
    if explicit:
        out: list[Path] = []
        for s in explicit:
            p = Path(s)
            if not p.is_file():
                p = input_dir / s
            if not p.is_file():
                raise FileNotFoundError(f"CSV niet gevonden: {s}")
            out.append(p.resolve())
        return out

    found: list[Path] = []
    missing: list[str] = []
    for name in DEFAULT_KTM_PRICE_CSV_NAMES:
        p = input_dir / name
        if p.is_file():
            found.append(p.resolve())
        else:
            missing.append(name)
    if found:
        if missing:
            print(
                "Waarschuwing: deze prijs-CSV's ontbreken in input/ en worden overgeslagen: "
                + ", ".join(missing),
                flush=True,
            )
        return found
    return [resolve_csv_path(None)]


def _parse_sales_price_incl_vat(raw: str) -> str | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    try:
        base = float(raw.replace(",", "."))
        final = round(base * VAT_MULTIPLIER, 2)
        return f"{final:.2f}"
    except ValueError:
        return None


def read_pricelist_csv_desired(csv_path: Path, today: date) -> dict[str, dict]:
    """
    Per SKU (uppercase): eta_iso (None = metafield wissen), price_incl (None = geen prijsupdate),
    product_status ACTIVE|DRAFT.
    """
    encodings = ("utf-8", "utf-8-sig", "cp1252", "latin1")
    out: dict[str, dict] = {}
    for enc in encodings:
        try:
            with open(csv_path, newline="", encoding=enc) as fh:
                first = fh.readline()
                fh.seek(0)
                delim = detect_0150_csv_delimiter(first)
                reader = csv.reader(fh, delimiter=delim)
                header = next(reader, None)
                if not header:
                    return {}
                try:
                    eta_col = header.index("hqETADate")
                    sku_col = header.index("ArticleNumber")
                    price_col = header.index("SalesPrice")
                    status_col = header.index("ArticleStatus")
                except ValueError:
                    eta_col, sku_col, price_col, status_col = 22, 1, 4, 10
                for row in reader:
                    need = max(eta_col, sku_col, price_col, status_col) + 1
                    if len(row) < need:
                        continue
                    sku = row[sku_col].strip().upper()
                    if not sku:
                        continue
                    raw_eta = (row[eta_col] or "").strip()
                    if not raw_eta:
                        eta_iso: str | None = None
                    else:
                        parsed = parse_hq_eta_to_iso(raw_eta)
                        if parsed is None:
                            continue
                        eta_iso = parsed
                        if date.fromisoformat(parsed) < today:
                            eta_iso = None
                    price_incl = _parse_sales_price_incl_vat(row[price_col] or "")
                    st = (row[status_col] or "").strip()
                    product_status = "DRAFT" if st == "80" else "ACTIVE"
                    out[sku] = {
                        "eta_iso": eta_iso,
                        "price_incl": price_incl,
                        "product_status": product_status,
                    }
            return out
        except UnicodeDecodeError:
            continue
    raise OSError(f"CSV kon niet worden gelezen: {csv_path}")


def read_pricelist_csv_desired_many(csv_paths: list[Path], today: date) -> dict[str, dict]:
    """
    Leest meerdere KTM-exporten; bij dezelfde SKU wint de **laatste** file in de lijst
    (zelfde semantiek als meerdere regels in één CSV: laatste wint).
    """
    merged: dict[str, dict] = {}
    for p in csv_paths:
        merged.update(read_pricelist_csv_desired(p, today))
    return merged


def load_variant_cache(path: Path) -> dict[str, list[tuple[str, str | None]]]:
    """
    SKU uppercase -> lijst (variant_id, product_id).
    Oude cache: één object {variant_id, product_id} of alleen variant-id string → één element in de lijst.
    Nieuwe cache: lijst van objecten (alle varianten met dezelfde SKU).
    """
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    out: dict[str, list[tuple[str, str | None]]] = {}

    def _pid(v) -> str | None:
        if v is None:
            return None
        return str(int(v)) if isinstance(v, (int, float)) else str(v).strip()

    for k, v in raw.items():
        sku = k.strip().upper()
        pairs: list[tuple[str, str | None]] = []

        def add(vid: str, pid_s: str | None) -> None:
            if vid:
                pairs.append((vid, pid_s if pid_s else None))

        if isinstance(v, list):
            for item in v:
                if not isinstance(item, dict):
                    continue
                vid = str(item.get("variant_id") or "").strip()
                add(vid, _pid(item.get("product_id")))
        elif isinstance(v, dict):
            vid = str(v.get("variant_id") or "").strip()
            if vid or "product_id" in v:
                add(vid, _pid(v.get("product_id")))
        else:
            add(str(v).strip(), None)

        if pairs:
            out[sku] = pairs
    return out


def product_id_for_variant(
    sku_to_vp: dict[str, list[tuple[str, str | None]]], sku: str, variant_id: str
) -> str | None:
    for vid, pid in sku_to_vp.get(sku, []):
        if vid == variant_id:
            return pid
    return None


def load_state(path: Path) -> dict:
    if not path.is_file():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}


def _graphql_errors_throttled(errors: list | None) -> bool:
    if not errors:
        return False
    for e in errors:
        if not isinstance(e, dict):
            continue
        ext = e.get("extensions") or {}
        if ext.get("code") == "THROTTLED":
            return True
        msg = (e.get("message") or "").lower()
        if "throttl" in msg:
            return True
    return False


_gql_inflight: threading.Semaphore | None = None
_gql_inflight_lock = threading.Lock()


def configure_graphql_inflight(limit: int) -> None:
    """Max parallel GraphQL HTTP-requests (alle threads). Eerste aanroep wint."""
    global _gql_inflight
    with _gql_inflight_lock:
        if _gql_inflight is not None:
            return
        _gql_inflight = threading.Semaphore(max(1, min(limit, 32)))


def _gql_acquire() -> None:
    global _gql_inflight
    if _gql_inflight is None:
        with _gql_inflight_lock:
            if _gql_inflight is None:
                n = int(os.environ.get("SHOPIFY_GRAPHQL_INFLIGHT", "4"))
                _gql_inflight = threading.Semaphore(max(1, min(n, 32)))
    _gql_inflight.acquire()


def _gql_release() -> None:
    _gql_inflight.release()


def save_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def graphql_post(
    shop: str,
    token: str,
    api_version: str,
    query: str,
    variables: dict | None = None,
    sess: requests.Session | None = None,
) -> dict:
    sess = sess or _http_session()
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    body: dict = {"query": query}
    if variables is not None:
        body["variables"] = variables
    n429 = 0
    n_throttled = 0
    n_transient = 0
    while True:
        _gql_acquire()
        try:
            r = sess.post(
                url,
                headers={
                    "X-Shopify-Access-Token": token,
                    "Content-Type": "application/json",
                },
                data=json.dumps(body),
                timeout=(15, 120),
                proxies={"http": None, "https": None},
            )
        finally:
            _gql_release()
        if r.status_code == 429:
            n429 += 1
            if n429 > 40:
                raise RuntimeError("GraphQL 429: te veel retries")
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    w = float(ra)
                except ValueError:
                    w = 2.0
            else:
                w = min(2.0 + n429 * 0.5, 60.0)
            print(f"GraphQL rate limit ({n429}/40), {w:.1f}s wachten…", flush=True)
            time.sleep(w)
            continue
        n429 = 0
        if r.status_code >= 500:
            n_transient += 1
            if n_transient > 30:
                r.raise_for_status()
            w = min(2.0 + n_transient * 0.75, 90.0) + random.uniform(0, 0.5)
            if n_transient <= 3 or n_transient % 8 == 0:
                print(
                    f"GraphQL HTTP {r.status_code} ({n_transient}/30), {w:.1f}s wachten…",
                    flush=True,
                )
            time.sleep(w)
            continue
        r.raise_for_status()
        try:
            out = r.json()
        except json.JSONDecodeError:
            n_transient += 1
            if n_transient > 30:
                raise RuntimeError(
                    f"GraphQL: ongeldig JSON-antwoord (HTTP {r.status_code})"
                ) from None
            w = min(2.0 + n_transient * 0.75, 90.0) + random.uniform(0, 0.5)
            if n_transient <= 3 or n_transient % 8 == 0:
                print(
                    f"GraphQL JSON parse ({n_transient}/30), {w:.1f}s wachten…",
                    flush=True,
                )
            time.sleep(w)
            continue
        n_transient = 0
        errs = out.get("errors")
        if errs and _graphql_errors_throttled(errs):
            n_throttled += 1
            if n_throttled > 50:
                raise RuntimeError(
                    "GraphQL THROTTLED: te veel retries\n" + json.dumps(errs, indent=2)
                )
            w = min(1.5 + n_throttled * 0.4, 25.0) + random.uniform(0, 0.35)
            if n_throttled <= 3 or n_throttled % 12 == 0:
                print(
                    f"GraphQL THROTTLED ({n_throttled}/50), {w:.1f}s wachten…",
                    flush=True,
                )
            time.sleep(w)
            continue
        n_throttled = 0
        if errs:
            raise RuntimeError(json.dumps(errs, indent=2))
        return out.get("data") or {}


def graphql_product_variants_bulk_update(
    shop: str,
    token: str,
    api_version: str,
    product_id_numeric: str,
    variant_id_prices: list[tuple[str, str]],
    sess: requests.Session | None = None,
) -> tuple[bool, str]:
    """
    Zet prijzen voor meerdere varianten van één product in één GraphQL-call
    (veel minder REST-429 dan PUT per variant).
    """
    if not variant_id_prices:
        return True, ""
    q = """
mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
  productVariantsBulkUpdate(productId: $productId, variants: $variants) {
    productVariants { id }
    userErrors { field message }
  }
}
"""
    pid_gid = f"gid://shopify/Product/{product_id_numeric}"
    variants = [
        {"id": f"gid://shopify/ProductVariant/{vid}", "price": price}
        for vid, price in variant_id_prices
    ]
    data = graphql_post(
        shop,
        token,
        api_version,
        q,
        {"productId": pid_gid, "variants": variants},
        sess=sess,
    )
    payload = (data or {}).get("productVariantsBulkUpdate") or {}
    uerr = payload.get("userErrors") or []
    if uerr:
        return False, str(uerr)
    return True, ""


def _run_price_bulk_for_product(
    shop: str,
    token: str,
    api_ver: str,
    pid: str,
    items: list[tuple[str, str, str]],
    max_variants: int,
) -> tuple[int, int, list[tuple[str, str, str]]]:
    """
    Eén product (alle prijswijzigingen voor dat product). Eigen HTTP-sessies (thread-safe).
    Returns: (n_done, n_errors, state_updates als (sku, variant_id, price_incl)).
    """
    gql_sess = _http_session()
    rest_sess = _http_session()
    n_done = 0
    n_err = 0
    updates: list[tuple[str, str, str]] = []
    for start in range(0, len(items), max_variants):
        chunk = items[start : start + max_variants]
        vid_prices = [(c[1], c[2]) for c in chunk]
        ok, err_msg = graphql_product_variants_bulk_update(
            shop, token, api_ver, pid, vid_prices, sess=gql_sess
        )
        if ok:
            for sku, vid, price in chunk:
                updates.append((sku, vid, price))
            n_done += len(chunk)
        else:
            n_err += 1
            print(f"  Bulk prijs-fout product {pid}: {err_msg[:300]}", flush=True)
            for sku, vid, price in chunk:
                if rest_variant_price(shop, token, api_ver, vid, price, sess=rest_sess):
                    updates.append((sku, vid, price))
                    n_done += 1
                else:
                    n_err += 1
    return n_done, n_err, updates


def graphql_metafields_set(shop: str, token: str, api_version: str, metafields: list[dict]) -> dict:
    q = """
mutation metafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { id namespace key }
    userErrors { field message }
  }
}
"""
    return graphql_post(shop, token, api_version, q, {"metafields": metafields})


def graphql_metafields_delete(
    shop: str, token: str, api_version: str, identifiers: list[dict]
) -> dict:
    q = """
mutation metafieldsDelete($metafields: [MetafieldIdentifierInput!]!) {
  metafieldsDelete(metafields: $metafields) {
    deletedMetafields { key namespace ownerId }
    userErrors { field message }
  }
}
"""
    return graphql_post(shop, token, api_version, q, {"metafields": identifiers})


def graphql_product_variant_product_id(
    shop: str, token: str, api_version: str, variant_id: str
) -> str | None:
    q = """
query($id: ID!) {
  productVariant(id: $id) {
    product { id }
  }
}
"""
    gid = f"gid://shopify/ProductVariant/{variant_id}"
    data = graphql_post(shop, token, api_version, q, {"id": gid})
    node = (data or {}).get("productVariant") or {}
    prod = node.get("product") or {}
    pid_gid = prod.get("id") or ""
    if not pid_gid or "/" not in pid_gid:
        return None
    return pid_gid.rsplit("/", 1)[-1]


def rest_put_json(
    shop: str,
    token: str,
    api_version: str,
    url: str,
    payload: dict,
    sess: requests.Session | None = None,
) -> tuple[bool, str]:
    """PUT met hergebruikte sessie (keep-alive); retry-limiet i.p.v. oneindige loop."""
    sess = sess or _http_session()
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    n429 = 0
    n500 = 0
    n_net = 0
    # Connect / read timeout: lange reads bij grote product-payloads zijn zeldzaam bij status-PUT.
    _timeout = (15, 60)
    while True:
        try:
            r = sess.put(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=_timeout,
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
                flush=True,
            )
            time.sleep(w)
            continue
        n_net = 0
        if r.status_code == 429:
            n429 += 1
            if n429 > 30:
                return False, "429 rate limit: te veel retries"
            print(f"REST rate limit ({n429}/30), wachten…", flush=True)
            time.sleep(min(2.0 + n429 * 0.3, 45.0))
            continue
        if r.status_code >= 500:
            n500 += 1
            if n500 > 10:
                return False, r.text[:500]
            print(f"Shopify serverfout ({n500}/10), retry…", flush=True)
            time.sleep(3)
            continue
        n500 = 0
        if r.status_code >= 400:
            return False, r.text[:500]
        return True, ""


def rest_variant_price(
    shop: str,
    token: str,
    api_version: str,
    variant_id: str,
    price: str,
    sess: requests.Session | None = None,
) -> bool:
    try:
        vid_int = int(variant_id)
    except ValueError:
        return False
    url = f"https://{shop}/admin/api/{api_version}/variants/{vid_int}.json"
    ok, err = rest_put_json(
        shop,
        token,
        api_version,
        url,
        {"variant": {"id": vid_int, "price": price}},
        sess=sess,
    )
    if not ok:
        print(f"  Variant prijs-fout {variant_id}: {err}", flush=True)
    return ok


def rest_product_status(
    shop: str,
    token: str,
    api_version: str,
    product_id: str,
    status: str,
    sess: requests.Session | None = None,
) -> bool:
    """status: 'active' of 'draft'."""
    try:
        pid_int = int(product_id)
    except ValueError:
        return False
    st = status.lower()
    if st not in ("active", "draft"):
        st = "active"
    url = f"https://{shop}/admin/api/{api_version}/products/{pid_int}.json"
    ok, err = rest_put_json(
        shop,
        token,
        api_version,
        url,
        {"product": {"id": pid_int, "status": st}},
        sess=sess,
    )
    if not ok:
        print(f"  Product status-fout {product_id}: {err}", flush=True)
    return ok


def needs_update(
    force: bool,
    state: dict,
    sku: str,
    key: str,
    desired,
) -> bool:
    if force:
        return True
    prev = (state.get(sku) or {}).get(key)
    return prev != desired


def needs_update_price(
    force: bool,
    state: dict,
    sku: str,
    desired: str | None,
    variant_ids: list[str],
) -> bool:
    if force:
        return True
    if desired is None:
        return False
    st = state.get(sku) or {}
    pv = st.get("price_variants") or {}
    legacy = st.get("price_incl")
    vids = [str(x) for x in variant_ids]
    if len(vids) <= 1:
        vid = vids[0] if vids else None
        if vid and vid in pv:
            return pv[vid] != desired
        return legacy != desired
    for vid in vids:
        if pv.get(vid) != desired:
            return True
    return False


def needs_update_eta(
    force: bool,
    state: dict,
    sku: str,
    desired: str | None,
    variant_ids: list[str],
) -> bool:
    if force:
        return True
    st = state.get(sku) or {}
    ev = st.get("eta_variants") or {}
    legacy = st.get("eta_iso")
    vids = [str(x) for x in variant_ids]

    if desired is None:
        for vid in vids:
            cur = ev.get(vid)
            if cur is None and len(vids) == 1:
                cur = legacy
            if cur is not None:
                return True
        return False

    for vid in vids:
        cur = ev.get(vid)
        if cur is None and len(vids) == 1:
            cur = legacy
        if cur is None and len(vids) > 1:
            return True
        if cur != desired:
            return True
    return False


def needs_update_product_status(
    force: bool,
    state: dict,
    sku: str,
    desired: str,
    product_ids: list[str],
) -> bool:
    if force:
        return True
    st = state.get(sku) or {}
    pb = st.get("product_status_by_product") or {}
    legacy = st.get("product_status")
    pids = [str(x) for x in product_ids if x]
    if len(pids) <= 1:
        pid = pids[0] if pids else None
        if pid and pid in pb:
            return pb[pid] != desired
        return legacy != desired
    for pid in pids:
        if pb.get(pid) != desired:
            return True
    return False


def _merge_state(state: dict, sku: str, updates: dict) -> None:
    entry = dict(state.get(sku) or {})
    for k, v in updates.items():
        if k == "price_variants" and isinstance(v, dict):
            prev = dict(entry.get("price_variants") or {})
            prev.update(v)
            entry["price_variants"] = prev
        elif k == "eta_variants" and isinstance(v, dict):
            prev = dict(entry.get("eta_variants") or {})
            for k2, v2 in v.items():
                if v2 is None:
                    prev.pop(str(k2), None)
                else:
                    prev[str(k2)] = v2
            entry["eta_variants"] = prev
        elif k == "product_status_by_product" and isinstance(v, dict):
            prev = dict(entry.get("product_status_by_product") or {})
            prev.update(v)
            entry["product_status_by_product"] = prev
        else:
            entry[k] = v
    state[sku] = entry


def main() -> int:
    load_dotenv()

    p = argparse.ArgumentParser(
        description="KTM prijs-CSV → Shopify: ETA, prijs (×BTW), draft bij status 80 — alleen bij wijziging"
    )
    p.add_argument(
        "--csv",
        action="append",
        metavar="PAD",
        dest="csv_paths",
        help=(
            "KTM prijs-CSV (zelfde kolommen als ERP-export; herhaalbaar voor merge). "
            f"Default zonder deze vlag: merge van {', '.join(DEFAULT_KTM_PRICE_CSV_NAMES)} "
            "die in input/ bestaan (volgorde = 1100 → 0910 → 0150 → 0140; laatste wint bij dubbele SKU), "
            "of anders eerste *_Z1_EUR_EN_csv.csv in input/."
        ),
    )
    p.add_argument("--dry-run", action="store_true", help="Geen API-calls; wel delta-tonen")
    p.add_argument(
        "--force",
        action="store_true",
        help="State negeren: alles toepassen zoals uit CSV (geen delta)",
    )
    p.add_argument("--variant-cache", type=Path, default=DEFAULT_VARIANT_CACHE)
    p.add_argument("--state-file", type=Path, default=DEFAULT_STATE_FILE)
    p.add_argument(
        "--limit", type=int, default=0, help="Max. aantal SKU's met wijzigingen (0=alles)"
    )
    p.add_argument(
        "--price-workers",
        type=int,
        default=0,
        metavar="N",
        help="Parallelle producten voor prijs-updates (0 = env SHOPIFY_PRICE_CONCURRENCY, default 4)",
    )
    p.add_argument(
        "--graphql-inflight",
        type=int,
        default=0,
        metavar="N",
        help="Max gelijktijdige GraphQL-requests (0 = env SHOPIFY_GRAPHQL_INFLIGHT, default 4)",
    )
    args = p.parse_args()

    state_path = args.state_file.resolve()
    migrate_legacy_state_file(state_path)

    graphql_inflight = args.graphql_inflight
    if graphql_inflight <= 0:
        graphql_inflight = int(os.environ.get("SHOPIFY_GRAPHQL_INFLIGHT", "4"))
    graphql_inflight = max(1, min(graphql_inflight, 16))
    configure_graphql_inflight(graphql_inflight)

    token = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
    shop = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
    api_ver = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "2024-10").strip()
    ns = os.environ.get("SHOPIFY_VARIANT_ETA_METAFIELD_NAMESPACE", "global").strip()
    key_mf = os.environ.get(
        "SHOPIFY_VARIANT_ETA_METAFIELD_KEY", "inventory_policy_eta_date"
    ).strip()

    if not args.dry_run and not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env of export).", flush=True)
        return 1

    csv_paths = resolve_csv_paths(args.csv_paths)
    today = date.today()
    desired_by_sku = read_pricelist_csv_desired_many(csv_paths, today)
    print(
        "CSV-bronnen (volgorde: latere bestand wint bij dubbele SKU tussen bestanden):",
        flush=True,
    )
    for cp in csv_paths:
        print(f"  • {cp}", flush=True)
    print(
        f"→ {len(desired_by_sku)} unieke SKU-regels na merge (binnen één bestand: laatste wint bij dubbel)",
        flush=True,
    )

    cache_path = args.variant_cache.resolve()
    if not cache_path.is_file():
        if args.dry_run:
            print(
                f"(Geen variant-cache op {cache_path} — geen overlap met Shopify-SKU's; "
                "leg aan met: python3 scripts/shopify_refresh_variant_cache.py)",
                flush=True,
            )
        else:
            print(
                f"Variant-cache ontbreekt: {cache_path}\n"
                "  python3 scripts/shopify_refresh_variant_cache.py",
                flush=True,
            )
            return 1

    sku_to_vp: dict[str, list[tuple[str, str | None]]] = {}
    if cache_path.is_file():
        sku_to_vp = load_variant_cache(cache_path)
        n_variants = sum(len(v) for v in sku_to_vp.values())
        print(
            f"Variant-cache: {len(sku_to_vp)} SKU's ({n_variants} varianten) — {cache_path}",
            flush=True,
        )

    state: dict = load_state(state_path) if not args.force else {}
    if args.force:
        print("--force: state genegeerd (volledige sync).", flush=True)
    else:
        print(f"Delta-state: {state_path} ({len(state)} SKU's)", flush=True)

    product_id_memo: dict[str, str] = {}

    def resolve_product_id(sku: str, variant_id: str) -> str | None:
        pid = product_id_for_variant(sku_to_vp, sku, variant_id)
        if pid:
            return pid
        if variant_id in product_id_memo:
            return product_id_memo[variant_id]
        if args.dry_run:
            return None
        pid = graphql_product_variant_product_id(shop, token, api_ver, variant_id)
        if pid:
            product_id_memo[variant_id] = pid
        return pid

    # Werklijst (prijs/ETA/status: delta = CSV vs state, zelfde model voor alles)
    eta_set: list[tuple[str, str, str]] = []  # sku, vid, iso
    eta_clear: list[tuple[str, str]] = []
    price_ops: list[tuple[str, str, str]] = []  # sku, vid, price
    product_ops: list[tuple[str, str, str]] = []  # sku, product_id, ACTIVE|DRAFT

    n_skip_no_variant = 0
    for sku, d in desired_by_sku.items():
        if sku not in sku_to_vp:
            n_skip_no_variant += 1
            continue
        pairs = sku_to_vp[sku]
        variant_ids = [p[0] for p in pairs]
        unique_product_ids = list(
            dict.fromkeys(p for _v, p in pairs if p)
        )

        if needs_update_eta(args.force, state, sku, d["eta_iso"], variant_ids):
            for vid, _pid in pairs:
                if d["eta_iso"] is None:
                    eta_clear.append((sku, vid))
                else:
                    eta_set.append((sku, vid, d["eta_iso"]))

        if d["price_incl"] is not None and needs_update_price(
            args.force, state, sku, d["price_incl"], variant_ids
        ):
            for vid, _pid in pairs:
                price_ops.append((sku, vid, d["price_incl"]))

        ps = d["product_status"]
        if needs_update_product_status(
            args.force, state, sku, ps, unique_product_ids
        ):
            if not unique_product_ids:
                if args.dry_run:
                    product_ops.append((sku, "", ps))
                else:
                    print(
                        f"  Waarschuwing: geen product_id in cache voor SKU {sku}; "
                        "ververs cache met shopify_refresh_variant_cache.py",
                        flush=True,
                    )
            else:
                seen_pid_local: set[str] = set()
                for _vid, pid in pairs:
                    if not pid or pid in seen_pid_local:
                        continue
                    seen_pid_local.add(pid)
                    if args.dry_run:
                        product_ops.append((sku, pid, ps))
                    else:
                        product_ops.append((sku, pid, ps))

    total_changes = len(eta_set) + len(eta_clear) + len(price_ops) + len(product_ops)
    print(
        f"Delta: ETA zetten {len(eta_set)}, ETA wissen {len(eta_clear)}, "
        f"prijs {len(price_ops)}, product status {len(product_ops)} "
        f"(totaal mutaties {total_changes})",
        flush=True,
    )
    if n_skip_no_variant:
        print(f"CSV-SKU's zonder variant in cache: {n_skip_no_variant}", flush=True)

    if args.limit > 0:
        # Beperk door sequentieel af te knippen (eenvoudig)
        combined = (
            [("eta_set", x) for x in eta_set]
            + [("eta_clear", x) for x in eta_clear]
            + [("price", x) for x in price_ops]
            + [("product", x) for x in product_ops]
        )
        combined = combined[: args.limit]
        eta_set = [x[1] for x in combined if x[0] == "eta_set"]
        eta_clear = [x[1] for x in combined if x[0] == "eta_clear"]
        price_ops = [x[1] for x in combined if x[0] == "price"]
        product_ops = [x[1] for x in combined if x[0] == "product"]
        print(f"--limit {args.limit}: uitgevoerde subset", flush=True)

    if args.dry_run:
        print("Dry-run: geen wijzigingen in Shopify.", flush=True)
        return 0

    errors = 0
    progress_every = 250  # voortgangsregel voor prijs/product (grote runs)

    # ETA batches (state na elke geslaagde batch naar schijf → volgende run herhaalt geen afgeronde batches)
    batch_size = 25
    i = 0
    all_eta_ops = [("set", t) for t in eta_set] + [("clear", t) for t in eta_clear]
    if all_eta_ops:
        print(
            "Tussentijdse state-opslag na elke geslaagde ETA-batch "
            "(veilig stoppen; hervatten = minder dubbel werk).",
            flush=True,
        )

    batch_num = 0
    while i < len(all_eta_ops):
        kind = all_eta_ops[i][0]
        batch = []
        while i < len(all_eta_ops) and len(batch) < batch_size and all_eta_ops[i][0] == kind:
            batch.append(all_eta_ops[i])
            i += 1
        batch_num += 1
        if kind == "clear":
            identifiers = [
                {
                    "ownerId": f"gid://shopify/ProductVariant/{op[1][1]}",
                    "namespace": ns,
                    "key": key_mf,
                }
                for op in batch
            ]
            data = graphql_metafields_delete(shop, token, api_ver, identifiers)
            mdel = (data or {}).get("metafieldsDelete") or {}
            uerr = mdel.get("userErrors") or []
            for err in uerr:
                print("Shopify userError (delete ETA):", err, flush=True)
                errors += 1
            deleted = mdel.get("deletedMetafields") or []
            if not uerr:
                for op in batch:
                    sku_c = op[1][0]
                    vid_c = op[1][1]
                    _merge_state(
                        state,
                        sku_c,
                        {"eta_iso": None, "eta_variants": {str(vid_c): None}},
                    )
                save_state(state_path, state)
            print(f"Batch {batch_num} ETA wissen: {len(deleted)} verwijderd.", flush=True)
        else:
            mfs = [
                {
                    "ownerId": f"gid://shopify/ProductVariant/{op[1][1]}",
                    "namespace": ns,
                    "key": key_mf,
                    "type": "date",
                    "value": op[1][2],
                }
                for op in batch
            ]
            data = graphql_metafields_set(shop, token, api_ver, mfs)
            mset = (data or {}).get("metafieldsSet") or {}
            uerr = mset.get("userErrors") or []
            for err in uerr:
                print("Shopify userError (set ETA):", err, flush=True)
                errors += 1
            if not uerr:
                for op in batch:
                    sku_s = op[1][0]
                    vid_s = op[1][1]
                    iso = op[1][2]
                    _merge_state(
                        state,
                        sku_s,
                        {"eta_iso": iso, "eta_variants": {str(vid_s): iso}},
                    )
                save_state(state_path, state)
            print(f"Batch {batch_num} ETA zetten: {len(mfs)} verzoeken.", flush=True)
        time.sleep(0.25)

    n_price = len(price_ops)
    price_workers = args.price_workers
    if price_workers <= 0:
        price_workers = int(os.environ.get("SHOPIFY_PRICE_CONCURRENCY", "4"))
    price_workers = max(1, min(price_workers, 16))
    if n_price:
        print(
            f"ETA afgerond. Start prijs-updates: {n_price} "
            f"(GraphQL bulk per product, {price_workers} workers, "
            f"max {graphql_inflight} gelijktijdige GraphQL-requests; "
            f"REST alleen zonder product_id of bij bulk-fout)…",
            flush=True,
        )

    # Prijzen: GraphQL productVariantsBulkUpdate, parallel per product (eigen sessie per thread).
    by_pid: dict[str, list[tuple[str, str, str]]] = defaultdict(list)
    no_pid_ops: list[tuple[str, str, str]] = []
    for sku, vid, price in price_ops:
        pid = product_id_for_variant(sku_to_vp, sku, vid)
        if pid:
            by_pid[pid].append((sku, vid, price))
        else:
            no_pid_ops.append((sku, vid, price))

    n_done = 0
    max_variants_per_mutation = 100
    n_products = len(by_pid)
    state_lock = threading.Lock()

    if n_products > 0:
        completed = 0
        with ThreadPoolExecutor(max_workers=price_workers) as ex:
            futs = {
                ex.submit(
                    _run_price_bulk_for_product,
                    shop,
                    token,
                    api_ver,
                    pid,
                    items,
                    max_variants_per_mutation,
                ): pid
                for pid, items in by_pid.items()
            }
            for fut in as_completed(futs):
                dn, ne, updates = fut.result()
                with state_lock:
                    for sku, vid, price in updates:
                        _merge_state(
                            state,
                            sku,
                            {
                                "price_incl": price,
                                "price_variants": {str(vid): price},
                            },
                        )
                    errors += ne
                    n_done += dn
                    completed += 1
                    if n_done > 0 and (n_done % progress_every == 0 or n_done == n_price):
                        save_state(state_path, state)
                if completed == 1 or completed == n_products or completed % 50 == 0:
                    print(
                        f"  Prijs bulk: {completed}/{n_products} producten "
                        f"({n_done}/{n_price} varianten)…",
                        flush=True,
                    )

    price_rest_sess = _http_session()
    for sku, vid, price in no_pid_ops:
        print(f"  Prijs REST (geen product_id in cache): SKU {sku} …", flush=True)
        if rest_variant_price(shop, token, api_ver, vid, price, sess=price_rest_sess):
            _merge_state(
                state,
                sku,
                {"price_incl": price, "price_variants": {str(vid): price}},
            )
            n_done += 1
        else:
            errors += 1
        save_state(state_path, state)
        time.sleep(0.1)

    # Product status (dedupe op product_id: laatste wint)
    seen_pid: set[str] = set()
    product_ops_rev = list(reversed(product_ops))
    deduped: list[tuple[str, str, str]] = []
    for sku, pid, ps in product_ops_rev:
        if not pid:
            continue
        if pid in seen_pid:
            continue
        seen_pid.add(pid)
        deduped.append((sku, pid, ps))
    deduped.reverse()

    skus_by_product_id: dict[str, list[str]] = {}
    for s, pairs in sku_to_vp.items():
        for _v, p in pairs:
            if p:
                skus_by_product_id.setdefault(str(p), []).append(s)

    n_prod = len(deduped)
    if n_prod:
        print(
            f"Prijzen afgerond. Start productstatus: {n_prod} unieke producten…",
            flush=True,
        )

    product_sess = _http_session()
    for pidx, (sku, pid, ps) in enumerate(deduped, start=1):
        st_rest = "draft" if ps == "DRAFT" else "active"
        if pidx == 1:
            print(
                f"  Product status {pidx}/{n_prod} (product {pid}) — REST PUT "
                "(kan tot ~60s duren; geen vastloper zolang je geen netwerkfout ziet)…",
                flush=True,
            )
        elif pidx == n_prod or pidx % progress_every == 0:
            print(f"  Product status {pidx}/{n_prod} (product {pid}) — verzoek…", flush=True)
        t0 = time.time()
        ok = rest_product_status(shop, token, api_ver, pid, st_rest, sess=product_sess)
        elapsed = time.time() - t0
        if pidx == 1 or elapsed > 15.0:
            print(
                f"  Product status {pidx}/{n_prod} (product {pid}) — "
                f"{'ok' if ok else 'fout'} in {elapsed:.1f}s",
                flush=True,
            )
        if ok:
            for s in skus_by_product_id.get(str(pid), []):
                if s in desired_by_sku and desired_by_sku[s]["product_status"] == ps:
                    _merge_state(
                        state,
                        s,
                        {
                            "product_status": ps,
                            "product_status_by_product": {str(pid): ps},
                        },
                    )
        else:
            errors += 1
        if pidx % progress_every == 0 or pidx == n_prod:
            save_state(state_path, state)
        time.sleep(0.1)

    save_state(state_path, state)
    print(f"State opgeslagen: {state_path}", flush=True)
    print(f"Klaar. userErrors/sluitfouten: {errors}", flush=True)
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
