#!/usr/bin/env python3
"""
Tag-clone: KTM-producten (ktm-shop.nl) die in de winkel-telling staan maar nog
niet in Motox POS (ktm-shop-nederland) → tag synkro-clone-ktm-shop-nederland.

Synkro kloont daarna het product; voorraad landt op Winkelvoorraad POS.
Niet zelf productCreate. Cap default 40 producten per run (Basic ~1000
nieuwe varianten/dag op Motox).

  python3 scripts/motox_synkro_clone_missing.py
  python3 scripts/motox_synkro_clone_missing.py --yes --max 40 --scope telling

Standaard dry-run tegen lokale caches. --yes haalt Motox-SKU's live op,
ververst status/tags op ktm-shop.nl, slaat verdwenen cache-IDs over, en schrijft de tag.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

from modules.env_loader import load_project_env  # noqa: E402

load_project_env()

import config  # noqa: E402

TELLING_CSV = ROOT / "motox" / "Voorraad winkel - WERKDOC EJIS.csv"
TELLING_SKUS_TXT = ROOT / "motox" / "telling_skus.txt"
KTM_SKU_MAP = ROOT / "cache" / "shopify_sku_to_product_id.json"
KTM_INDEX = ROOT / "cache" / "shopify_products_index.json"
MOTOX_SKUS_CACHE = ROOT / "cache" / "motox" / "shopify_skus.json"
OUT_DIR = ROOT / "output"

CLONE_TAG_DEFAULT = "synkro-clone-ktm-shop-nederland"
_REQUEST_TIMEOUT = (15, 120)
_SKU_KEY_RE = re.compile(r"[\s\-\.]+")

_GQL_NODES = """
query SynkroCloneNodes($ids: [ID!]!) {
  nodes(ids: $ids) {
    ... on Product {
      id
      handle
      title
      status
      tags
    }
  }
}
"""

_GQL_VARIANTS_PAGE = """
query SynkroCloneSkus($c: String) {
  productVariants(first: 250, after: $c) {
    pageInfo { hasNextPage endCursor }
    nodes { sku }
  }
}
"""

_GQL_VARIANTS_QUERY = """
query SynkroCloneSkuSearch($q: String!) {
  productVariants(first: 50, query: $q) {
    nodes {
      sku
      product {
        id
        handle
        title
        status
        tags
      }
    }
  }
}
"""

_GQL_TAGS_ADD = """
mutation SynkroCloneTagsAdd($id: ID!, $tags: [String!]!) {
  tagsAdd(id: $id, tags: $tags) {
    node {
      ... on Product { id tags }
    }
    userErrors { field message }
  }
}
"""


@dataclass
class ProductRow:
    product_id: str
    handle: str = ""
    title: str = ""
    status: str = ""
    tags: list[str] = field(default_factory=list)
    skus: list[str] = field(default_factory=list)
    action: str = ""
    error: str = ""
    in_run: bool = False
    live_found: bool | None = None


def sku_key(raw: str | None) -> str:
    return _SKU_KEY_RE.sub("", str(raw or "")).upper()


def _numeric_id(gid: str) -> str:
    return (gid or "").rsplit("/", 1)[-1]


def product_gid(pid: str) -> str:
    p = (pid or "").strip()
    if p.startswith("gid://"):
        return p
    return f"gid://shopify/Product/{p}"


def tags_list(raw: Any) -> list[str]:
    if isinstance(raw, list):
        return [str(t).strip() for t in raw if str(t).strip()]
    return [t.strip() for t in str(raw or "").split(",") if t.strip()]


def has_tag(tags: list[str], tag: str) -> bool:
    want = tag.strip().lower()
    return any(t.strip().lower() == want for t in tags)


def handle_rank(handle: str) -> int:
    h = (handle or "").strip().lower()
    if not h:
        return 2
    if h.startswith("hsq-") or h.startswith("wp-"):
        return 1
    return 0


def strip_brand_prefix(handle: str) -> str:
    h = (handle or "").strip().lower()
    if h.startswith("hsq-"):
        return h[4:]
    if h.startswith("wp-"):
        return h[3:]
    return h


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def gql(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    query: str,
    variables: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    last: dict[str, Any] = {}
    for attempt in range(12):
        r = sess.post(
            url,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            json={"query": query, "variables": variables or {}},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
            time.sleep(min(2.0 * (attempt + 1), 20.0))
            continue
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED"
            or "throttl" in str(e).lower()
            for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 20.0))
            continue
        if errs:
            raise RuntimeError(errs[:3])
        return last
    raise RuntimeError(last.get("errors") or "GraphQL mislukt")


def load_telling_skus(path: Path | None = None) -> dict[str, str]:
    """sku_key → originele SKU (eerste voorkomen)."""
    csv_path = path or TELLING_CSV
    out: dict[str, str] = {}
    if csv_path.is_file():
        with csv_path.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames or "Variant SKU" not in reader.fieldnames:
                raise SystemExit(f"Kolom 'Variant SKU' ontbreekt in {csv_path}")
            for row in reader:
                raw = (row.get("Variant SKU") or "").strip()
                key = sku_key(raw)
                if not key or key == "0":
                    continue
                out.setdefault(key, raw)
        return out
    if TELLING_SKUS_TXT.is_file():
        for line in TELLING_SKUS_TXT.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            key = sku_key(raw)
            if not key or key == "0":
                continue
            out.setdefault(key, raw)
        return out
    raise SystemExit(
        f"Telling-bestand ontbreekt ({TELLING_CSV.name} of {TELLING_SKUS_TXT.name})."
    )


def load_sku_set_from_json_list(path: Path) -> set[str]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise SystemExit(f"Verwacht een JSON-lijst in {path}")
    return {sku_key(s) for s in raw if sku_key(s)}


def load_ktm_index() -> tuple[dict[str, dict[str, Any]], dict[str, str], dict[str, dict[str, Any]]]:
    """handle → meta, pid → handle, pid → meta."""
    handle_to_meta: dict[str, dict[str, Any]] = {}
    pid_to_handle: dict[str, str] = {}
    pid_to_meta: dict[str, dict[str, Any]] = {}
    if not KTM_INDEX.is_file():
        return handle_to_meta, pid_to_handle, pid_to_meta
    raw = json.loads(KTM_INDEX.read_text(encoding="utf-8"))
    for handle, meta in (raw or {}).items():
        h = str(handle or "").strip().lower()
        if not h or not isinstance(meta, dict):
            continue
        pid = str(meta.get("id") or "").strip()
        rec = {
            "id": pid,
            "handle": h,
            "title": str(meta.get("title") or "").strip(),
            "tags": tags_list(meta.get("tags")),
            "status": str(meta.get("status") or "").strip().upper(),
        }
        handle_to_meta[h] = rec
        if pid:
            pid_to_handle[pid] = h
            pid_to_meta[pid] = rec
    return handle_to_meta, pid_to_handle, pid_to_meta


def load_ktm_sku_map(
    pid_to_handle: dict[str, str],
    handle_to_meta: dict[str, dict[str, Any]],
) -> dict[str, str]:
    """sku_key → product_id, met voorkeur voor KTM-handle zonder hsq-/wp-."""
    out: dict[str, str] = {}
    if not KTM_SKU_MAP.is_file():
        return out
    raw = json.loads(KTM_SKU_MAP.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise SystemExit(f"Verwacht een JSON-object in {KTM_SKU_MAP}")
    for sku, pid in raw.items():
        key = sku_key(sku)
        pid_s = str(pid or "").strip()
        if not key or not pid_s:
            continue
        prev = out.get(key)
        if prev is None:
            out[key] = pid_s
            continue
        if handle_rank(pid_to_handle.get(pid_s, "")) < handle_rank(
            pid_to_handle.get(prev, "")
        ):
            out[key] = pid_s
    for key, pid in list(out.items()):
        out[key] = prefer_plain_pid(pid, handle_to_meta, pid_to_handle)
    return out


def prefer_plain_pid(
    pid: str,
    handle_to_meta: dict[str, dict[str, Any]],
    pid_to_handle: dict[str, str],
) -> str:
    handle = pid_to_handle.get(pid, "")
    bare = strip_brand_prefix(handle)
    if bare and bare != handle and bare in handle_to_meta:
        alt = str(handle_to_meta[bare].get("id") or "").strip()
        if alt:
            return alt
    return pid


def pick_preferred_product(products: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not products:
        return None
    ranked = sorted(
        products,
        key=lambda p: (
            handle_rank(str(p.get("handle") or "")),
            0 if str(p.get("status") or "").upper() == "ACTIVE" else 1,
            str(p.get("handle") or ""),
            _numeric_id(str(p.get("id") or "")),
        ),
    )
    return ranked[0]


def ktm_creds() -> tuple[str, str, str]:
    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    api = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION")
        or config.SHOPIFY_ADMIN_API_VERSION
        or "2024-10"
    ).strip()
    return shop, token, api


def fetch_motox_skus_live() -> set[str]:
    from modules.motox_shopify_cache import load_motox_env

    domain, token, api = load_motox_env()
    print(f"Motox-SKU's live ophalen ({domain})…", flush=True)
    sess = _session()
    skus: set[str] = set()
    cursor = None
    pages = 0
    while True:
        body = gql(sess, domain, token, api, _GQL_VARIANTS_PAGE, {"c": cursor})
        conn = ((body.get("data") or {}).get("productVariants")) or {}
        for node in conn.get("nodes") or []:
            key = sku_key(node.get("sku"))
            if key:
                skus.add(key)
        pages += 1
        if pages % 20 == 0:
            print(f"  Motox varianten-pagina {pages}, {len(skus)} SKU's…", flush=True)
        info = conn.get("pageInfo") or {}
        if not info.get("hasNextPage"):
            break
        cursor = info.get("endCursor")
        if not cursor:
            break
    print(f"Motox live: {len(skus)} SKU's ({pages} pagina's).", flush=True)
    return skus


def lookup_skus_on_ktm(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    missing: dict[str, str],
    known: dict[str, str],
    handle_to_meta: dict[str, dict[str, Any]],
    pid_to_handle: dict[str, str],
    pid_to_meta: dict[str, dict[str, Any]],
    sleep_s: float,
) -> None:
    """Vul known (sku_key → pid) voor SKU's die niet in de KTM-cache zitten."""
    todo = [k for k in missing if k not in known]
    if not todo:
        return
    print(f"KTM-API: {len(todo)} SKU's opzoeken die niet in cache zitten…", flush=True)
    batch_size = 15
    found = 0
    for i in range(0, len(todo), batch_size):
        chunk_keys = todo[i : i + batch_size]
        parts = []
        for key in chunk_keys:
            raw = missing[key]
            escaped = raw.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'sku:"{escaped}"')
        q = " OR ".join(parts)
        try:
            body = gql(sess, shop, token, api, _GQL_VARIANTS_QUERY, {"q": q})
        except Exception as e:
            print(f"  SKU-zoekfout batch {i}: {e}", file=sys.stderr, flush=True)
            time.sleep(max(0.0, sleep_s))
            continue
        nodes = (((body.get("data") or {}).get("productVariants")) or {}).get("nodes") or []
        by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for node in nodes:
            key = sku_key(node.get("sku"))
            prod = node.get("product") or {}
            if not key or not prod.get("id"):
                continue
            by_key[key].append(prod)
        for key, prods in by_key.items():
            if key not in missing or key in known:
                continue
            picked = pick_preferred_product(prods)
            if not picked:
                continue
            pid = _numeric_id(str(picked.get("id") or ""))
            handle = str(picked.get("handle") or "").strip().lower()
            rec = {
                "id": pid,
                "handle": handle,
                "title": str(picked.get("title") or "").strip(),
                "tags": tags_list(picked.get("tags")),
                "status": str(picked.get("status") or "").strip().upper(),
            }
            if handle:
                handle_to_meta.setdefault(handle, rec)
                pid_to_handle[pid] = handle
            pid_to_meta[pid] = rec
            known[key] = prefer_plain_pid(pid, handle_to_meta, pid_to_handle)
            found += 1
        if (i // batch_size) % 20 == 0 and i:
            print(f"  opgezocht {min(i + batch_size, len(todo))}/{len(todo)}  hits={found}", flush=True)
        time.sleep(max(0.0, sleep_s))
    print(f"KTM-API lookup klaar: {found} extra SKU's gekoppeld.", flush=True)


def hydrate_products(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    rows: list[ProductRow],
    sleep_s: float,
) -> None:
    if not rows:
        return
    print(f"Live status/tags ophalen voor {len(rows)} KTM-producten…", flush=True)
    by_id = {r.product_id: r for r in rows}
    for r in rows:
        r.live_found = False
    ids = [product_gid(r.product_id) for r in rows]
    for i in range(0, len(ids), 100):
        chunk = ids[i : i + 100]
        body = gql(sess, shop, token, api, _GQL_NODES, {"ids": chunk})
        for node in (body.get("data") or {}).get("nodes") or []:
            if not node:
                continue
            pid = _numeric_id(str(node.get("id") or ""))
            row = by_id.get(pid)
            if not row:
                continue
            row.live_found = True
            row.handle = str(node.get("handle") or row.handle).strip().lower()
            row.title = str(node.get("title") or row.title).strip()
            row.status = str(node.get("status") or row.status).strip().upper()
            row.tags = tags_list(node.get("tags"))
        time.sleep(max(0.0, sleep_s))
    gone = sum(1 for r in rows if r.live_found is False)
    if gone:
        print(f"  {gone} product-IDs bestaan niet meer in Shopify (stale cache).", flush=True)


def is_missing_product_error(msg: str) -> bool:
    return "product does not exist" in (msg or "").lower()


def mark_gone(row: ProductRow, msg: str = "Product does not exist") -> None:
    row.action = "skip_gone"
    row.error = msg
    row.live_found = False


def tags_add(
    sess: requests.Session,
    shop: str,
    token: str,
    api: str,
    product_id: str,
    tag: str,
) -> tuple[bool, str]:
    body = gql(
        sess,
        shop,
        token,
        api,
        _GQL_TAGS_ADD,
        {"id": product_gid(product_id), "tags": [tag]},
    )
    payload = ((body.get("data") or {}).get("tagsAdd")) or {}
    errs = payload.get("userErrors") or []
    if errs:
        msg = "; ".join(
            f"{e.get('field')}: {e.get('message')}" if e.get("field") else str(e.get("message") or e)
            for e in errs
        )
        return False, msg
    return True, "ok"


def write_csv(path: Path, rows: list[ProductRow], clone_tag: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "product_id",
        "handle",
        "title",
        "status",
        "tags",
        "missing_skus",
        "sku_count",
        "has_clone_tag",
        "action",
        "in_run",
        "error",
    ]
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";")
        w.writeheader()
        for r in rows:
            w.writerow(
                {
                    "product_id": r.product_id,
                    "handle": r.handle,
                    "title": r.title,
                    "status": r.status,
                    "tags": ", ".join(r.tags),
                    "missing_skus": ",".join(r.skus),
                    "sku_count": str(len(r.skus)),
                    "has_clone_tag": "1" if has_tag(r.tags, clone_tag) else "0",
                    "action": r.action,
                    "in_run": "1" if r.in_run else "0",
                    "error": r.error,
                }
            )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--yes", action="store_true", help="Schrijf tag op ktm-shop.nl")
    ap.add_argument("--max", type=int, default=40, help="Max producten om te taggen (default 40)")
    ap.add_argument(
        "--scope",
        choices=["telling"],
        default="telling",
        help="telling = SKU's uit winkel-telling (tab 1)",
    )
    ap.add_argument("--telling-csv", type=Path, default=None, help="Pad naar telling-CSV")
    ap.add_argument(
        "--live-motox",
        action="store_true",
        help="Motox-SKU's live ophalen i.p.v. cache",
    )
    ap.add_argument(
        "--live-ktm",
        action="store_true",
        help="Productstatus/tags live bij ktm-shop.nl ophalen (stale IDs overslaan)",
    )
    ap.add_argument(
        "--lookup-missing",
        action="store_true",
        help="SKU's die niet in de KTM-cache zitten één voor één live opzoeken",
    )
    ap.add_argument("--clone-tag", default=CLONE_TAG_DEFAULT, help="Synkro clone-tag")
    ap.add_argument("--sleep", type=float, default=0.2, help="Pauze tussen Shopify-calls")
    ap.add_argument("--output", type=Path, default=None, help="CSV-rapport")
    args = ap.parse_args()
    if args.max < 0:
        print("--max moet >= 0", file=sys.stderr)
        return 1

    live_motox = bool(args.yes or args.live_motox)
    live_ktm = bool(args.yes or args.live_ktm)
    clone_tag = (args.clone_tag or CLONE_TAG_DEFAULT).strip()

    ktm_shop, ktm_token, ktm_api = ktm_creds()
    if args.yes and (not ktm_shop or not ktm_token):
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt (ktm-shop.nl).", file=sys.stderr)
        return 1

    print(f"Telling-SKU's laden ({args.scope})…", flush=True)
    telling = load_telling_skus(args.telling_csv)
    print(f"Telling unieke SKU's: {len(telling)}", flush=True)

    if live_motox:
        motox = fetch_motox_skus_live()
    elif MOTOX_SKUS_CACHE.is_file():
        motox = load_sku_set_from_json_list(MOTOX_SKUS_CACHE)
        print(f"Motox-cache: {len(motox)} SKU's ({MOTOX_SKUS_CACHE})", flush=True)
    else:
        print(
            "Motox-cache ontbreekt (cache/motox/shopify_skus.json). Gebruik --live-motox.",
            file=sys.stderr,
        )
        return 1

    missing = {k: v for k, v in telling.items() if k not in motox}
    print(f"Ontbreekt in Motox: {len(missing)}", flush=True)

    handle_to_meta, pid_to_handle, pid_to_meta = load_ktm_index()
    sku_to_pid = load_ktm_sku_map(pid_to_handle, handle_to_meta)
    print(
        f"KTM-cache: {len(sku_to_pid)} SKU→product, {len(handle_to_meta)} handles",
        flush=True,
    )

    sess = _session() if (live_ktm and ktm_shop and ktm_token) else None
    if live_ktm:
        if not ktm_shop or not ktm_token:
            print("live-ktm gevraagd maar KTM-credentials ontbreken.", file=sys.stderr)
            return 1
        assert sess is not None
        if args.lookup_missing or not sku_to_pid:
            lookup_skus_on_ktm(
                sess,
                ktm_shop,
                ktm_token,
                ktm_api,
                missing,
                sku_to_pid,
                handle_to_meta,
                pid_to_handle,
                pid_to_meta,
                args.sleep,
            )
        elif missing:
            cached_hits = sum(1 for k in missing if k in sku_to_pid)
            print(
                f"KTM-cache dekt {cached_hits}/{len(missing)} ontbrekende SKU's "
                f"(geen volledige live-SKU-zoek; gebruik --lookup-missing om de rest te scannen).",
                flush=True,
            )

    if not sku_to_pid and not live_ktm:
        print(
            "KTM SKU→product-cache ontbreekt. Zet cache/shopify_sku_to_product_id.json "
            "of gebruik --live-ktm.",
            file=sys.stderr,
        )
        return 1

    pid_skus: dict[str, list[str]] = defaultdict(list)
    unresolved = 0
    for key, raw in missing.items():
        pid = sku_to_pid.get(key)
        if not pid:
            unresolved += 1
            continue
        pid = prefer_plain_pid(pid, handle_to_meta, pid_to_handle)
        if raw not in pid_skus[pid]:
            pid_skus[pid].append(raw)

    rows: list[ProductRow] = []
    for pid, skus in pid_skus.items():
        meta = pid_to_meta.get(pid) or {}
        rows.append(
            ProductRow(
                product_id=pid,
                handle=str(meta.get("handle") or pid_to_handle.get(pid) or ""),
                title=str(meta.get("title") or ""),
                status=str(meta.get("status") or "").upper(),
                tags=list(meta.get("tags") or []),
                skus=skus,
            )
        )
    rows.sort(key=lambda r: (handle_rank(r.handle), r.handle, r.product_id))

    if live_ktm and sess is not None:
        hydrate_products(sess, ktm_shop, ktm_token, ktm_api, rows, args.sleep)
        gone_rows = [r for r in rows if r.live_found is False]
        retry_skus: dict[str, str] = {}
        dead_pids = {r.product_id for r in gone_rows}
        for r in gone_rows:
            mark_gone(r)
            for sku in r.skus:
                retry_skus[sku_key(sku)] = sku
        if retry_skus:
            print(
                f"{len(gone_rows)} verdwenen producten; {len(retry_skus)} SKU's opnieuw opzoeken…",
                flush=True,
            )
            remapped: dict[str, str] = {}
            lookup_skus_on_ktm(
                sess,
                ktm_shop,
                ktm_token,
                ktm_api,
                retry_skus,
                remapped,
                handle_to_meta,
                pid_to_handle,
                pid_to_meta,
                args.sleep,
            )
            by_pid = {r.product_id: r for r in rows if r.action != "skip_gone"}
            new_rows: list[ProductRow] = []
            for key, pid in remapped.items():
                if pid in dead_pids:
                    continue
                raw = retry_skus.get(key)
                if not raw:
                    continue
                existing = by_pid.get(pid)
                if existing is not None:
                    if raw not in existing.skus:
                        existing.skus.append(raw)
                    continue
                meta = pid_to_meta.get(pid) or {}
                nr = ProductRow(
                    product_id=pid,
                    handle=str(meta.get("handle") or pid_to_handle.get(pid) or ""),
                    title=str(meta.get("title") or ""),
                    status=str(meta.get("status") or "").upper(),
                    tags=list(meta.get("tags") or []),
                    skus=[raw],
                )
                by_pid[pid] = nr
                new_rows.append(nr)
            if new_rows:
                hydrate_products(sess, ktm_shop, ktm_token, ktm_api, new_rows, args.sleep)
                for nr in new_rows:
                    if nr.live_found is False:
                        mark_gone(nr)
                    rows.append(nr)
                rows.sort(key=lambda r: (handle_rank(r.handle), r.handle, r.product_id))

    skip_archived = 0
    skip_tagged = 0
    skip_gone = 0
    candidates: list[ProductRow] = []
    for r in rows:
        if r.action == "skip_gone" or r.live_found is False:
            if r.action != "skip_gone":
                mark_gone(r)
            skip_gone += 1
            continue
        if r.status == "ARCHIVED":
            r.action = "skip_archived"
            skip_archived += 1
            continue
        if has_tag(r.tags, clone_tag):
            r.action = "skip_tagged"
            skip_tagged += 1
            continue
        r.action = "would_tag"
        candidates.append(r)

    cap = args.max
    selected: list[ProductRow] = []
    apply_ok = 0
    apply_fail = 0
    if args.yes:
        assert sess is not None
        print(f"Tag '{clone_tag}' zetten (max {cap})…", flush=True)
        for r in candidates:
            if apply_ok >= cap:
                break
            r.in_run = True
            selected.append(r)
            try:
                ok, msg = tags_add(sess, ktm_shop, ktm_token, ktm_api, r.product_id, clone_tag)
            except Exception as e:
                ok, msg = False, str(e)[:500]
            if ok:
                r.action = "tagged"
                if clone_tag not in r.tags:
                    r.tags.append(clone_tag)
                apply_ok += 1
            elif is_missing_product_error(msg):
                mark_gone(r, msg)
                skip_gone += 1
                print(
                    f"  Overgeslagen (bestaat niet) {r.product_id} {r.handle}",
                    flush=True,
                )
            else:
                r.action = "error"
                r.error = msg
                apply_fail += 1
                print(f"  Fout {r.product_id} {r.handle}: {msg}", file=sys.stderr, flush=True)
            time.sleep(max(0.0, args.sleep))
        if not selected:
            print("Niets te taggen deze run.", flush=True)
    else:
        selected = candidates[:cap]
        for r in selected:
            r.in_run = True

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = args.output or (OUT_DIR / f"motox_synkro_clone_missing_{stamp}.csv")
    write_csv(out, rows, clone_tag)

    mode = "APPLY" if args.yes else "DRY-RUN"
    print(
        f"{mode}  telling={len(telling)}  motox={len(motox)}  missing={len(missing)}  "
        f"niet_op_ktm={unresolved}  ktm_producten={len(rows)}",
        flush=True,
    )
    print(
        f"skip_archived={skip_archived}  skip_tagged={skip_tagged}  skip_gone={skip_gone}  "
        f"kandidaten={len(candidates)}  deze_run={len(selected)}  max={cap}",
        flush=True,
    )
    if args.yes:
        print(f"getagd={apply_ok}  fout={apply_fail}", flush=True)
    print(f"CSV: {out}", flush=True)
    if selected:
        print("Eerste handles deze run:", flush=True)
        for r in selected[:10]:
            print(f"  {r.handle or r.product_id}  skus={len(r.skus)}  {r.action}", flush=True)
    return 1 if apply_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
