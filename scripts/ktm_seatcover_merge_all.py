#!/usr/bin/env python3
"""
Migreer Kutvek Seat Covers naar verzamelproducten (zoals pilot 35167614).

Veiligheid:
  - Snapshot van shop-SKU's is leidend
  - Elke bron-SKU van een groep moet op ≥1 variant van de survivor blijven
  - Survivor-varianten mogen alleen SKU's uit die groep gebruiken
  - Dry-run standaard; --apply schrijft pas na geslaagde verificatie per groep

Stappen per design-groep:
  1. Survivor productSet: Model (YMM-notatie) + Bouwjaar (enkel jaar), CONTINUE
  2. YMM metafields
  3. Collecties powerparts-seats + ktm
  4. Redirects oude handles → survivor
  5. Oude producten → DRAFT

Voorbeelden:
  python3 scripts/ktm_seatcover_merge_all.py
  python3 scripts/ktm_seatcover_merge_all.py --apply --limit 3
  python3 scripts/ktm_seatcover_merge_all.py --apply --group 35167614
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import re
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402
from modules.metafields_manager_export import (  # noqa: E402
    _pipe_join_sorted,
    _ymm_summary,
    _ymm_tuples_to_fits_on_json,
)

_spec = importlib.util.spec_from_file_location(
    "ktm_seatcover_ymm_dry_run",
    PROJECT_ROOT / "scripts" / "ktm_seatcover_ymm_dry_run.py",
)
_dry = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_dry)
normalize_kutvek_model = _dry.normalize_kutvek_model
normalize_make = _dry.normalize_make

SNAP_DIR = PROJECT_ROOT / "output" / "seatcovers" / "migration"
DEFAULT_SNAP = SNAP_DIR / "snapshot_products_latest.jsonl"
COLLECTIONS = {
    "powerparts-seats": "gid://shopify/Collection/282129137845",
    "ktm": "gid://shopify/Collection/626102272330",
}

TITLE_RE = re.compile(
    r"^(?P<head>.+?)\s*-\s*(?P<make>KTM|HUSQVARNA|HUSKY|GASGAS|GAS\s*GAS)\s+"
    r"(?P<model>.+?)\s*\((?P<y1>\d{4})\s*[-–]\s*(?P<y2>\d{4})\)\s*$",
    re.I,
)
FULL = re.compile(r"^(\d{4})\s*[-–]\s*(\d{4})$")
OPEN = re.compile(r"^(\d{4})\s*[-–]\s*$")
ONLY = re.compile(r"^(\d{4})$")


def load_dotenv(path: Path | None = None) -> None:
    path = path or (PROJECT_ROOT / ".env")
    if not path.is_file():
        return
    with path.open(encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, val = line.partition("=")
            key = key.strip()
            if not key or key in os.environ:
                continue
            val = val.strip().strip('"').strip("'")
            os.environ[key] = val


def digits(s: str) -> str:
    return re.sub(r"[^0-9]", "", s or "")


def design_key(sku: str) -> str:
    d = digits(sku)
    return d[:-3] if len(d) >= 11 else ""


def numeric_id(gid: str) -> str:
    m = re.search(r"/(\d+)$", gid or "")
    return m.group(1) if m else ""


def session_url() -> tuple[requests.Session, str, dict]:
    load_dotenv()
    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    ver = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION")
        or config.SHOPIFY_ADMIN_API_VERSION
        or "2024-10"
    ).strip()
    if not shop or not token:
        raise SystemExit("SHOPIFY credentials ontbreken")
    sess = requests.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    url = f"https://{shop}/admin/api/{ver}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    return sess, url, headers


def gql(sess, url, headers, query, variables=None, timeout=300):
    for i in range(12):
        r = sess.post(
            url, headers=headers, json={"query": query, "variables": variables or {}}, timeout=timeout
        )
        if r.status_code == 429:
            time.sleep(2 + i)
            continue
        r.raise_for_status()
        body = r.json()
        errs = body.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs):
            time.sleep(2 + i)
            continue
        if errs:
            raise RuntimeError(json.dumps(errs, indent=2)[:3000])
        return body.get("data") or {}
    raise RuntimeError("throttled")


def load_snapshot(path: Path) -> list[dict]:
    if not path.is_file():
        raise SystemExit(f"Snapshot ontbreekt: {path}")
    return [json.loads(l) for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]


def title_head_and_model(title: str) -> tuple[str, str, str, int, int] | None:
    title = re.sub(r"\s+", " ", (title or "")).strip()
    m = TITLE_RE.match(title)
    if not m:
        return None
    head = re.sub(r"\s+", " ", m.group("head")).strip()
    make = normalize_make(m.group("make"))
    model = m.group("model").strip()
    return head, make, model, int(m.group("y1")), int(m.group("y2"))


def group_products(products: list[dict]) -> tuple[dict[str, list[dict]], list[dict]]:
    groups: dict[str, list[dict]] = defaultdict(list)
    mangled: list[dict] = []
    other: list[dict] = []
    for p in products:
        parsed = title_head_and_model(p.get("title") or "")
        if not parsed:
            other.append(p)
            continue
        skus = [
            (v.get("sku") or "").strip()
            for v in ((p.get("variants") or {}).get("nodes") or [])
            if (v.get("sku") or "").strip()
        ]
        skus11 = [s for s in skus if len(digits(s)) >= 11]
        p = dict(p)
        p["_parse"] = {
            "head": parsed[0],
            "make": parsed[1],
            "model_raw": parsed[2],
            "y1": parsed[3],
            "y2": parsed[4],
            "design_key": "",
        }
        if skus11:
            keys = [design_key(s) for s in skus11 if design_key(s)]
            dkey = max(set(keys), key=keys.count)
            p["_parse"]["design_key"] = dkey
            groups[dkey].append(p)
        elif skus and all(len(digits(s)) == 10 for s in skus):
            mangled.append(p)
        else:
            other.append(p)

    # 10-cijferige EXC125-SKU's: koppel via title-head + SKU-prefix6.
    # Zelfde designnaam kan meerdere design-keys hebben (351769 vs 351763);
    # *13 is altijd alleen SMR 450, *14 is de hoofdgroep → bij twijfel *14.
    head_to_keys: dict[str, list[str]] = defaultdict(list)
    for dk, nodes in groups.items():
        head = (nodes[0].get("_parse") or {}).get("head") or ""
        head_to_keys[head.lower()].append(dk)

    still_other = []
    for p in mangled:
        head = (p.get("_parse") or {}).get("head") or ""
        skus = [
            (v.get("sku") or "").strip()
            for v in ((p.get("variants") or {}).get("nodes") or [])
            if (v.get("sku") or "").strip()
        ]
        prefix6 = digits(skus[0])[:6] if skus else ""
        candidates = [dk for dk in (head_to_keys.get(head.lower()) or []) if dk.startswith(prefix6)]
        chosen = None
        if len(candidates) == 1:
            chosen = candidates[0]
        elif len(candidates) > 1:
            pref14 = [dk for dk in candidates if dk.endswith("14")]
            if len(pref14) == 1:
                chosen = pref14[0]
            else:
                # Meeste bronproducten wint (nooit *13 SMR-only bij EXC125)
                chosen = max(candidates, key=lambda dk: len(groups[dk]))
        if chosen:
            p["_parse"]["design_key"] = chosen
            p["_parse"]["mangled_attach"] = True
            groups[chosen].append(p)
        else:
            still_other.append(p)

    other.extend(still_other)
    return groups, other


def pick_survivor(nodes: list[dict]) -> dict:
    """Kies survivor: al gemigreerd (Model+Bouwjaar jaren) wint, anders meeste vars."""

    def already_merged(n: dict) -> bool:
        opts = n.get("options") or []
        names = {o.get("name") for o in opts}
        if "Model" not in names or "Bouwjaar" not in names:
            return False
        bj = next((o for o in opts if o.get("name") == "Bouwjaar"), None)
        vals = (bj or {}).get("values") or []
        return bool(vals) and all(re.fullmatch(r"\d{4}", str(v)) for v in vals[:5])

    merged = [n for n in nodes if already_merged(n)]
    pool = merged or nodes

    def score(n: dict) -> tuple:
        nvar = len(((n.get("variants") or {}).get("nodes") or []))
        pid = int(numeric_id(n.get("id") or "0") or 0)
        return (nvar, -pid)

    return max(pool, key=score)


def expand_model_rows(model_raw: str, rows: list[dict]) -> list[dict]:
    """Zelfde logica als pilot: closed bands eerst, open bands vullen gaten, orphan SKUs forceren."""
    best_band: dict[str, dict] = {}
    for r in rows:
        key = r["band"]
        prev = best_band.get(key)
        if prev is None or (
            len(digits(r["sku"])) >= 11 and len(digits(prev["sku"])) < 11
        ):
            best_band[key] = r
    rows = list(best_band.values())
    if not rows:
        return []
    title_y2 = max(r["title_y2"] for r in rows)
    model_norm, _note = normalize_kutvek_model(model_raw)
    model_norm = model_norm or model_raw.upper()

    closed, opens = [], []
    for r in rows:
        if FULL.match(r["band"]):
            closed.append(r)
        else:
            opens.append(r)

    year_to: dict[str, dict] = {}
    for r in closed:
        m = FULL.match(r["band"])
        assert m
        a, b = int(m.group(1)), int(m.group(2))
        for y in range(a, b + 1):
            year_to[str(y)] = r

    starts: list[int] = []
    for r in rows:
        m = FULL.match(r["band"]) or OPEN.match(r["band"]) or ONLY.match(r["band"])
        if m:
            starts.append(int(m.group(1)))
    starts = sorted(set(starts))

    def open_start(band: str) -> int:
        m = OPEN.match(band) or ONLY.match(band) or FULL.match(band)
        if not m:
            m = re.match(r"(\d{4})", band)
        return int(m.group(1)) if m else 0

    for r in sorted(opens, key=lambda x: -open_start(x["band"])):
        m = OPEN.match(r["band"]) or ONLY.match(r["band"])
        if not m:
            continue
        y_start = int(m.group(1))
        later = [s for s in starts if s > y_start]
        y_end = (min(later) - 1) if later else title_y2
        if ONLY.match(r["band"]):
            y_end = y_start
        for y in range(y_start, max(y_start, y_end) + 1):
            ys = str(y)
            if ys not in year_to:
                year_to[ys] = r

    # Orphan SKUs: forceer minstens startjaar zodat geen bron-SKU verdwijnt
    used = {year_to[y]["sku"] for y in year_to}
    for r in rows:
        if r["sku"] in used:
            continue
        m = FULL.match(r["band"]) or OPEN.match(r["band"]) or ONLY.match(r["band"])
        if not m:
            continue
        y = str(int(m.group(1)))
        year_to[y] = r
        used.add(r["sku"])

    out = []
    for y, r in sorted(year_to.items(), key=lambda kv: int(kv[0])):
        out.append(
            {
                "model": model_norm,
                "model_raw": model_raw,
                "year": y,
                "sku": r["sku"],
                "price": r["price"],
                "band": r["band"],
            }
        )
    return out


def build_group_plan(group_id: str, nodes: list[dict]) -> dict:
    survivor = pick_survivor(nodes)
    head = next((n["_parse"]["head"] for n in nodes if n.get("_parse")), "Seat Cover")
    # Collect rows per raw model from NON-survivor sources preferentially for SKU truth,
    # but include all nodes including survivor if still in Kutvek title form.
    by_model_raw: dict[str, list[dict]] = defaultdict(list)
    source_skus: set[str] = set()
    source_rows: list[dict] = []

    for n in nodes:
        # If already merged survivor, recover mapping from existing variants
        parsed = n.get("_parse")
        opts = {o.get("name"): o for o in (n.get("options") or [])}
        already = (
            "Model" in opts
            and "Bouwjaar" in opts
            and all(
                re.fullmatch(r"\d{4}", str(v))
                for v in (opts["Bouwjaar"].get("values") or [])[:3]
            )
        )
        for v in ((n.get("variants") or {}).get("nodes") or []):
            sku = (v.get("sku") or "").strip()
            price = (v.get("price") or "").strip() or "0.00"
            if not sku:
                continue
            source_skus.add(sku)
            if already:
                # Model + year already on variant
                so = {o["name"]: o["value"] for o in (v.get("selectedOptions") or [])}
                model = (so.get("Model") or "").strip()
                year = (so.get("Bouwjaar") or "").strip()
                if model and re.fullmatch(r"\d{4}", year):
                    source_rows.append(
                        {
                            "from": "merged",
                            "model_raw": model,  # already YMM
                            "model": model,
                            "year": year,
                            "sku": sku,
                            "price": price,
                            "band": year,
                            "title_y2": int(year),
                        }
                    )
                continue
            if not parsed:
                continue
            band = (v.get("title") or "").strip()
            if not band or band.lower() == "default title":
                continue
            row = {
                "from": "kutvek",
                "model_raw": parsed["model_raw"],
                "band": band,
                "sku": sku,
                "price": price,
                "title_y1": parsed["y1"],
                "title_y2": parsed["y2"],
            }
            by_model_raw[parsed["model_raw"]].append(row)
            source_rows.append(row)

    expanded: list[dict] = []
    if any(r.get("from") == "kutvek" for r in source_rows):
        for model_raw, rows in sorted(by_model_raw.items()):
            expanded.extend(expand_model_rows(model_raw, rows))
        # Also keep already-merged survivor years if siblings still Kutvek-only
        # (expanded from siblings should cover)
    else:
        # Entire group already merged (only survivor left active?) — keep as-is
        for r in source_rows:
            if r.get("from") == "merged":
                expanded.append(
                    {
                        "model": r["model"],
                        "model_raw": r["model_raw"],
                        "year": r["year"],
                        "sku": r["sku"],
                        "price": r["price"],
                        "band": r["band"],
                    }
                )

    # Dedup (model, year)
    best: dict[tuple[str, str], dict] = {}
    for e in expanded:
        key = (e["model"], e["year"])
        prev = best.get(key)
        if prev is None or (len(digits(e["sku"])) >= 11 and len(digits(prev["sku"])) < 11):
            best[key] = e
    expanded = sorted(best.values(), key=lambda x: (x["model"], int(x["year"])))

    out_skus = {e["sku"] for e in expanded}
    missing = sorted(source_skus - out_skus)
    extra = sorted(out_skus - source_skus)

    # Force missing SKUs onto their band-start year
    if missing:
        by_sku = defaultdict(list)
        for r in source_rows:
            if r.get("from") == "kutvek" and r["sku"] in missing:
                by_sku[r["sku"]].append(r)
        for sku, rows in by_sku.items():
            r = rows[0]
            m = FULL.match(r["band"]) or OPEN.match(r["band"]) or ONLY.match(r["band"])
            if not m:
                continue
            y = str(int(m.group(1)))
            model_norm, _ = normalize_kutvek_model(r["model_raw"])
            model_norm = model_norm or r["model_raw"].upper()
            best[(model_norm, y)] = {
                "model": model_norm,
                "model_raw": r["model_raw"],
                "year": y,
                "sku": sku,
                "price": r["price"],
                "band": r["band"],
            }
        expanded = sorted(best.values(), key=lambda x: (x["model"], int(x["year"])))
        out_skus = {e["sku"] for e in expanded}
        missing = sorted(source_skus - out_skus)
        extra = sorted(out_skus - source_skus)

    tuples: set[tuple[str, str, str]] = set()
    for e in expanded:
        # make from first node
        make = next((n["_parse"]["make"] for n in nodes if n.get("_parse")), "KTM")
        tuples.add((make, e["model"], e["year"]))

    models = sorted({e["model"] for e in expanded})
    years = sorted({e["year"] for e in expanded}, key=int)
    archives = [n for n in nodes if n.get("handle") != survivor.get("handle")]

    ok = not missing and not extra and bool(expanded)
    return {
        "group_id": group_id,
        "ok": ok,
        "title": head,
        "survivor_id": survivor.get("id"),
        "survivor_handle": survivor.get("handle"),
        "survivor_status": survivor.get("status"),
        "products_now": len(nodes),
        "archive_handles": [n.get("handle") for n in archives],
        "archive_ids": [n.get("id") for n in archives],
        "source_sku_count": len(source_skus),
        "output_sku_count": len(out_skus),
        "variant_count": len(expanded),
        "models": models,
        "years": years,
        "missing_skus": missing,
        "extra_skus": extra,
        "ymm_summary": (_ymm_summary(tuples) or "")[:255],
        "fits_on": json.loads(_ymm_tuples_to_fits_on_json(tuples)) if tuples else {},
        "variants": expanded,
        "source_skus": sorted(source_skus),
    }


def model_sort_key(name: str) -> tuple:
    m = re.match(r"^(\d{2,4})\b", name)
    cc = int(m.group(1)) if m else 9999
    return (cc, name)


def apply_group(sess, url, headers, plan: dict, dry_run: bool) -> dict:
    result = {"group_id": plan["group_id"], "ok": False, "steps": []}
    if not plan["ok"]:
        result["error"] = "plan failed verification"
        return result

    survivor_id = plan["survivor_id"]
    variants = plan["variants"]
    models = sorted(plan["models"], key=model_sort_key)
    years = sorted(plan["years"], key=int)

    if dry_run:
        result["ok"] = True
        result["steps"].append("dry-run skip writes")
        return result

    # 1) productSet
    variant_inputs = [
        {
            "optionValues": [
                {"optionName": "Model", "name": e["model"]},
                {"optionName": "Bouwjaar", "name": e["year"]},
            ],
            "sku": e["sku"],
            "price": e["price"],
            "inventoryPolicy": "CONTINUE",
        }
        for e in sorted(variants, key=lambda x: (int(x["year"]), x["model"]))
    ]
    mutation = """
    mutation mergeSet($input: ProductSetInput!, $synchronous: Boolean!) {
      productSet(synchronous: $synchronous, input: $input) {
        productSetOperation { id status }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "synchronous": False,
        "input": {
            "id": survivor_id,
            "title": plan["title"],
            "status": "ACTIVE",
            "productOptions": [
                {"name": "Model", "position": 1, "values": [{"name": m} for m in models]},
                {"name": "Bouwjaar", "position": 2, "values": [{"name": y} for y in years]},
            ],
            "variants": variant_inputs,
        },
    }
    data = gql(sess, url, headers, mutation, variables)
    payload = data.get("productSet") or {}
    if payload.get("userErrors"):
        result["error"] = payload["userErrors"]
        return result
    op_id = (payload.get("productSetOperation") or {}).get("id")
    if not op_id:
        result["error"] = "no productSetOperation id"
        return result
    status_q = """
    query($id: ID!) {
      productOperation(id: $id) {
        ... on ProductSetOperation {
          status
          userErrors { message code }
          product {
            id handle
            variantsCount { count }
            options { name optionValues { name } }
          }
        }
      }
    }
    """
    for _ in range(90):
        time.sleep(2)
        st = (gql(sess, url, headers, status_q, {"id": op_id}).get("productOperation")) or {}
        if st.get("status") in {"COMPLETE", "COMPLETE_WITH_ERRORS", "FAILED", "CANCELED"}:
            if st.get("status") != "COMPLETE" or st.get("userErrors"):
                result["error"] = {"status": st.get("status"), "userErrors": st.get("userErrors")}
                return result
            count = ((st.get("product") or {}).get("variantsCount") or {}).get("count")
            if count != plan["variant_count"]:
                result["error"] = f"variant count {count} != expected {plan['variant_count']}"
                return result
            # reorder model options
            break
    else:
        result["error"] = "productSet timeout"
        return result
    result["steps"].append("productSet")

    # Reorder models by cc
    reorder = """
    mutation reorder($productId: ID!, $options: [OptionReorderInput!]!) {
      productOptionsReorder(productId: $productId, options: $options) {
        userErrors { message }
      }
    }
    """
    gql(
        sess,
        url,
        headers,
        reorder,
        {
            "productId": survivor_id,
            "options": [
                {"name": "Model", "values": [{"name": m} for m in models]},
                {"name": "Bouwjaar", "values": [{"name": y} for y in years]},
            ],
        },
    )
    result["steps"].append("reorder")

    # 2) metafields
    fits_on = json.dumps(plan["fits_on"], ensure_ascii=False, separators=(",", ":"))
    tuples = {
        (make, model, year)
        for make, models_map in plan["fits_on"].items()
        for model, years_list in models_map.items()
        for year in years_list
    }
    meta = """
    mutation setYmm($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        userErrors { message }
      }
    }
    """
    metafields = [
        {
            "ownerId": survivor_id,
            "namespace": "global",
            "key": "fits_on",
            "type": "json",
            "value": fits_on,
        },
        {
            "ownerId": survivor_id,
            "namespace": "global",
            "key": "ymm_summary",
            "type": "single_line_text_field",
            "value": plan["ymm_summary"],
        },
        {
            "ownerId": survivor_id,
            "namespace": "global",
            "key": "fits_on_make",
            "type": "single_line_text_field",
            "value": _pipe_join_sorted({t[0] for t in tuples}),
        },
        {
            "ownerId": survivor_id,
            "namespace": "global",
            "key": "fits_on_model",
            "type": "single_line_text_field",
            "value": _pipe_join_sorted({t[1] for t in tuples}),
        },
        {
            "ownerId": survivor_id,
            "namespace": "global",
            "key": "fits_on_year",
            "type": "single_line_text_field",
            "value": _pipe_join_sorted({t[2] for t in tuples}),
        },
    ]
    mpay = gql(sess, url, headers, meta, {"metafields": metafields}).get("metafieldsSet") or {}
    if mpay.get("userErrors"):
        result["error"] = mpay["userErrors"]
        return result
    result["steps"].append("ymm")

    # 3) collections
    add_c = """
    mutation add($id: ID!, $productIds: [ID!]!) {
      collectionAddProducts(id: $id, productIds: $productIds) {
        userErrors { message }
      }
    }
    """
    for _handle, cid in COLLECTIONS.items():
        cp = gql(
            sess, url, headers, add_c, {"id": cid, "productIds": [survivor_id]}
        ).get("collectionAddProducts") or {}
        # ignore "already in collection" style errors
        result["steps"].append(f"collection:{_handle}")

    # 4) redirects (soft — token mist mogelijk write_online_store_navigation)
    # 5) draft archives
    redir = """
    mutation redir($urlRedirect: UrlRedirectInput!) {
      urlRedirectCreate(urlRedirect: $urlRedirect) {
        urlRedirect { id path target }
        userErrors { message code }
      }
    }
    """
    draft = """
    mutation draft($input: ProductInput!) {
      productUpdate(input: $input) {
        product { id status }
        userErrors { message }
      }
    }
    """
    target = f"/products/{plan['survivor_handle']}"
    redirect_ok = 0
    redirect_fail = 0
    redirect_denied = False
    for handle, pid in zip(plan["archive_handles"], plan["archive_ids"]):
        if not handle or not pid:
            continue
        path = f"/products/{handle}"
        if not redirect_denied:
            try:
                rp = gql(
                    sess,
                    url,
                    headers,
                    redir,
                    {"urlRedirect": {"path": path, "target": target}},
                ).get("urlRedirectCreate") or {}
                errs = rp.get("userErrors") or []
                if errs and any("taken" in (e.get("message") or "").lower() or e.get("code") == "TAKEN" for e in errs):
                    redirect_ok += 1  # bestaat al
                elif errs:
                    redirect_fail += 1
                else:
                    redirect_ok += 1
            except RuntimeError as e:
                msg = str(e)
                if "write_online_store_navigation" in msg or "ACCESS_DENIED" in msg:
                    redirect_denied = True
                    redirect_fail += 1
                else:
                    redirect_fail += 1
        else:
            redirect_fail += 1

        dp = gql(
            sess,
            url,
            headers,
            draft,
            {"input": {"id": pid, "status": "DRAFT"}},
        ).get("productUpdate") or {}
        if dp.get("userErrors"):
            result["error"] = {"draft": handle, "errors": dp["userErrors"]}
            return result
        time.sleep(0.12)
    result["steps"].append(
        f"redirects:ok={redirect_ok}/fail={redirect_fail}"
        + (";csv_import_needed" if redirect_denied or redirect_fail else "")
    )
    result["steps"].append(f"drafted:{len(plan['archive_handles'])}")
    result["redirect_denied"] = redirect_denied
    result["redirect_ok"] = redirect_ok
    result["redirect_fail"] = redirect_fail

    # Post-verify SKUs on survivor
    vq = """
    query($id: ID!, $cursor: String) {
      product(id: $id) {
        status
        variants(first: 100, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes { sku }
        }
      }
    }
    """
    got: set[str] = set()
    cursor = None
    while True:
        pdata = gql(sess, url, headers, vq, {"id": survivor_id, "cursor": cursor})["product"]
        for v in pdata["variants"]["nodes"]:
            if v.get("sku"):
                got.add(v["sku"])
        if not pdata["variants"]["pageInfo"]["hasNextPage"]:
            break
        cursor = pdata["variants"]["pageInfo"]["endCursor"]
    expect = set(plan["source_skus"])
    if got != expect:
        result["error"] = {
            "postcheck": "sku mismatch after write",
            "missing": sorted(expect - got),
            "extra": sorted(got - expect),
        }
        return result
    result["ok"] = True
    result["steps"].append("postcheck_skus_ok")
    return result


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, default=DEFAULT_SNAP)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Max groepen om te schrijven")
    ap.add_argument("--group", action="append", default=[], help="Alleen deze design-group id(s)")
    ap.add_argument("--skip-ok-pilot", action="store_true", help="Skip groepen die al alleen survivor ACTIVE hebben")
    args = ap.parse_args()

    products = load_snapshot(args.snapshot)
    groups, other = group_products(products)
    print(f"Snapshot producten: {len(products)}")
    print(f"Kutvek design-groepen: {len(groups)}")
    print(f"Overig/OEM ongemoeid: {len(other)}")

    plans = []
    for gid, nodes in sorted(groups.items(), key=lambda x: -len(x[1])):
        if args.group and gid not in args.group:
            continue
        plans.append(build_group_plan(gid, nodes))

    ok_plans = [p for p in plans if p["ok"]]
    bad_plans = [p for p in plans if not p["ok"]]
    print(f"Plannen OK: {len(ok_plans)}  FAIL: {len(bad_plans)}")
    if bad_plans:
        print("Eerste failures:")
        for p in bad_plans[:10]:
            print(
                f"  {p['group_id']}: missing={p['missing_skus'][:5]} extra={p['extra_skus'][:5]} "
                f"vars={p['variant_count']} src={p['source_sku_count']}"
            )

    SNAP_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    plan_path = SNAP_DIR / f"merge_all_plan_{stamp}.json"
    # strip bulky identical source lists in summary file? keep full for apply
    plan_path.write_text(json.dumps(plans, indent=2, ensure_ascii=False), encoding="utf-8")
    (SNAP_DIR / "merge_all_plan_latest.json").write_bytes(plan_path.read_bytes())

    # CSV overview
    overview = SNAP_DIR / f"merge_all_overview_{stamp}.csv"
    with overview.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "ok",
            "group_id",
            "title",
            "products_now",
            "survivor_handle",
            "archive_count",
            "variant_count",
            "source_sku_count",
            "output_sku_count",
            "missing_skus",
            "extra_skus",
            "ymm_summary",
        ]
        w = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        w.writeheader()
        for p in plans:
            w.writerow(
                {
                    "ok": "1" if p["ok"] else "0",
                    "group_id": p["group_id"],
                    "title": p["title"],
                    "products_now": p["products_now"],
                    "survivor_handle": p["survivor_handle"],
                    "archive_count": len(p["archive_handles"]),
                    "variant_count": p["variant_count"],
                    "source_sku_count": p["source_sku_count"],
                    "output_sku_count": p["output_sku_count"],
                    "missing_skus": " | ".join(p["missing_skus"]),
                    "extra_skus": " | ".join(p["extra_skus"]),
                    "ymm_summary": p["ymm_summary"],
                }
            )
    print(f"Plan: {plan_path}")
    print(f"Overview: {overview}")

    # Shopify Admin → Online Store → URL redirects → Import
    redir_csv = SNAP_DIR / f"merge_all_redirects_{stamp}.csv"
    with redir_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Redirect from", "Redirect to"])
        for p in ok_plans:
            target = f"/products/{p['survivor_handle']}"
            for handle in p["archive_handles"]:
                if handle:
                    w.writerow([f"/products/{handle}", target])
    (SNAP_DIR / "merge_all_redirects_latest.csv").write_bytes(redir_csv.read_bytes())
    n_redir = sum(len(p["archive_handles"]) for p in ok_plans)
    print(f"Redirects CSV ({n_redir}): {redir_csv}")

    if bad_plans:
        print("ABORT: niet alle groepen verifiëren schoon. Geen apply.")
        return 2

    if not args.apply:
        total_archive = sum(len(p["archive_handles"]) for p in ok_plans)
        print(
            f"Dry-run OK. Zou {len(ok_plans)} survivors schrijven en "
            f"{total_archive} producten draften + redirecten."
        )
        return 0

    sess, url, headers = session_url()
    results = []
    todo = ok_plans
    if args.limit:
        todo = todo[: args.limit]
    for i, plan in enumerate(todo, 1):
        print(
            f"[{i}/{len(todo)}] {plan['group_id']} {plan['title']!r} "
            f"{plan['products_now']}→1 vars={plan['variant_count']}…",
            flush=True,
        )
        res = apply_group(sess, url, headers, plan, dry_run=False)
        results.append(res)
        print("  ", "OK" if res.get("ok") else "FAIL", res.get("steps"), res.get("error"), flush=True)
        if not res.get("ok"):
            print("STOP bij failure — rest niet aangepast.")
            break
        time.sleep(0.5)

    res_path = SNAP_DIR / f"merge_all_results_{stamp}.json"
    res_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Results: {res_path}")
    failed = [r for r in results if not r.get("ok")]
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
