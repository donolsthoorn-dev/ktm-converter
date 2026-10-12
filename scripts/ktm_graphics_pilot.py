#!/usr/bin/env python3
"""
Pilot: één Kutvek Graphics & Stickers product normaliseren (geen merge).

- Titel = merk + model + segment + design (zoals Kutvek-import), bv.
  "Husqvarna 300 TE Dirt Bike Zombies Dark Graphic Kit Black"
- Opties: Bouwjaar (enkel jaar) + Uitvoering (geen Model — één fiets per product)
- Year-banden uitklappen naar losse jaren (zelfde SKU hergebruikt)
- YMM metafields (fits_on / ymm_summary / fits_on_*)
- Garagecatalogus: ontbrekende jaren voor dit model toevoegen

Voorbeeld:
  python3 scripts/ktm_graphics_pilot.py --handle 45-167-005-kutvek
  python3 scripts/ktm_graphics_pilot.py --handle 45-167-005-kutvek --apply
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
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

TITLE_RE = re.compile(
    r"^(?P<make>KTM|Husqvarna|HUSQVARNA|GASGAS|GasGas|Gas\s*Gas)\s+"
    r"(?P<bike>.+?)\s+"
    r"(?P<seg>Dirt\s*Bike|Street\s*Bike|Motocross|Dirtbike)\s+"
    r"(?P<design>.+?)\s+(?:(?P<graphic>Graphic)\s+)?Kit(?:\s+(?P<color>.+))?$",
    re.I,
)
DESIGN_ONLY_TITLE_RE = re.compile(
    r"^(?P<design>.+?)\s+(?:(?P<graphic>Graphic)\s+)?Kit(?:\s+(?P<color>.+))?$",
    re.I,
)
YEAR_BAND = re.compile(r"^(\d{4})\s*[-–]\s*(\d{4})$")
YEAR_ONLY = re.compile(r"^(\d{4})$")

CATALOG_PATHS = [
    PROJECT_ROOT / "ktm" / "garage" / "catalog" / "vehicles.json",
    PROJECT_ROOT
    / "ktm"
    / "garage"
    / "extensions"
    / "motor-garage"
    / "assets"
    / "vehicles.json",
]
OUT_DIR = PROJECT_ROOT / "output" / "graphics"


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
            os.environ[key] = val.strip().strip('"').strip("'")


def normalize_make(raw: str) -> str:
    u = re.sub(r"\s+", " ", (raw or "").strip()).upper()
    if u in {"HUSKY", "HUSQVARNA"}:
        return "HUSQVARNA"
    if u in {"GAS GAS", "GASGAS"}:
        return "GASGAS"
    return u or "KTM"


def normalize_graphics_model(make: str, bike_raw: str) -> tuple[str, str]:
    """
    Titel-bike → catalogusmodel.
    HUSQVARNA/GASGAS: '300 TE' → 'TE 300', '450 MCF' → 'MC 450F'
    KTM: '450 SXF' → '450 SX-F', '125 SX' → '125 SX'
    """
    s = re.sub(r"\s+", " ", (bike_raw or "").strip()).upper()
    s = s.replace("SX F", "SX-F").replace("EXC F", "EXC-F").replace("XC F", "XC-F")
    s = s.replace("SXF", "SX-F").replace("EXCF", "EXC-F").replace("XCF", "XC-F")
    note = ""

    if make in {"HUSQVARNA", "GASGAS"}:
        # 300 TE / 125 TC / 450 FC → TE 300
        m = re.match(r"^(\d{2,4})\s+(TE|TC|FC|FE|FS|FX|TX|EE)(?:\s+(.*))?$", s)
        if m:
            cc, line, rest = m.group(1), m.group(2), (m.group(3) or "").strip()
            model = f"{line} {cc}" + (f" {rest}" if rest else "")
            return model, note
        # 450 MCF / 250 ECF → MC 450F / EC 250F
        m = re.match(r"^(\d{2,4})\s+(MC|EC)F(?:\s+(.*))?$", s)
        if m:
            cc, line, rest = m.group(1), m.group(2), (m.group(3) or "").strip()
            model = f"{line} {cc}F" + (f" {rest}" if rest else "")
            return model, "mcf_ecf"
        # 701 ENDURO LR → 701 ENDURO LR (al catalogus-achtig)
        if s:
            return s, note
        return "", "empty_bike"

    # KTM: al cc-first in titels meestal
    if s:
        return s, note
    return "", "empty_bike"


def years_from_band(band: str) -> list[str]:
    band = re.sub(r"\s+", " ", (band or "").strip())
    m = YEAR_BAND.match(band)
    if m:
        y1, y2 = int(m.group(1)), int(m.group(2))
        if y2 < y1:
            y1, y2 = y2, y1
        return [str(y) for y in range(y1, y2 + 1)]
    m = YEAR_ONLY.match(band)
    if m:
        return [m.group(1)]
    return []


def display_make(make: str) -> str:
    u = normalize_make(make)
    if u == "HUSQVARNA":
        return "Husqvarna"
    if u == "GASGAS":
        return "GASGAS"
    return "KTM"


def normalize_segment(raw: str) -> str:
    s = re.sub(r"\s+", " ", (raw or "").strip())
    if re.fullmatch(r"Dirtbike", s, re.I):
        return "Dirt Bike"
    return s or "Dirt Bike"


def catalog_to_title_bike(make: str, catalog_model: str) -> str:
    """Catalogusmodel → bike-deel in producttitel (Kutvek-vorm)."""
    s = re.sub(r"\s+", " ", (catalog_model or "").strip()).upper()
    mk = normalize_make(make)
    if mk in {"HUSQVARNA", "GASGAS"}:
        m = re.match(r"^(TE|TC|FC|FE|FS|FX|TX|EE|MC|EC)\s+(\d{2,4})(F?)(?:\s+(.*))?$", s)
        if m:
            line, cc, f, rest = m.group(1), m.group(2), m.group(3), (m.group(4) or "").strip()
            if line in {"MC", "EC"} and f:
                return f"{cc} {line}F" + (f" {rest}" if rest else "")
            return f"{cc} {line}" + (f" {rest}" if rest else "")
    return catalog_model.strip()


def product_display_title(
    make: str,
    bike_raw: str,
    segment: str,
    design: str,
    color: str | None,
    kit_word: str = "Graphic Kit",
) -> str:
    design = re.sub(r"\s+", " ", (design or "").strip())
    color = re.sub(r"\s+", " ", (color or "").strip())
    bike = re.sub(r"\s+", " ", (bike_raw or "").strip())
    seg = normalize_segment(segment)
    head = f"{display_make(make)} {bike} {seg}".strip()
    kit = kit_word or "Graphic Kit"
    if color:
        return f"{head} {design} {kit} {color}"
    return f"{head} {design} {kit}"


def load_plan_bike_context(handle: str) -> dict | None:
    plan_path = OUT_DIR / f"pilot_{handle}_plan.json"
    if not plan_path.is_file():
        return None
    try:
        plan = json.loads(plan_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    old = (plan.get("old_title") or "").strip()
    m = TITLE_RE.match(old)
    if m:
        return {
            "make": normalize_make(m.group("make")),
            "bike_raw": m.group("bike").strip(),
            "segment": normalize_segment(m.group("seg")),
            "design": m.group("design").strip(),
            "color": (m.group("color") or "").strip() or None,
        }
    if plan.get("bike_raw") and plan.get("segment"):
        return {
            "make": normalize_make(plan.get("make") or ""),
            "bike_raw": plan["bike_raw"],
            "segment": normalize_segment(plan.get("segment") or "Dirt Bike"),
            "design": plan.get("design") or "",
            "color": plan.get("color"),
        }
    return None


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
        raise SystemExit("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.")
    sess = requests.Session()
    sess.trust_env = False
    sess.proxies = {"http": None, "https": None}
    url = f"https://{shop}/admin/api/{ver}/graphql.json"
    headers = {"X-Shopify-Access-Token": token, "Content-Type": "application/json"}
    return sess, url, headers


def gql(sess: requests.Session, url: str, headers: dict, query: str, variables: dict) -> dict:
    for i in range(8):
        r = sess.post(
            url, headers=headers, json={"query": query, "variables": variables}, timeout=120
        )
        if r.status_code == 429:
            time.sleep(2 + i)
            continue
        r.raise_for_status()
        body = r.json()
        errors = body.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errors):
            time.sleep(2 + i)
            continue
        if errors:
            raise RuntimeError(json.dumps(errors, indent=2)[:2000])
        return body.get("data") or {}
    raise RuntimeError("te vaak throttled")


def fetch_product(sess, url, headers, handle: str) -> dict:
    query = """
    query($q: String!) {
      products(first: 1, query: $q) {
        nodes {
          id
          handle
          title
          status
          vendor
          productType
          options { name values }
          variants(first: 100) {
            nodes {
              id
              sku
              title
              price
              selectedOptions { name value }
            }
          }
          ymm: metafield(namespace: "global", key: "ymm_summary") { value }
          fits: metafield(namespace: "global", key: "fits_on") { value }
        }
      }
    }
    """
    data = gql(sess, url, headers, query, {"q": f"handle:{handle}"})
    nodes = ((data.get("products") or {}).get("nodes")) or []
    if not nodes:
        raise SystemExit(f"Product niet gevonden: handle={handle}")
    return nodes[0]


def build_plan(node: dict) -> dict:
    title = re.sub(r"\s+", " ", (node.get("title") or "")).strip()
    handle = node.get("handle") or ""
    m = TITLE_RE.match(title)
    design = color = segment = bike_raw = ""
    make = normalize_make(node.get("vendor") or "")

    kit_word = "Graphic Kit"
    if m:
        make = normalize_make(m.group("make"))
        bike_raw = m.group("bike").strip()
        segment = normalize_segment(m.group("seg"))
        design = m.group("design").strip()
        color = (m.group("color") or "").strip() or None
        kit_word = "Graphic Kit" if m.group("graphic") else "Kit"
    else:
        m2 = DESIGN_ONLY_TITLE_RE.match(title)
        if not m2:
            raise SystemExit(f"Titel niet parseerbaar: {title!r}")
        design = m2.group("design").strip()
        color = (m2.group("color") or "").strip() or None
        kit_word = "Graphic Kit" if m2.group("graphic") else "Kit"
        ctx = load_plan_bike_context(handle)
        if ctx:
            make = ctx["make"]
            bike_raw = ctx["bike_raw"]
            segment = ctx["segment"]
            if not design:
                design = ctx.get("design") or design
        else:
            opts = {o.get("name"): o for o in (node.get("options") or [])}
            model_vals = (opts.get("Model") or {}).get("values") or []
            catalog_model = model_vals[0] if model_vals else ""
            if not catalog_model:
                fits_raw = ((node.get("fits") or {}) or {}).get("value")
                if fits_raw:
                    fo = json.loads(fits_raw)
                    for mk, models in fo.items():
                        make = normalize_make(mk)
                        if models:
                            catalog_model = next(iter(models.keys()))
                            break
            if not catalog_model:
                raise SystemExit(
                    f"Titel is design-only ({title!r}) en geen plan/Model/fits_on — "
                    "kan merk/model niet afleiden."
                )
            bike_raw = catalog_to_title_bike(make, catalog_model)
            segment = "Dirt Bike"

    model, model_note = normalize_graphics_model(make, bike_raw)
    new_title = product_display_title(
        make, bike_raw, segment, design, color, kit_word=kit_word
    )

    uitvoeringen = sorted(
        {
            o["value"]
            for v in ((node.get("variants") or {}).get("nodes") or [])
            for o in (v.get("selectedOptions") or [])
            if o.get("name") == "Uitvoering" and o.get("value")
        }
    )
    if not uitvoeringen:
        uitvoeringen = ["Complete Set"]

    expanded: list[dict] = []
    source_skus: set[str] = set()
    for v in ((node.get("variants") or {}).get("nodes") or []):
        sku = (v.get("sku") or "").strip()
        price = (v.get("price") or "").strip() or "0.00"
        so = {o["name"]: o["value"] for o in (v.get("selectedOptions") or [])}
        band = (so.get("Year") or so.get("Bouwjaar") or so.get("Model jaar") or "").strip()
        uitvoering = (so.get("Uitvoering") or "Complete Set").strip()
        if not sku or not band:
            continue
        source_skus.add(sku)
        year_list = years_from_band(band)
        if not year_list:
            raise SystemExit(f"Kan year-band niet parsen: {band!r} (sku={sku})")
        for y in year_list:
            expanded.append(
                {
                    "model": model,
                    "year": y,
                    "uitvoering": uitvoering,
                    "sku": sku,
                    "price": price,
                    "band": band,
                }
            )

    # Dedup (year, uitvoering) — één model per product
    best: dict[tuple[str, str], dict] = {}
    for e in expanded:
        key = (e["year"], e["uitvoering"])
        if key not in best:
            best[key] = e
    variants = sorted(
        best.values(),
        key=lambda e: (int(e["year"]), e["uitvoering"]),
    )
    out_skus = {e["sku"] for e in variants}
    missing = sorted(source_skus - out_skus)
    extra = sorted(out_skus - source_skus)

    tuples = {(make, model, e["year"]) for e in variants}
    years = sorted({e["year"] for e in variants}, key=int)

    return {
        "ok": not missing and not extra and bool(variants) and bool(model),
        "product_id": node["id"],
        "handle": node.get("handle"),
        "old_title": title,
        "new_title": new_title,
        "make": make,
        "bike_raw": bike_raw,
        "segment": segment,
        "design": design,
        "color": color,
        "model": model,
        "model_note": model_note,
        "years": years,
        "uitvoeringen": uitvoeringen,
        "variant_count": len(variants),
        "source_sku_count": len(source_skus),
        "output_sku_count": len(out_skus),
        "missing_skus": missing,
        "extra_skus": extra,
        "ymm_summary": (_ymm_summary(tuples) or "")[:255],
        "fits_on": json.loads(_ymm_tuples_to_fits_on_json(tuples)) if tuples else {},
        "fits_on_json": _ymm_tuples_to_fits_on_json(tuples) if tuples else "{}",
        "tuples": sorted(tuples),
        "variants": variants,
        "old_ymm": ((node.get("ymm") or {}) or {}).get("value"),
        "old_fits": ((node.get("fits") or {}) or {}).get("value"),
    }


def ensure_catalog_years(make: str, model: str, years: list[str]) -> dict:
    """Voeg ontbrekende jaren (en eventueel model) toe aan beide vehicles.json-kopieën."""
    report = {"make": make, "model": model, "added": [], "model_added": False, "paths": []}
    needed = set(years)
    for path in CATALOG_PATHS:
        data = json.loads(path.read_text(encoding="utf-8"))
        found_make = False
        found_model = False
        for mk in data.get("makes") or []:
            if (mk.get("make") or "").upper() != make.upper():
                continue
            found_make = True
            for m in mk.get("models") or []:
                if (m.get("model") or "").upper() != model.upper():
                    continue
                found_model = True
                existing = {str(y) for y in (m.get("years") or [])}
                missing = sorted(needed - existing, key=int)
                if missing:
                    m["years"] = sorted(existing | needed, key=lambda y: -int(y))
                    report["added"] = missing
                    path.write_text(
                        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                        encoding="utf-8",
                    )
                    report["paths"].append(str(path.relative_to(PROJECT_ROOT)))
                else:
                    report["paths"].append(f"{path.relative_to(PROJECT_ROOT)} (already ok)")
                break
            if not found_model:
                mk.setdefault("models", []).append(
                    {
                        "model": model,
                        "years": sorted(needed, key=lambda y: -int(y)),
                    }
                )
                report["model_added"] = True
                report["added"] = sorted(needed, key=int)
                path.write_text(
                    json.dumps(data, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8",
                )
                report["paths"].append(str(path.relative_to(PROJECT_ROOT)))
            break
        if not found_make:
            raise SystemExit(f"Make {make} ontbreekt in {path}")
    return report


def _media_filename(url: str) -> str:
    return (url or "").split("?", 1)[0].rsplit("/", 1)[-1].lower()


def _is_light_media(url: str, alt: str | None) -> bool:
    name = _media_filename(url)
    a = (alt or "").lower()
    return "-light" in name or "_light" in name or "light versie" in a


def _classify_graphics_media(nodes: list[dict]) -> dict[str, str | None]:
    """Bike vóór maquette; Complete vóór Light in galerij."""
    complete_bike = light_bike = complete_maq = light_maq = None
    for m in nodes:
        img_url = ((m.get("image") or {}) or {}).get("url") or ""
        name = _media_filename(img_url)
        mid = m.get("id")
        if "maquette" in name:
            if _is_light_media(img_url, m.get("alt")):
                light_maq = mid
            else:
                complete_maq = mid
        elif _is_light_media(img_url, m.get("alt")):
            light_bike = mid
        else:
            complete_bike = mid
    return {
        "complete_bike": complete_bike,
        "light_bike": light_bike,
        "complete_maq": complete_maq,
        "light_maq": light_maq,
    }


def reorder_graphics_media(sess, url, headers, product_id: str, nodes: list[dict]) -> list[str]:
    """Galerij: Complete fiets → Light fiets → Complete maquette → Light maquette."""
    slots = _classify_graphics_media(nodes)
    order = [
        slots["complete_bike"],
        slots["light_bike"],
        slots["complete_maq"],
        slots["light_maq"],
    ]
    order = [mid for mid in order if mid]
    if len(order) < 2:
        return order
    moves = [{"id": mid, "newPosition": str(i)} for i, mid in enumerate(order)]
    mut = """
    mutation($id: ID!, $moves: [MoveInput!]!) {
      productReorderMedia(id: $id, moves: $moves) {
        mediaUserErrors { field message code }
      }
    }
    """
    data = gql(sess, url, headers, mut, {"id": product_id, "moves": moves})
    errs = ((data.get("productReorderMedia") or {}).get("mediaUserErrors")) or []
    if errs:
        print("productReorderMedia warnings:", json.dumps(errs, indent=2))
    return order


def link_uitvoering_media(sess, url, headers, product_id: str) -> dict:
    """
    Koppel hoofdfoto Complete Set / Light versie aan de juiste varianten.
    Herkenning via bestandsnaam (-light) of alt. Maquettes blijven productgalerij.
    """
    query = """
    query($id: ID!) {
      product(id: $id) {
        media(first: 30) {
          nodes {
            ... on MediaImage {
              id
              alt
              image { url }
            }
          }
        }
        variants(first: 100) {
          nodes {
            id
            selectedOptions { name value }
            media(first: 5) { nodes { id } }
          }
        }
      }
    }
    """
    product = (gql(sess, url, headers, query, {"id": product_id}).get("product")) or {}
    media_nodes = ((product.get("media") or {}).get("nodes") or [])
    reorder_graphics_media(sess, url, headers, product_id, media_nodes)

    complete_id = None
    light_id = None
    alt_updates: list[dict] = []
    for m in media_nodes:
        img_url = ((m.get("image") or {}) or {}).get("url") or ""
        name = _media_filename(img_url)
        if "maquette" in name:
            want_alt = (
                "Light versie – maquette"
                if _is_light_media(img_url, m.get("alt"))
                else "Complete Set – maquette"
            )
            if (m.get("alt") or "") != want_alt:
                alt_updates.append({"id": m["id"], "alt": want_alt})
            continue
        if _is_light_media(img_url, m.get("alt")):
            light_id = m["id"]
            if (m.get("alt") or "") != "Light versie":
                alt_updates.append({"id": m["id"], "alt": "Light versie"})
        else:
            complete_id = m["id"]
            if (m.get("alt") or "") != "Complete Set":
                alt_updates.append({"id": m["id"], "alt": "Complete Set"})

    report = {
        "complete_media": complete_id,
        "light_media": light_id,
        "linked": 0,
        "skipped": "ok",
    }
    if not complete_id and not light_id:
        report["skipped"] = "geen uitvoeringsfoto's herkend"
        return report
    if "Light versie" in {
        o.get("value")
        for v in ((product.get("variants") or {}).get("nodes") or [])
        for o in (v.get("selectedOptions") or [])
        if o.get("name") == "Uitvoering"
    } and not light_id:
        report["skipped"] = "Light versie aanwezig maar geen light-foto"
        return report

    if alt_updates:
        alt_mut = """
        mutation($media: [UpdateMediaInput!]!, $productId: ID!) {
          productUpdateMedia(media: $media, productId: $productId) {
            mediaUserErrors { field message code }
          }
        }
        """
        adata = gql(
            sess,
            url,
            headers,
            alt_mut,
            {"productId": product_id, "media": alt_updates},
        )
        aerrs = ((adata.get("productUpdateMedia") or {}).get("mediaUserErrors")) or []
        if aerrs:
            print("productUpdateMedia warnings:", json.dumps(aerrs, indent=2))

    detach_media: list[dict] = []
    variant_media: list[dict] = []
    for v in ((product.get("variants") or {}).get("nodes") or []):
        so = {o["name"]: o["value"] for o in (v.get("selectedOptions") or [])}
        uit = so.get("Uitvoering") or ""
        current = [n["id"] for n in ((v.get("media") or {}).get("nodes") or [])]
        mid = None
        if uit == "Light versie":
            mid = light_id
        elif uit == "Complete Set":
            mid = complete_id
        if not mid:
            continue
        if current == [mid]:
            continue
        if current:
            detach_media.append({"variantId": v["id"], "mediaIds": current})
        variant_media.append({"variantId": v["id"], "mediaIds": [mid]})

    if detach_media:
        detach = """
        mutation($productId: ID!, $variantMedia: [ProductVariantDetachMediaInput!]!) {
          productVariantDetachMedia(productId: $productId, variantMedia: $variantMedia) {
            userErrors { field message code }
          }
        }
        """
        for i in range(0, len(detach_media), 25):
            chunk = detach_media[i : i + 25]
            data = gql(
                sess,
                url,
                headers,
                detach,
                {"productId": product_id, "variantMedia": chunk},
            )
            errs = ((data.get("productVariantDetachMedia") or {}).get("userErrors")) or []
            if errs:
                raise RuntimeError("productVariantDetachMedia: " + json.dumps(errs, indent=2))

    if not variant_media:
        report["skipped"] = "al gekoppeld of geen match"
        return report

    append = """
    mutation($productId: ID!, $variantMedia: [ProductVariantAppendMediaInput!]!) {
      productVariantAppendMedia(productId: $productId, variantMedia: $variantMedia) {
        userErrors { field message code }
      }
    }
    """
    for i in range(0, len(variant_media), 25):
        chunk = variant_media[i : i + 25]
        data = gql(
            sess,
            url,
            headers,
            append,
            {"productId": product_id, "variantMedia": chunk},
        )
        errs = ((data.get("productVariantAppendMedia") or {}).get("userErrors")) or []
        if errs:
            raise RuntimeError("productVariantAppendMedia: " + json.dumps(errs, indent=2))
    report["linked"] = len(variant_media)
    report["detached"] = len(detach_media)
    report["skipped"] = ""
    return report


def apply_plan(sess, url, headers, plan: dict, quiet: bool = False) -> None:
    def log(msg: str) -> None:
        if not quiet:
            print(msg, flush=True)

    product_id = plan["product_id"]
    years = plan["years"]
    uitvoeringen = plan["uitvoeringen"]

    variant_inputs = [
        {
            "optionValues": [
                {"optionName": "Bouwjaar", "name": e["year"]},
                {"optionName": "Uitvoering", "name": e["uitvoering"]},
            ],
            "sku": e["sku"],
            "price": e["price"],
            "inventoryPolicy": "CONTINUE",
        }
        for e in plan["variants"]
    ]

    mutation = """
    mutation graphicsPilot($input: ProductSetInput!, $synchronous: Boolean!) {
      productSet(synchronous: $synchronous, input: $input) {
        product {
          id
          title
          handle
          options { name values }
          variants(first: 100) {
            nodes { id sku title selectedOptions { name value } }
          }
        }
        userErrors { field message code }
      }
    }
    """
    variables = {
        "synchronous": True,
        "input": {
            "id": product_id,
            "title": plan["new_title"],
            "productOptions": [
                {"name": "Bouwjaar", "position": 1, "values": [{"name": y} for y in years]},
                {
                    "name": "Uitvoering",
                    "position": 2,
                    "values": [{"name": u} for u in uitvoeringen],
                },
            ],
            "variants": variant_inputs,
        },
    }
    log(f"Shopify productSet ({len(variant_inputs)} variants)…")
    data = gql(sess, url, headers, mutation, variables)
    payload = data.get("productSet") or {}
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError("productSet userErrors: " + json.dumps(errs, indent=2))
    product = payload.get("product") or {}
    log(
        f"OK title={product.get('title')!r} "
        f"variants={len((product.get('variants') or {}).get('nodes') or [])}"
    )

    # Option order (sommige shops herschikken)
    reorder = """
    mutation reorder($productId: ID!, $options: [OptionReorderInput!]!) {
      productOptionsReorder(productId: $productId, options: $options) {
        userErrors { field message code }
      }
    }
    """
    rdata = gql(
        sess,
        url,
        headers,
        reorder,
        {
            "productId": product_id,
            "options": [
                {"name": "Bouwjaar", "values": [{"name": y} for y in years]},
                {"name": "Uitvoering", "values": [{"name": u} for u in uitvoeringen]},
            ],
        },
    )
    rerrs = ((rdata.get("productOptionsReorder") or {}).get("userErrors")) or []
    if rerrs and not quiet:
        print("productOptionsReorder warnings:", json.dumps(rerrs, indent=2))

    meta_mutation = """
    mutation setYmm($metafields: [MetafieldsSetInput!]!) {
      metafieldsSet(metafields: $metafields) {
        metafields { namespace key value }
        userErrors { field message code }
      }
    }
    """
    tuples = {(t[0], t[1], t[2]) for t in plan["tuples"]}
    meta_vars = {
        "metafields": [
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on",
                "type": "json",
                "value": plan["fits_on_json"],
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "ymm_summary",
                "type": "single_line_text_field",
                "value": plan["ymm_summary"],
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_make",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({m for m, _, _ in tuples}),
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_model",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({md for _, md, _ in tuples}),
            },
            {
                "ownerId": product_id,
                "namespace": "global",
                "key": "fits_on_year",
                "type": "single_line_text_field",
                "value": _pipe_join_sorted({y for _, _, y in tuples}),
            },
        ]
    }
    log("Shopify metafieldsSet…")
    mdata = gql(sess, url, headers, meta_mutation, meta_vars)
    merrs = ((mdata.get("metafieldsSet") or {}).get("userErrors")) or []
    if merrs:
        raise RuntimeError("metafieldsSet userErrors: " + json.dumps(merrs, indent=2))
    log("YMM metafields gezet.")

    log("Shopify variant-media (Complete/Light)…")
    media_report = link_uitvoering_media(sess, url, headers, product_id)
    if media_report.get("linked"):
        log(f"Media gekoppeld: {media_report['linked']} varianten")
    elif media_report.get("skipped") and not quiet:
        log(f"Media skip: {media_report['skipped']}")


def main() -> int:
    load_dotenv()
    ap = argparse.ArgumentParser(description="Pilot: één graphics product normaliseren")
    ap.add_argument("--handle", default="45-167-005-kutvek")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--skip-catalog",
        action="store_true",
        help="Sla vehicles.json-update over",
    )
    args = ap.parse_args()

    sess, url, headers = session_url()
    node = fetch_product(sess, url, headers, args.handle)
    plan = build_plan(node)

    print(f"Handle:       {plan['handle']}")
    print(f"Title:        {plan['old_title']!r}")
    print(f"           →  {plan['new_title']!r}")
    print(f"Model:        {plan['bike_raw']!r} → {plan['model']!r} ({plan['model_note'] or 'ok'})")
    print(f"Years:        {plan['years']}")
    print(f"Uitvoering:   {plan['uitvoeringen']}")
    print(f"Variants:     {len((node.get('variants') or {}).get('nodes') or [])} → {plan['variant_count']}")
    print(f"SKUs:         {plan['source_sku_count']} source → {plan['output_sku_count']} unique on output")
    print(f"ymm_summary:  {plan['old_ymm']!r}")
    print(f"           →  {plan['ymm_summary']!r}")
    print(f"fits_on:      {json.dumps(plan['fits_on'], ensure_ascii=False)}")
    if plan["missing_skus"] or plan["extra_skus"]:
        print(f"MISSING skus: {plan['missing_skus']}")
        print(f"EXTRA skus:   {plan['extra_skus']}")
    if not plan["ok"]:
        print("Plan FAIL — geen apply.", file=sys.stderr)
        return 1

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    plan_path = OUT_DIR / f"pilot_{plan['handle']}_plan.json"
    plan_path.write_text(json.dumps(plan, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Plan:         {plan_path}")

    if not args.skip_catalog:
        cat = ensure_catalog_years(plan["make"], plan["model"], plan["years"])
        print(
            f"Catalog:      {plan['make']} {plan['model']} "
            f"added years={cat['added'] or 'none'} paths={cat['paths']}"
        )

    if not args.apply:
        print("Dry-run — geen Shopify-writes. Gebruik --apply om door te zetten.")
        return 0

    apply_plan(sess, url, headers, plan)
    print(f"Controleer: https://www.ktm-shop.nl/products/{plan['handle']}")
    print("Let op: garage vehicles.json moet gedeployed worden voor live dropdown-jaren.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
