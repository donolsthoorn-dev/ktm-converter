#!/usr/bin/env python3
"""Bulk-verrijk ktm-bikes collectie met data van ktm-bikes.at.

Schrijft product-metafields:
  custom.battery_wh, motor, weight_kg, max_system_weight_kg,
  geometry, marketing_text, bike_subtitle

Ruimt legacy Geometrie/Frame-size klik-rijen uit descriptionHtml.

Gebruik:
  python3 scripts/enrich_bikes_from_ktm_at.py --dry-run --limit 10
  python3 scripts/enrich_bikes_from_ktm_at.py --dry-run
  python3 scripts/enrich_bikes_from_ktm_at.py --only-missing
  python3 scripts/enrich_bikes_from_ktm_at.py --rebuild-index
"""

from __future__ import annotations

import argparse
import html as html_lib
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

NS = "custom"
COLLECTION = "ktm-bikes"
BASE = "https://www.ktm-bikes.at"
CACHE = PROJECT_ROOT / "cache" / "ktm_bikes_at"
INDEX_PATH = CACHE / "sku_index.json"
REPORT_PATH = CACHE / "enrich_report.json"
DETAIL_CACHE = CACHE / "details"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
SSL_CTX = ssl._create_unverified_context()

LIST_SEEDS = [
    "/e-bikes/list/e-bikes-mountainbike",
    "/e-bikes/list/e-bikes-city-urban",
    "/e-bikes/list/e-bikes-trekking",
    "/e-bikes/list/e-bikes-utility",
    "/light-ebikes/list/light-e-bikes-mountainbike",
    "/light-ebikes/list/light-e-bikes-gravel",
    "/light-ebikes/list/city-urban",
    "/bikes/list/bikes-mountainbike",
    "/bikes/list/bikes-gravel",
    "/bikes/list/bikes-roadbike",
    "/bikes/list/bikes-trekking",
    "/bikes/list/bikes-kids",
]

ARCHIVE_YEARS = list(range(2020, 2027))

ENRICHMENT_KEYS = (
    "battery_wh",
    "motorvermogen",
    "motor",
    "weight_kg",
    "max_system_weight_kg",
    "geometry",
    "marketing_text",
    "bike_subtitle",
)

DEFINITIONS = [
    {
        "name": "Batterij (Wh)",
        "key": "battery_wh",
        "type": "number_integer",
        "description": "Accucapaciteit in Wh voor e-bike badge.",
    },
    {
        "name": "Motorvermogen",
        "key": "motorvermogen",
        "type": "single_line_text_field",
        "description": "Accucapaciteit als filterlabel (bijv. 800Wh).",
    },
    {
        "name": "Motor",
        "key": "motor",
        "type": "single_line_text_field",
        "description": "E-bike motoraanduiding.",
    },
    {
        "name": "Gewicht (kg)",
        "key": "weight_kg",
        "type": "number_decimal",
        "description": "Fietsgewicht in kg volgens KTM-specificatie.",
    },
    {
        "name": "Max. systeemgewicht (kg)",
        "key": "max_system_weight_kg",
        "type": "number_decimal",
        "description": "Toelaatbaar totaalgewicht in kg.",
    },
    {
        "name": "Geometrie",
        "key": "geometry",
        "type": "json",
        "description": "Geometrietabel per framemaat.",
    },
    {
        "name": "Marketingtekst",
        "key": "marketing_text",
        "type": "multi_line_text_field",
        "description": "Marketingtekst voor productpagina (bron: ktm-bikes.at).",
    },
    {
        "name": "Fiets ondertitel",
        "key": "bike_subtitle",
        "type": "single_line_text_field",
        "description": "Drivetrain/ondertitel onder de titel.",
    },
]


# ---------------------------------------------------------------------------
# HTTP / Shopify helpers
# ---------------------------------------------------------------------------


def http_get(url: str, *, retries: int = 5) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": UA})
    last_err: Exception | None = None
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=90, context=SSL_CTX) as resp:
                return resp.read().decode("utf-8", errors="replace")
        except urllib.error.HTTPError as exc:
            last_err = exc
            if exc.code in (429, 500, 502, 503) and attempt < retries - 1:
                time.sleep(2 + attempt * 2)
                continue
            raise
        except urllib.error.URLError as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep(2 + attempt * 2)
                continue
            raise
    raise RuntimeError(f"HTTP failed for {url}: {last_err}")


def gql(query: str, variables: dict | None = None) -> dict:
    if not config.SHOPIFY_ACCESS_TOKEN:
        raise RuntimeError("SHOPIFY_ACCESS_TOKEN ontbreekt in .env")
    body = {"query": query, "variables": variables or {}}
    req = urllib.request.Request(
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json",
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
        },
        method="POST",
    )
    payload: dict = {}
    for attempt in range(8):
        try:
            with urllib.request.urlopen(req, timeout=120, context=SSL_CTX) as resp:
                payload = json.load(resp)
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503) and attempt < 7:
                time.sleep(2 + attempt)
                continue
            raise
        errs = payload.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs):
            time.sleep(2 + attempt)
            continue
        return payload
    return payload


def strip_tags(text: str) -> str:
    text = re.sub(r"<br\s*/?>", "\n", text, flags=re.I)
    text = re.sub(r"<[^>]+>", "", text)
    return html_lib.unescape(text).replace("\xa0", " ").strip()


def sku_digits(raw: str | None) -> str:
    if not raw:
        return ""
    m = re.search(r"(\d{7,})", str(raw).upper().replace("MX", ""))
    return m.group(1) if m else ""


def sku_base(digits: str) -> str:
    """Artikelbasis: eerste 7 cijfers (kleur/maat in de rest)."""
    return digits[:7] if len(digits) >= 7 else digits


# ---------------------------------------------------------------------------
# Index builder
# ---------------------------------------------------------------------------


def abs_url(href: str) -> str:
    if href.startswith("http"):
        return href
    return urllib.parse.urljoin(BASE + "/", href.lstrip("/"))


def discover_list_paths(seed_html: str) -> set[str]:
    paths = set()
    for m in re.finditer(
        r'href="(/(?:e-bikes|light-ebikes|bikes)/list/[^"#?]+)"', seed_html
    ):
        paths.add(m.group(1).rstrip("/"))
    return paths


def parse_list_cards(html: str) -> list[dict]:
    """Extract data-sku cards with detail href + optional subtitle/battery."""
    items: list[dict] = []
    seen: set[str] = set()

    # Find each data-sku occurrence and search a window for detail href
    for m in re.finditer(r'\bdata-sku="(MX\d+)"', html, re.I):
        sku = m.group(1).upper()
        digits = sku_digits(sku)
        if not digits or digits in seen:
            continue
        start = max(0, m.start() - 200)
        end = min(len(html), m.end() + 2500)
        chunk = html[start:end]
        href_m = re.search(
            r'href="((?:/e-bikes|/light-ebikes|/bikes)/detail/[^"]+)"', chunk, re.I
        )
        if not href_m:
            continue
        seen.add(digits)
        sub_m = re.search(
            r'class="[^"]*product-subtitle[^"]*"[^>]*>(.*?)</', chunk, re.I | re.S
        )
        filt: dict = {}
        filt_m = re.search(r"data-filter-item='([^']+)'", chunk)
        if not filt_m:
            filt_m = re.search(r'data-filter-item="([^"]+)"', chunk)
        if filt_m:
            try:
                filt = json.loads(html_lib.unescape(filt_m.group(1)))
            except json.JSONDecodeError:
                filt = {}
        items.append(
            {
                "sku": sku,
                "digits": digits,
                "href": href_m.group(1),
                "subtitle": strip_tags(sub_m.group(1)) if sub_m else "",
                "filter": filt,
                "source": "list",
            }
        )
    return items


def parse_archive_cards(html: str) -> list[dict]:
    items: list[dict] = []
    seen: set[str] = set()
    for m in re.finditer(
        r'href="(/archive/detail/(mx\d+)[^"]*)"', html, re.I
    ):
        digits = re.search(r"\d+", m.group(2)).group(0)
        if digits in seen:
            continue
        seen.add(digits)
        sku = "MX" + digits
        items.append(
            {
                "sku": sku,
                "digits": digits,
                "href": html_lib.unescape(m.group(1)),
                "subtitle": "",
                "filter": {},
                "source": "archive",
            }
        )
    return items


def archive_pager_links(html: str) -> list[str]:
    links: list[str] = []
    for m in re.finditer(
        r'<a[^>]+href="([^"]+)"[^>]*class="page-link"', html, re.I
    ):
        links.append(html_lib.unescape(m.group(1)))
    for m in re.finditer(
        r'<a[^>]+class="page-link"[^>]+href="([^"]+)"', html, re.I
    ):
        links.append(html_lib.unescape(m.group(1)))
    out: list[str] = []
    seen: set[str] = set()
    for href in links:
        if not href.startswith("/archive"):
            continue
        # After unescape: tx_products_productarchive[currentPage]=N
        if "currentPage" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        out.append(href)
    return out


def crawl_archive(*, delay: float) -> list[dict]:
    """Walk archive pagination (cHash required). Year POST filter is unreliable."""
    items: list[dict] = []
    seen_pages: set[str] = set()
    queue: list[str] = ["/archive"]
    while queue:
        path = queue.pop(0)
        if path in seen_pages:
            continue
        seen_pages.add(path)
        url = abs_url(path)
        try:
            html = http_get(url)
        except Exception as exc:  # noqa: BLE001
            print(f"    archive fail {path[:80]}: {exc}")
            continue
        cards = parse_archive_cards(html)
        items.extend(cards)
        for link in archive_pager_links(html):
            if link not in seen_pages:
                queue.append(link)
        print(
            f"    archive page {len(seen_pages)}: +{len(cards)} "
            f"(unique digits so far ~{len({c['digits'] for c in items})}, queue={len(queue)})"
        )
        time.sleep(delay)
    # Deduplicate by digits keeping first
    by_digits: dict[str, dict] = {}
    for c in items:
        by_digits.setdefault(c["digits"], c)
    return list(by_digits.values())


def build_index(*, rebuild: bool, delay: float) -> dict[str, dict]:
    """Return map digits -> best index entry (exact SKU preferred later at lookup)."""
    CACHE.mkdir(parents=True, exist_ok=True)
    if INDEX_PATH.exists() and not rebuild:
        data = json.loads(INDEX_PATH.read_text())
        print(f"  index geladen: {len(data)} SKUs ({INDEX_PATH})")
        return data

    by_digits: dict[str, dict] = {}

    def add(entry: dict) -> None:
        digits = entry.get("digits") or sku_digits(entry.get("sku"))
        if not digits:
            return
        entry = dict(entry)
        entry["digits"] = digits
        entry["url"] = abs_url(entry["href"])
        entry["base"] = sku_base(digits)
        prev = by_digits.get(digits)
        if prev is None:
            by_digits[digits] = entry
            return
        rank = {"archive": 2, "list": 3, "list-href": 1}.get(entry["source"], 0)
        prev_rank = {"archive": 2, "list": 3, "list-href": 1}.get(prev["source"], 0)
        # Prefer list (current) only when same year prefix; else keep archive for older SKUs
        if entry["source"] == "list" and prev["source"] == "archive":
            if entry["digits"][:3] == prev["digits"][:3]:
                if not entry.get("subtitle") and prev.get("subtitle"):
                    entry["subtitle"] = prev["subtitle"]
                by_digits[digits] = entry
            return
        if rank >= prev_rank:
            if not entry.get("subtitle") and prev.get("subtitle"):
                entry["subtitle"] = prev["subtitle"]
            by_digits[digits] = entry

    # Seed + discover list paths
    list_paths: set[str] = set(LIST_SEEDS)
    print("  list-pagina's scrapen…")
    for seed in list(LIST_SEEDS):
        try:
            html = http_get(abs_url(seed))
            list_paths |= discover_list_paths(html)
            time.sleep(delay)
        except Exception as exc:  # noqa: BLE001
            print(f"    seed fail {seed}: {exc}")

    for path in sorted(list_paths):
        try:
            html = http_get(abs_url(path))
            cards = parse_list_cards(html)
            for c in cards:
                add(c)
            print(f"    {path}: {len(cards)} cards (index={len(by_digits)})")
            time.sleep(delay)
        except Exception as exc:  # noqa: BLE001
            print(f"    list fail {path}: {exc}")

    print("  archive scrapen (cHash-pager)…")
    for c in crawl_archive(delay=delay):
        add(c)
    print(f"  archive toegevoegd, index={len(by_digits)}")

    INDEX_PATH.write_text(json.dumps(by_digits, ensure_ascii=False, indent=2))
    print(f"  index geschreven: {len(by_digits)} SKUs → {INDEX_PATH}")
    return by_digits


def lookup_index(index: dict[str, dict], digits: str) -> dict | None:
    if not digits:
        return None
    if digits in index:
        return index[digits]
    base = sku_base(digits)
    candidates = [e for e in index.values() if e.get("base") == base]
    if not candidates:
        # Try same first 6 digits as weaker fallback
        weak = digits[:6]
        candidates = [e for e in index.values() if e.get("digits", "").startswith(weak)]
    if not candidates:
        return None

    def score(entry: dict) -> tuple:
        # Prefer exact year prefix (first 3 digits), then archive, then shorter digit distance
        same_year = 1 if entry["digits"][:3] == digits[:3] else 0
        is_archive = 1 if entry.get("source") == "archive" else 0
        dist = abs(int(entry["digits"]) - int(digits)) if entry["digits"].isdigit() else 10**12
        return (same_year, is_archive, -dist)

    return sorted(candidates, key=score, reverse=True)[0]


# ---------------------------------------------------------------------------
# Detail scraper
# ---------------------------------------------------------------------------


def parse_spec_table(html: str) -> dict[str, str]:
    specs: dict[str, str] = {}
    for m in re.finditer(
        r"<tr[^>]*>\s*<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>\s*</tr>",
        html,
        re.I | re.S,
    ):
        label = strip_tags(m.group(1)).upper()
        value = strip_tags(m.group(2))
        if label and value and label not in specs:
            # Skip geometry-ish labels that appear in size tabs with same table class
            if label in {
                "SIZE",
                "SEATTUBE LENGTH",
                "TOPTUBE LENGTH",
                "SEAT ANGLE",
                "HEAD ANGLE",
                "HEADTUBE LENGTH",
                "CHAINSTAY LENGTH",
                "WHEELBASE",
                "STACK",
                "REACH",
                "STANDOVER HEIGHT",
                "BB DROP",
            }:
                continue
            specs[label] = value
    return specs


def parse_geometry(html: str) -> dict[str, dict[str, str]]:
    geometry: dict[str, dict[str, str]] = {}
    # Map tab id -> size label from nav
    tab_labels: dict[str, str] = {}
    for m in re.finditer(
        r'href="#(geometry-[^"]+)"[^>]*>\s*([^<]+)', html, re.I
    ):
        label = strip_tags(m.group(2)).upper()
        if label and label != "GEOMETRY":
            tab_labels[m.group(1)] = label

    for pane_id, size in tab_labels.items():
        pane_m = re.search(
            rf'id="{re.escape(pane_id)}"[^>]*>(.*?)</div>\s*(?=<div[^>]*id="geometry-|<div[^>]*class="[^"]*tab-content|$)',
            html,
            re.I | re.S,
        )
        if not pane_m:
            # simpler: find id= and take until next geometry- id
            pane_m = re.search(
                rf'id="{re.escape(pane_id)}"[^>]*>(.*?)(?=id="geometry-|$)',
                html,
                re.I | re.S,
            )
        if not pane_m:
            continue
        chunk = pane_m.group(1)
        rows: dict[str, str] = {}
        for rm in re.finditer(
            r"<tr[^>]*>\s*<th[^>]*>(.*?)</th>\s*<td[^>]*>(.*?)</td>\s*</tr>",
            chunk,
            re.I | re.S,
        ):
            label = strip_tags(rm.group(1))
            value = strip_tags(rm.group(2))
            if label and value:
                rows[label] = value
        if rows:
            geometry[size] = rows
    return geometry


def parse_marketing(html: str) -> str:
    # Prefer the hero marketing column paragraph(s)
    chunks: list[str] = []
    for m in re.finditer(
        r'class="[^"]*col-lg-6[^"]*offset-lg-6[^"]*"[^>]*>(.*?)</div>',
        html,
        re.I | re.S,
    ):
        block = m.group(1)
        for pm in re.finditer(r"<p[^>]*>(.*?)</p>", block, re.I | re.S):
            text = strip_tags(pm.group(1))
            if len(text) > 80:
                chunks.append(text)
        if chunks:
            break
    if not chunks:
        # Fallback: longest paragraph on page that looks like marketing
        paras = [
            strip_tags(pm.group(1))
            for pm in re.finditer(r"<p[^>]*>(.*?)</p>", html, re.I | re.S)
        ]
        paras = [p for p in paras if len(p) > 120 and not p.lower().startswith("cookie")]
        if paras:
            chunks = [max(paras, key=len)]
    if not chunks:
        return ""
    # Split long single blobs into paragraphs on sentence boundaries if no breaks
    text = "\n\n".join(chunks)
    if "\n\n" not in text and len(text) > 400:
        parts = re.split(r"(?<=\.)\s+(?=[A-ZÀ-ÖØ-Þ])", text)
        # Group into ~2-3 sentence paragraphs
        paras: list[str] = []
        buf: list[str] = []
        for sent in parts:
            buf.append(sent.strip())
            if len(buf) >= 2:
                paras.append(" ".join(buf))
                buf = []
        if buf:
            paras.append(" ".join(buf))
        text = "\n\n".join(p for p in paras if p)
    return text.strip()


def parse_subtitle(html: str) -> str:
    # First subtitle after the product H1 (avoid related-product cards lower on page)
    m = re.search(
        r"<h1[^>]*>[\s\S]*?</h1>[\s\S]{0,2500}?class=\"[^\"]*product-subtitle[^\"]*\"[^>]*>(.*?)</",
        html,
        re.I,
    )
    if m:
        return strip_tags(m.group(1))
    m = re.search(
        r'class="[^"]*product-subtitle[^"]*"[^>]*>(.*?)</', html, re.I | re.S
    )
    return strip_tags(m.group(1)) if m else ""


def extract_battery_wh(specs: dict[str, str], filt: dict | None = None) -> int | None:
    if filt:
        for item in filt.get("battery_e_bike") or []:
            m = re.search(r"(\d+)\s*Wh", str(item), re.I)
            if m:
                return int(m.group(1))
    for key in ("BATTERY", "BATTERY-CAPACITY", "BATTERY CAPACITY"):
        val = specs.get(key) or ""
        m = re.search(r"(\d+)\s*Wh", val, re.I)
        if m:
            return int(m.group(1))
        if key == "BATTERY-CAPACITY" and val.isdigit():
            return int(val)
    return None


def extract_decimal(specs: dict[str, str], *labels: str) -> str | None:
    for label in labels:
        val = specs.get(label.upper()) or specs.get(label)
        if not val:
            continue
        m = re.search(r"(\d+[.,]?\d*)", val.replace(" ", ""))
        if m:
            return m.group(1).replace(",", ".")
    return None


def scrape_detail(url: str, *, delay: float, use_cache: bool = True) -> dict:
    DETAIL_CACHE.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_-]+", "_", url.split("//", 1)[-1])[:180]
    cache_file = DETAIL_CACHE / f"{safe}.html"
    if use_cache and cache_file.exists():
        html = cache_file.read_text(errors="replace")
    else:
        html = http_get(url)
        cache_file.write_text(html)
        time.sleep(delay)

    specs = parse_spec_table(html)
    geometry = parse_geometry(html)
    marketing = parse_marketing(html)
    subtitle = parse_subtitle(html)
    battery_wh = extract_battery_wh(specs)
    motor = specs.get("MOTOR") or ""
    # Prefer short motor string; drop parenthetical noise lightly
    weight = extract_decimal(specs, "WEIGHT")
    max_w = extract_decimal(specs, "PERMISSIBLE TOTAL WEIGHT")

    return {
        "url": url,
        "subtitle": subtitle,
        "marketing": marketing,
        "geometry": geometry,
        "specs": specs,
        "battery_wh": battery_wh,
        "motor": motor.strip(),
        "weight_kg": weight,
        "max_system_weight_kg": max_w,
    }


# ---------------------------------------------------------------------------
# Shopify
# ---------------------------------------------------------------------------


def ensure_definitions(dry_run: bool) -> None:
    for item in DEFINITIONS:
        existing = gql(
            """
            query($ns:String!, $key:String!) {
              metafieldDefinitions(first:1, ownerType:PRODUCT, namespace:$ns, key:$key) {
                nodes { id }
              }
            }
            """,
            {"ns": NS, "key": item["key"]},
        )
        nodes = (
            ((existing.get("data") or {}).get("metafieldDefinitions") or {}).get("nodes")
            or []
        )
        if nodes:
            continue
        print(f"  metafield aanmaken: {NS}.{item['key']}")
        if dry_run:
            continue
        created = gql(
            """
            mutation($definition: MetafieldDefinitionInput!) {
              metafieldDefinitionCreate(definition: $definition) {
                createdDefinition { id }
                userErrors { field message code }
              }
            }
            """,
            {
                "definition": {
                    "name": item["name"],
                    "namespace": NS,
                    "key": item["key"],
                    "description": item["description"],
                    "type": item["type"],
                    "ownerType": "PRODUCT",
                    "access": {"storefront": "PUBLIC_READ"},
                }
            },
        )
        result = ((created.get("data") or {}).get("metafieldDefinitionCreate")) or {}
        errors = result.get("userErrors") or created.get("errors") or []
        if errors:
            raise RuntimeError(f"Definitie {item['key']}: {errors}")


def load_collection_products() -> list[dict]:
    products: list[dict] = []
    cursor = None
    while True:
        data = gql(
            """
            query($handle:String!, $cursor:String) {
              collectionByHandle(handle:$handle) {
                products(first:50, after:$cursor) {
                  pageInfo { hasNextPage endCursor }
                  nodes {
                    id
                    title
                    handle
                    descriptionHtml
                    variants(first:20) {
                      nodes { sku }
                    }
                    metafields(first:30, namespace:"custom") {
                      nodes { key value }
                    }
                  }
                }
              }
            }
            """,
            {"handle": COLLECTION, "cursor": cursor},
        )
        col = (data.get("data") or {}).get("collectionByHandle")
        if not col:
            raise RuntimeError(f"Collectie {COLLECTION} niet gevonden")
        conn = col["products"]
        products.extend(conn["nodes"])
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
    return products


def product_skus(product: dict) -> list[str]:
    out = []
    for v in (product.get("variants") or {}).get("nodes") or []:
        d = sku_digits(v.get("sku"))
        if d:
            out.append(d)
    return out


def existing_enrichment(product: dict) -> dict[str, str]:
    return {
        m["key"]: m.get("value") or ""
        for m in (product.get("metafields") or {}).get("nodes") or []
        if m.get("key") in ENRICHMENT_KEYS
    }


def enrichment_complete(existing: dict[str, str]) -> bool:
    # Complete if we have geometry + (subtitle or marketing) — battery optional for non-e
    if not (existing.get("geometry") or "").strip():
        return False
    if not (
        (existing.get("bike_subtitle") or "").strip()
        or (existing.get("marketing_text") or "").strip()
    ):
        return False
    return True


def strip_legacy_description(html: str) -> str | None:
    if not html:
        return None
    new = html
    patterns = [
        r"(?is)<tr>\s*<td[^>]*>\s*<b>\s*<br\s*/?>\s*</b>\s*</td>\s*<td[^>]*>\s*<br\s*/?>\s*</td>\s*</tr>\s*",
        r"(?is)<tr>\s*<td[^>]*>\s*<b>\s*Geometrie\s*</b>\s*</td>\s*<td[^>]*>.*?Klik hier voor de geometrie.*?</td>\s*</tr>\s*",
        r"(?is)<tr>\s*<td[^>]*>\s*<b>\s*Frame size\s*</b>\s*</td>\s*<td[^>]*>.*?Klik hier voor de juiste maatvoering.*?</td>\s*</tr>\s*",
    ]
    changed = False
    for p in patterns:
        new2, n = re.subn(p, "", new)
        if n:
            changed = True
            new = new2
    return new if changed else None


def build_metafields(product_id: str, scraped: dict) -> list[dict]:
    mfs: list[dict] = []

    def add(key: str, typ: str, value: str | None) -> None:
        if value is None or value == "":
            return
        mfs.append(
            {
                "ownerId": product_id,
                "namespace": NS,
                "key": key,
                "type": typ,
                "value": value,
            }
        )

    if scraped.get("battery_wh") is not None:
        add("battery_wh", "number_integer", str(int(scraped["battery_wh"])))
        add(
            "motorvermogen",
            "single_line_text_field",
            f'{int(scraped["battery_wh"])}Wh',
        )
    add("motor", "single_line_text_field", scraped.get("motor") or None)
    add("weight_kg", "number_decimal", scraped.get("weight_kg"))
    add(
        "max_system_weight_kg",
        "number_decimal",
        scraped.get("max_system_weight_kg"),
    )
    geom = scraped.get("geometry") or {}
    if geom:
        add("geometry", "json", json.dumps(geom, ensure_ascii=False))
    add("marketing_text", "multi_line_text_field", scraped.get("marketing") or None)
    add("bike_subtitle", "single_line_text_field", scraped.get("subtitle") or None)
    return mfs


def write_product(
    product: dict, scraped: dict, *, dry_run: bool
) -> dict:
    product_id = product["id"]
    mfs = build_metafields(product_id, scraped)
    new_html = strip_legacy_description(product.get("descriptionHtml") or "")

    result = {
        "metafields": len(mfs),
        "keys": [m["key"] for m in mfs],
        "description_cleaned": bool(new_html),
    }

    if dry_run:
        return result

    if mfs:
        res = gql(
            """
            mutation($metafields: [MetafieldsSetInput!]!) {
              metafieldsSet(metafields: $metafields) {
                metafields { id key }
                userErrors { field message }
              }
            }
            """,
            {"metafields": mfs},
        )
        payload = ((res.get("data") or {}).get("metafieldsSet")) or {}
        errors = payload.get("userErrors") or res.get("errors") or []
        if errors:
            raise RuntimeError(errors)

    if new_html is not None:
        res = gql(
            """
            mutation($input: ProductInput!) {
              productUpdate(input: $input) {
                product { id }
                userErrors { field message }
              }
            }
            """,
            {"input": {"id": product_id, "descriptionHtml": new_html}},
        )
        payload = ((res.get("data") or {}).get("productUpdate")) or {}
        errors = payload.get("userErrors") or res.get("errors") or []
        if errors:
            raise RuntimeError(errors)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=0, help="Max producten")
    parser.add_argument(
        "--only-missing",
        action="store_true",
        help="Skip producten die al geometry+subtitle/marketing hebben",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overschrijf bestaande enrichment (negeert --only-missing)",
    )
    parser.add_argument("--rebuild-index", action="store_true")
    parser.add_argument("--delay", type=float, default=0.35)
    parser.add_argument("--no-detail-cache", action="store_true")
    args = parser.parse_args()

    CACHE.mkdir(parents=True, exist_ok=True)
    print(f"Shop: {config.SHOPIFY_SHOP_DOMAIN}")
    print(f"Collectie: {COLLECTION}")

    print("1) Metafield-definities")
    ensure_definitions(args.dry_run)

    print("2) ktm-bikes.at index")
    index = build_index(rebuild=args.rebuild_index, delay=args.delay)

    print("3) Shopify producten laden")
    products = load_collection_products()
    print(f"  {len(products)} producten")

    report = {
        "ok": [],
        "skipped_complete": [],
        "no_match": [],
        "scrape_fail": [],
        "write_fail": [],
    }

    processed = 0
    for product in products:
        if args.limit and processed >= args.limit:
            break

        existing = existing_enrichment(product)
        if args.only_missing and not args.force and enrichment_complete(existing):
            report["skipped_complete"].append(
                {"handle": product["handle"], "title": product["title"]}
            )
            continue

        skus = product_skus(product)
        match = None
        matched_via = None
        for s in skus:
            match = lookup_index(index, s)
            if match:
                matched_via = s
                break

        processed += 1
        label = f"{product['title'][:50]} [{product['handle']}]"

        if not match:
            print(f"  NO_MATCH {label} skus={skus}")
            report["no_match"].append(
                {"handle": product["handle"], "title": product["title"], "skus": skus}
            )
            continue

        print(
            f"  {'DRY ' if args.dry_run else ''}MATCH {label} "
            f"sku={matched_via} → {match['sku']} ({match['source']})"
        )
        try:
            scraped = scrape_detail(
                match["url"],
                delay=args.delay,
                use_cache=not args.no_detail_cache,
            )
            # Prefer list subtitle if detail missing
            if not scraped.get("subtitle") and match.get("subtitle"):
                scraped["subtitle"] = match["subtitle"]
            if scraped.get("battery_wh") is None:
                scraped["battery_wh"] = extract_battery_wh({}, match.get("filter"))
        except Exception as exc:  # noqa: BLE001
            print(f"    SCRAPE_FAIL: {exc}")
            report["scrape_fail"].append(
                {
                    "handle": product["handle"],
                    "title": product["title"],
                    "url": match["url"],
                    "error": str(exc),
                }
            )
            continue

        if (
            not scraped.get("geometry")
            and not scraped.get("marketing")
            and not scraped.get("motor")
            and not scraped.get("subtitle")
            and scraped.get("battery_wh") is None
            and not scraped.get("weight_kg")
        ):
            print("    SCRAPE_FAIL: empty payload")
            report["scrape_fail"].append(
                {
                    "handle": product["handle"],
                    "title": product["title"],
                    "url": match["url"],
                    "error": "empty payload",
                }
            )
            continue

        try:
            result = write_product(product, scraped, dry_run=args.dry_run)
            print(
                f"    keys={result['keys']} cleaned={result['description_cleaned']}"
            )
            report["ok"].append(
                {
                    "handle": product["handle"],
                    "title": product["title"],
                    "matched_sku": match["sku"],
                    "url": match["url"],
                    "keys": result["keys"],
                    "description_cleaned": result["description_cleaned"],
                    "battery_wh": scraped.get("battery_wh"),
                    "subtitle": scraped.get("subtitle"),
                }
            )
        except Exception as exc:  # noqa: BLE001
            print(f"    WRITE_FAIL: {exc}")
            report["write_fail"].append(
                {
                    "handle": product["handle"],
                    "title": product["title"],
                    "error": str(exc),
                }
            )

    report["summary"] = {
        "ok": len(report["ok"]),
        "skipped_complete": len(report["skipped_complete"]),
        "no_match": len(report["no_match"]),
        "scrape_fail": len(report["scrape_fail"]),
        "write_fail": len(report["write_fail"]),
        "dry_run": args.dry_run,
        "limit": args.limit,
        "only_missing": args.only_missing,
        "index_size": len(index),
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print("4) Rapport")
    print(json.dumps(report["summary"], indent=2))
    print(f"  → {REPORT_PATH}")
    print("Klaar." + (" (dry-run)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
