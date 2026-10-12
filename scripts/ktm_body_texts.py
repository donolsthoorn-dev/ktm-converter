#!/usr/bin/env python3
"""
ktm-shop.nl: AI-bodyteksten voor producten met lege, table-only of te korte body.

Gebruikt titel, productType, tags, bruikbare eigenschappen (Collection /
Playground / Gender) en optioneel ymm_summary als context. Geen kleur of
variantenmaat in de tekst — die wisselen per variant terwijl de body vaststaat.

Alleen thin bodies (fill-if-empty). Goede TEXTART-teksten blijven staan.
Zonder --apply geen writes naar Shopify.

  python3 scripts/ktm_body_texts.py --limit 40
  python3 scripts/ktm_body_texts.py --handle 00010000363 --handle 3ki210026800
  python3 scripts/ktm_body_texts.py --limit 40 --apply

Cache: cache/ktm/body_texts.json
"""

from __future__ import annotations

import argparse
import csv
import html
import importlib.util
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(1)

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.chdir(ROOT)

import config  # noqa: E402
from modules.seo_completeness import collapse_ws, strip_html, strip_homnn_plain  # noqa: E402

_motox_seo_path = ROOT / "scripts" / "motox_ebihr_seo_descriptions.py"
_spec = importlib.util.spec_from_file_location("motox_ebihr_seo_descriptions", _motox_seo_path)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Kan {_motox_seo_path} niet laden")
motox_seo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(motox_seo)

PROMPT_VERSION = "3"
CACHE_PATH = ROOT / "cache" / "ktm" / "body_texts.json"
THIN_PLAIN_MAX = 80
# Producttypes die meestal geen winkeltekst nodig hebben
_SKIP_TYPES = frozenset(
    {
        "partstream",
        "pos",
        "event material",
        "marketing material",
        "folder",
        "engine sale",
    }
)
_SKIP_TAG_FRAGMENTS = frozenset(
    {
        "partstream",
        "marketing material",
        "event material",
        "folder",
    }
)

_SKIP_TABLE_LABELS = frozenset(
    {
        "colour",
        "color",
        "kleur",
        "size",
        "maat",
        "dimension",
        "afmeting",
        "approvals",
        "model year",
        "type",
        "surface",
        "not available in",
        "terrain",
        "factory racing",
    }
)
_KEEP_TABLE_LABELS = frozenset(
    {
        "gender",
        "ktm collection",
        "ktm playground",
        "collection",
        "playground",
        "material",
    }
)
# Engelse tabel-labels → Nederlands in de bewaarde eigenschappentabel
_TABLE_LABEL_NL = {
    "gender": "Geslacht",
    "material": "Materiaal",
    "ktm collection": "KTM-collectie",
    "ktm playground": "KTM Playground",
    "collection": "Collectie",
    "playground": "Playground",
}
_MATERIAL_NL = {
    "aluminium": "aluminium",
    "aluminum": "aluminium",
    "stainless steel": "roestvrij staal",
    "synthetic material": "kunststof",
    "carbon": "carbon",
    "titanium": "titanium",
    "steel": "staal",
    "plastic": "kunststof",
    "rubber": "rubber",
    "cotton": "katoen",
}
_APPROVAL_OR_YEAR = re.compile(
    r"(?i)\b(?:approvals?|goedkeuring(?:en)?|homnn(?:[_\s-]?eu)?|abe|ec-v|cccuo(?:_eu)?|"
    r"model\s*year|modeljaar|not available in)\b"
)
_FLUFF = re.compile(
    r"(?i)\b(?:essentieel\w*|ideaal\w*|unie\w*|perfect\w*|must-?have|versterkt de|"
    r"trendy|lifestyle van de|community)\b"
)
_COLOR_WORD = re.compile(
    r"(?i)\b(?:colour|color|kleur|zwart|wit|rood|blauw|groen|geel|oranje|grijs|"
    r"gray|black|white|red|blue|green|yellow|orange|grey|silver|gold|beige|"
    r"pink|paars|purple|navy|anthracite|antraciet)\b"
)
_FACTORY_RACING = re.compile(r"(?i)\bfactory\s*racing\b")
_OTHER_BRAND = re.compile(r"(?i)\b(?:husqvarna|gas\s*gas|gasgas|wp\b|honda|yamaha|kawasaki|suzuki|bmw)\b")
_ENGLISH_LABEL = re.compile(
    r"(?i)\b(?:material|gender|colour|color|approvals?|factory racing|type|tags)\s*:"
)
_META_LEAK = re.compile(r"(?i)\b(?:nada-hidden|synkro-clone)\b")
# Jaren / cc-ranges / modelcodes die alleen mogen als ze in de titel staan
_YEAR_TOKEN = re.compile(r"\b(?:19|20)\d{2}\b")
_CC_RANGE = re.compile(r"\b\d{2,4}\s*[-–—]\s*\d{2,4}\b")
_MODEL_TOKEN = re.compile(
    r"(?i)\b(?:exc|exc-f|sx|sxf|xc|xcf|xcw|smr|smc|sms|duke|superduke|"
    r"adventure|freeride|enduro|supermoto|rc8r?|lc4|tpi)\b"
)
_FITMENT_PHRASE = re.compile(
    r"(?i)\b(?:geschikt voor|past op|compatibel met|specifiek voor|"
    r"ontwikkeld voor gebruik|voor verschillende|voor diverse)\b"
)
_TABLE_RE = re.compile(r"(?is)<table\b[^>]*product--properties-table[^>]*>.*?</table>")
_ROW_RE = re.compile(r"(?is)<tr\b[^>]*>.*?</tr>")
_CELL_RE = re.compile(r"(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>")

_SYSTEM = """Je schrijft Nederlandse productbodyteksten (HTML) voor de officiële KTM-webshop ktm-shop.nl.
Regels:
- Alleen feiten uit titel, type, tags en toegestane eigenschappen. Verzin niets.
- Geen kleur en geen kleding-/variantenmaat (S, M, L, XL, maat 42, size, zwart, oranje, …).
- Technische maat die het artikel zelf is (M8x20, Ø14 mm) mag wél als die in de titel staat.
- GEEN passendheid/modellen/jaren gokken. Noem geen motorfamilie (EXC, SX, Duke, …), geen cc-range en geen jaartallen, tenzij die letterlijk in de producttitel staan. YMM/metafields bestaan apart en horen niet in de body.
- Noem geen andere merken (Husqvarna, GasGas, …) tenzij die letterlijk in de titel staan.
- Geen "Factory Racing", geen goedkeuringen (ABE, HOMNN, …), geen intern modeljaar uit tabellen.
- Materiaal en andere eigenschappen in het Nederlands (bijv. "aluminium", "roestvrij staal", "kunststof") — geen Engelse labels zoals "Material:".
- Geen prijs, verzending, voorraad of vage reclame ("essentieel", "ideaal", "uniek").
- Antwoord ALLEEN met HTML:
  <p>…één of twee korte, concrete zinnen…</p>
  <ul><li>…</li><li>…</li><li>…</li></ul>
- Maximaal 3 bulletpunten. Geen tabel, geen heading, geen class-attributen.
- "Origineel" alleen bij duidelijke OEM/PowerParts-onderdelen van KTM.
- Geen aanhalingstekens om het antwoord. Geen markdown."""

_QUERY_PAGE = """
query ($c: String, $q: String!) {
  products(first: 40, after: $c, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      vendor
      productType
      status
      tags
      descriptionHtml
      onlineStoreUrl
      ymm: metafield(namespace: "global", key: "ymm_summary") { value }
    }
  }
}
"""

_QUERY_HANDLE = """
query ($h: String!) {
  productByHandle(handle: $h) {
    id
    handle
    title
    vendor
    productType
    status
    tags
    descriptionHtml
    onlineStoreUrl
    ymm: metafield(namespace: "global", key: "ymm_summary") { value }
  }
}
"""

_MUT_BODY = """
mutation ($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id descriptionHtml }
    userErrors { field message }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _plain_len(body_html: str) -> int:
    return len(strip_homnn_plain(strip_html(body_html or "")))


def _has_marketing_copy(body_html: str) -> bool:
    """True als er tekst buiten de eigenschappentabel staat."""
    without = _TABLE_RE.sub(" ", body_html or "")
    return _plain_len(without) >= 12


def classify_body(body_html: str) -> str:
    raw = (body_html or "").strip()
    if not raw:
        return "empty"
    plain = _plain_len(raw)
    has_table = "product--properties-table" in raw
    if has_table and not _has_marketing_copy(raw):
        return "table_only"
    if plain < THIN_PLAIN_MAX:
        return "thin"
    return "ok"


def needs_body(body_html: str) -> bool:
    return classify_body(body_html) != "ok"


def _cell_text(raw: str) -> str:
    return collapse_ws(strip_html(raw))


def extract_table_facts(body_html: str) -> dict[str, str]:
    """Haal bruikbare (niet-kleur/maat) eigenschappen uit de properties-tabel."""
    facts: dict[str, str] = {}
    for m in _TABLE_RE.finditer(body_html or ""):
        for row in _ROW_RE.finditer(m.group(0)):
            cells = [_cell_text(c) for c in _CELL_RE.findall(row.group(0))]
            if len(cells) < 2:
                continue
            label = cells[0].rstrip(":").strip().lower()
            value = cells[1].strip()
            if not label or not value:
                continue
            if label in _SKIP_TABLE_LABELS or any(s in label for s in _SKIP_TABLE_LABELS):
                continue
            if label in _KEEP_TABLE_LABELS or label.startswith("ktm "):
                facts[label] = _value_nl(label, value)
    return facts


def _value_nl(label: str, value: str) -> str:
    if label == "material":
        parts = [p.strip() for p in re.split(r"[,/;]", value) if p.strip()]
        mapped = [_MATERIAL_NL.get(p.lower(), p.lower()) for p in parts]
        return ", ".join(mapped)
    if label == "gender":
        g = value.strip().lower()
        return {
            "men": "heren",
            "man": "heren",
            "women": "dames",
            "woman": "dames",
            "unisex": "unisex",
            "babies and children": "kinderen",
            "kids": "kinderen",
            "children": "kinderen",
        }.get(g, value)
    return value


def strip_variant_rows_from_table(body_html: str) -> str:
    """Behoud properties-tabel zonder kleur/maat/Factory Racing; labels in het Nederlands."""

    def _clean_table(m: re.Match[str]) -> str:
        table = m.group(0)
        kept_rows: list[str] = []
        for row in _ROW_RE.finditer(table):
            cells = [_cell_text(c) for c in _CELL_RE.findall(row.group(0))]
            if len(cells) < 2:
                continue
            label = cells[0].rstrip(":").strip().lower()
            value = cells[1].strip()
            if label in _SKIP_TABLE_LABELS or any(s in label for s in _SKIP_TABLE_LABELS):
                continue
            if label not in _KEEP_TABLE_LABELS and not label.startswith("ktm "):
                continue
            nl_label = _TABLE_LABEL_NL.get(label, cells[0].rstrip(":").strip())
            nl_value = _value_nl(label, value)
            kept_rows.append(
                '<tr class="product--properties-row">'
                f'<td class="product--properties-label is--bold">{html.escape(nl_label)}:</td>'
                f'<td class="product--properties-label">{html.escape(nl_value)}</td>'
                "</tr>"
            )
        if not kept_rows:
            return ""
        return (
            '<table class="product--properties-table"><tbody>'
            + "".join(kept_rows)
            + "</tbody></table>"
        )

    return _TABLE_RE.sub(_clean_table, body_html or "").strip()


def _product_row(p: dict | None) -> dict | None:
    if not p or (p.get("status") or "").upper() == "ARCHIVED":
        return None
    tags = p.get("tags") or []
    if isinstance(tags, str):
        tag_list = [t.strip() for t in tags.split(",") if t.strip()]
    else:
        tag_list = [str(t).strip() for t in tags if str(t).strip()]
    typ = p.get("productType") or ""
    if _seed_skip(typ, ", ".join(tag_list)):
        return None
    body = p.get("descriptionHtml") or ""
    return {
        "id": p.get("id") or "",
        "handle": p.get("handle") or "",
        "title": p.get("title") or "",
        "vendor": p.get("vendor") or "",
        "type": typ,
        "status": p.get("status") or "",
        "tags": tag_list,
        "body_html": body,
        "kind": classify_body(body),
        "ymm": ((p.get("ymm") or {}) or {}).get("value") or "",
        "url": p.get("onlineStoreUrl") or "",
    }


def iter_products(
    sess,
    url,
    token,
    *,
    handles: list[str],
    limit: int,
    batch: str,
    fill_from_scan: bool = False,
):
    yielded = 0
    target = None if limit <= 0 else limit
    seen: set[str] = set()

    def _take(row: dict | None) -> bool:
        nonlocal yielded
        if row is None or row["id"] in seen or not needs_body(row["body_html"]):
            return False
        seen.add(row["id"])
        yielded += 1
        return True

    if handles:
        for handle in handles:
            if target is not None and yielded >= target:
                return
            body = motox_seo._gql(sess, url, token, _QUERY_HANDLE, {"h": handle})
            row = _product_row(((body.get("data") or {}).get("productByHandle")))
            if row is None:
                print(f"Overgeslagen {handle} (niet gevonden, gearchiveerd of uitgesloten)", flush=True)
                continue
            if not needs_body(row["body_html"]):
                print(f"Overgeslagen {handle} (body al voldoende: {row['kind']})", flush=True)
                continue
            if row["id"] in seen:
                continue
            seen.add(row["id"])
            yielded += 1
            yield row
        if not fill_from_scan or (target is not None and yielded >= target):
            return

    for query in motox_seo._BATCH_QUERIES[batch]:
        cursor = None
        while target is None or yielded < target:
            body = motox_seo._gql(sess, url, token, _QUERY_PAGE, {"c": cursor, "q": query})
            conn = ((body.get("data") or {}).get("products")) or {}
            for node in conn.get("nodes") or []:
                if target is not None and yielded >= target:
                    return
                row = _product_row(node)
                if _take(row):
                    yield row
            page = conn.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
            time.sleep(0.2)
        if target is not None and yielded >= target:
            return


def _clean_model_html(raw: str) -> str:
    text = (raw or "").strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:html)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    text = text.strip().strip('"').strip("'")
    # Alleen p + ul toestaan
    text = re.sub(r"(?is)</?(?!/?\s*(?:p|ul|li|br)\b)[a-z0-9]+[^>]*>", " ", text)
    text = collapse_ws(text)
    # Herstel minimale whitespace in tags
    text = re.sub(r"\s*(</?(?:p|ul|li|br)\b[^>]*>)\s*", r"\1", text)
    text = text.replace("<br>", "<br />")
    return text.strip()


def _title_allows(token: str, title: str) -> bool:
    """True als token letterlijk (case-insensitive) in de titel voorkomt."""
    return token.lower() in (title or "").lower()


def _invented_fitment(text: str, title: str) -> bool:
    """True als de body modellen/jaren/merken noemt die niet in de titel staan."""
    plain = strip_html(text)
    title = title or ""

    if _OTHER_BRAND.search(plain):
        for m in _OTHER_BRAND.finditer(plain):
            if not _title_allows(m.group(0), title):
                return True

    for m in _YEAR_TOKEN.finditer(plain):
        if not _title_allows(m.group(0), title):
            return True

    for m in _CC_RANGE.finditer(plain):
        # normaliseer spaties/streepjes
        compact = re.sub(r"\s*", "", m.group(0)).replace("–", "-").replace("—", "-")
        title_compact = re.sub(r"\s*", "", title).replace("–", "-").replace("—", "-")
        if compact.lower() not in title_compact.lower():
            return True

    for m in _MODEL_TOKEN.finditer(plain):
        if not _title_allows(m.group(0), title):
            return True

    # Vage fitment zonder concrete titel-tokens
    if _FITMENT_PHRASE.search(plain):
        # Toegestaan alleen als de zin ook een token uit de titel herhaalt
        # (cc-range of modelcode). Anders: gok.
        has_title_model = bool(_MODEL_TOKEN.search(title) or _CC_RANGE.search(title) or _YEAR_TOKEN.search(title))
        if not has_title_model:
            return True
        # Zelfs mét titel-tokens: zin als "geschikt voor KTM en Husqvarna" is al via brand gefilterd;
        # "geschikt voor verschillende KTM-modellen" zonder titel-detail → afkeuren
        if re.search(r"(?i)verschillende|diverse\s+ktm|ktm-modellen|ktm modellen", plain):
            if not (_MODEL_TOKEN.search(plain) or _CC_RANGE.search(plain)):
                return True

    return False


def _mentions_variant_attrs(text: str, title: str = "") -> bool:
    plain = strip_html(text)
    if _COLOR_WORD.search(plain):
        return True
    if motox_seo.mentions_size(plain):
        return True
    if _APPROVAL_OR_YEAR.search(plain):
        return True
    if _FLUFF.search(plain):
        return True
    if _FACTORY_RACING.search(plain):
        return True
    if _ENGLISH_LABEL.search(plain):
        return True
    if _META_LEAK.search(plain):
        return True
    if _invented_fitment(text, title):
        return True
    return False


def _valid_body_html(text: str, title: str = "") -> bool:
    if not text or _mentions_variant_attrs(text, title):
        return False
    if "<p>" not in text.lower():
        return False
    plain = strip_html(text)
    if len(plain) < 40 or len(plain) > 900:
        return False
    return True


def _user_prompt(row: dict, facts: dict[str, str]) -> str:
    fact_lines = "\n".join(f"- {k}: {v}" for k, v in facts.items()) or "(geen)"
    tags = ", ".join(row.get("tags") or []) or "(geen)"
    plain = strip_homnn_plain(strip_html(row.get("body_html") or ""))[:500]
    plain = _COLOR_WORD.sub(" ", plain)
    plain = _APPROVAL_OR_YEAR.sub(" ", plain)
    plain = _FACTORY_RACING.sub(" ", plain)
    plain = motox_seo.strip_sizes(plain)
    plain = collapse_ws(plain)
    title = row.get("title") or "-"
    return (
        "Schrijf een Nederlandse productbody (HTML) voor dit KTM-artikel.\n"
        f"Merk: {row.get('vendor') or 'KTM'}\n"
        f"Titel: {title}\n"
        f"Type: {row.get('type') or '-'}\n"
        f"Tags: {tags}\n"
        f"Eigenschappen (Nederlands, geen kleur/maat/Factory Racing):\n{fact_lines}\n"
        f"Bestaande brontekst (opgeschoond):\n{plain or '(leeg)'}\n"
        "STRIKT: noem geen modellen, cc-ranges of jaartallen tenzij ze letterlijk in de titel staan. "
        "Geen Husqvarna/andere merken, geen Factory Racing, geen Engelse labels (Material:), "
        "geen kleur/maat, geen Past op / geschikt-voor-gokken."
    )


def propose_body(row: dict, *, call, model: str, cache: dict) -> tuple[str, str]:
    facts = extract_table_facts(row.get("body_html") or "")
    payload = {
        "v": PROMPT_VERSION,
        "model": model,
        "title": row["title"],
        "vendor": row["vendor"],
        "type": row["type"],
        "tags": row.get("tags") or [],
        "facts": facts,
        "body": strip_homnn_plain(strip_html(row.get("body_html") or ""))[:500],
    }
    items: dict = cache.setdefault("items", {})
    digest = motox_seo._source_hash(payload)
    cached = items.get(digest) or {}
    title = row.get("title") or ""
    if cached.get("body_html") and _valid_body_html(cached["body_html"], title):
        return cached["body_html"], "cache"
    if cached.get("body_html"):
        items.pop(digest, None)

    user = _user_prompt(row, facts)
    last_err = ""
    for _ in range(3):
        try:
            raw = call(user + (f"\nVorige poging afgekeurd: {last_err}" if last_err else ""))
        except SystemExit:
            raise
        except Exception as exc:
            print(f"AI-aanroep mislukt: {exc}"[:240], flush=True)
            return "", "fout"
        text = _clean_model_html(raw)
        if _valid_body_html(text, title):
            items[digest] = {
                "body_html": text,
                "handle": row["handle"],
                "model": model,
            }
            motox_seo._save_cache(CACHE_PATH, cache)
            return text, "ai"
        if _invented_fitment(text, title):
            last_err = (
                "geen modellen/jaren/merken noemen tenzij letterlijk in de titel; "
                "geen geschikt-voor-gokken"
            )
        elif _mentions_variant_attrs(text, title):
            last_err = (
                "geen kleur, maat, Factory Racing, Engelse labels of vage reclame"
            )
        elif "<p>" not in text.lower():
            last_err = "antwoord moet met <p>…</p> en optioneel <ul> beginnen"
        else:
            last_err = "tekst te kort, te lang, of ongeldige HTML"
    return "", "afgekeurd"


def assemble_body(generated: str, existing_html: str) -> str:
    """Nieuwe marketingcopy + opgeschoonde properties-tabel (zonder kleur/maat)."""
    cleaned_table = strip_variant_rows_from_table(existing_html)
    # Alleen de tabel zelf bewaren
    tables = _TABLE_RE.findall(cleaned_table)
    table_html = "".join(tables)
    parts = [generated.strip()]
    if table_html:
        parts.append(table_html)
    return "\n".join(parts)


def _write_body(sess, url, token, product_gid: str, description_html: str) -> str:
    body = motox_seo._gql(
        sess,
        url,
        token,
        _MUT_BODY,
        {"product": {"id": product_gid, "descriptionHtml": description_html}},
    )
    payload = ((body.get("data") or {}).get("productUpdate")) or {}
    errs = payload.get("userErrors") or []
    if errs:
        return str(errs)[:300]
    return ""


def _seed_skip(typ: str, tags: str) -> bool:
    low_type = (typ or "").strip().lower()
    if low_type in _SKIP_TYPES:
        return True
    low_tags = (tags or "").lower()
    return any(frag in low_tags for frag in _SKIP_TAG_FRAGMENTS)


def _load_seed_handles(path: Path, limit: int) -> list[str]:
    """Divers thin-sample uit een Shopify product-export CSV."""
    buckets: dict[str, list[str]] = {"empty": [], "table_only": [], "thin": []}
    seen_types: dict[str, set[str]] = {k: set() for k in buckets}
    with path.open(newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            title = (row.get("Title") or "").strip()
            if not title:
                continue
            handle = (row.get("Handle") or "").strip()
            if not handle:
                continue
            typ = (row.get("Type") or "").strip()
            tags = row.get("Tags") or ""
            if _seed_skip(typ, tags):
                continue
            kind = classify_body(row.get("Body (HTML)") or "")
            if kind == "ok":
                continue
            cap = max(10, limit // 3 + 3)
            if typ in seen_types[kind] and len(buckets[kind]) >= cap:
                continue
            seen_types[kind].add(typ)
            if handle not in buckets[kind]:
                buckets[kind].append(handle)
    out: list[str] = []
    for kind in ("empty", "table_only", "thin"):
        for h in buckets[kind]:
            if h not in out:
                out.append(h)
            if len(out) >= limit:
                return out
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=40, help="Aantal producten (0 = alle thin)")
    ap.add_argument("--handle", action="append", default=[], help="Alleen deze handle (herhaalbaar)")
    ap.add_argument(
        "--batch",
        choices=tuple(motox_seo._BATCH_QUERIES),
        default="priority",
        help="priority = eerst voorraad, daarna de rest (alleen zonder --handle/--seed-csv)",
    )
    ap.add_argument(
        "--seed-csv",
        type=Path,
        default=None,
        help="Shopify product-export CSV om diverse thin-handles te kiezen",
    )
    ap.add_argument("--apply", action="store_true", help="Schrijf descriptionHtml naar Shopify")
    ap.add_argument("--sleep", type=float, default=0.35, help="Pauze tussen AI-aanroepen")
    args = ap.parse_args()

    api_key, model, base_url = motox_seo._openai_settings()
    if not api_key:
        print("OPENAI_API_KEY ontbreekt. Zet die in .env (niet committen).", file=sys.stderr)
        return 2

    shop = (os.environ.get("SHOPIFY_SHOP_DOMAIN") or config.SHOPIFY_SHOP_DOMAIN or "").strip()
    token = (os.environ.get("SHOPIFY_ACCESS_TOKEN") or config.SHOPIFY_ACCESS_TOKEN or "").strip()
    api = (
        os.environ.get("SHOPIFY_ADMIN_API_VERSION") or config.SHOPIFY_ADMIN_API_VERSION or "2024-10"
    ).strip()
    if not shop or not token:
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1

    shop_url = f"https://{shop}/admin/api/{api}/graphql.json"
    sess = _session()
    cache = motox_seo._load_cache(CACHE_PATH)
    if cache.get("version") != PROMPT_VERSION:
        cache = {"version": PROMPT_VERSION, "items": cache.get("items") or {}}

    def call(user: str) -> str:
        url = f"{base_url}/chat/completions"
        payload = {
            "model": model,
            "temperature": 0.3,
            "messages": [
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": user},
            ],
        }
        for attempt in range(4):
            r = sess.post(
                url,
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
                timeout=motox_seo._REQUEST_TIMEOUT,
                proxies={"http": None, "https": None},
            )
            if r.status_code in (429, 500, 502, 503):
                time.sleep(1.5 + attempt)
                continue
            if r.status_code in (401, 403):
                raise SystemExit(
                    "AI-aanroep geweigerd (controleer OPENAI_API_KEY / OPENAI_BASE_URL)."
                )
            r.raise_for_status()
            body = r.json()
            choices = body.get("choices") or []
            if not choices:
                raise RuntimeError(f"Leeg modelantwoord: {str(body)[:300]}")
            return str((choices[0].get("message") or {}).get("content") or "")
        raise RuntimeError("AI-aanroep mislukt na retries")

    handles = list(args.handle or [])
    fill_from_scan = False
    if not handles and args.seed_csv:
        pool = max(args.limit * 4, 120) if args.limit > 0 else 120
        handles = _load_seed_handles(args.seed_csv, pool)
        fill_from_scan = True
        print(f"Seed-CSV: {args.seed_csv.name} → {len(handles)} kandidaten", flush=True)
    elif not handles:
        fill_from_scan = True
    elif args.limit > 0 and len(handles) < args.limit:
        # Bij een te korte handle-lijst verder zoeken tot --limit geslaagde teksten
        fill_from_scan = True
    elif args.limit > 0 and len(handles) >= args.limit:
        # Zelfde handles opnieuw: bij afkeuringen aanvullen via scan
        fill_from_scan = True

    mode = "APPLY" if args.apply else "dry-run"
    print(
        f"{mode}  shop={shop}  model={model}  batch={args.batch}  limit={args.limit}",
        flush=True,
    )

    rows: list[dict] = []
    written = 0
    wanted = args.limit if args.limit > 0 else 10**9
    # Haal ruim genoeg kandidaten op; alleen geslaagde voorstellen tellen mee
    fetch_limit = 0 if args.limit <= 0 else max(args.limit * 3, args.limit + 40)
    for product in iter_products(
        sess,
        shop_url,
        token,
        handles=handles,
        limit=fetch_limit,
        batch=args.batch,
        fill_from_scan=fill_from_scan,
    ):
        if len([r for r in rows if r.get("voorstel_marketing")]) >= wanted:
            break
        generated, bron = propose_body(product, call=call, model=model, cache=cache)
        if bron not in ("cache",) and generated:
            time.sleep(args.sleep)

        final = assemble_body(generated, product["body_html"]) if generated else ""
        if not final:
            print(
                f"\n[mislukt] {product['title']}\n"
                f"  handle {product['handle']}  kind={product['kind']}  {bron}",
                flush=True,
            )
            continue

        fout = ""
        if args.apply and final:
            fout = _write_body(sess, shop_url, token, product["id"], final)
            if not fout:
                written += 1
            time.sleep(0.25)

        status = "geschreven" if args.apply and final and not fout else (
            "fout" if fout else "voorstel"
        )
        plain_preview = strip_html(generated)[:160] if generated else ""
        print(
            f"\n[{status}] {product['title']}\n"
            f"  handle {product['handle']}  kind={product['kind']}  {bron}\n"
            f"  type {product['type']}  tags {', '.join(product['tags'][:4])}\n"
            f"  was: {strip_html(product['body_html'])[:100] or '(leeg)'}\n"
            f"  nieuw: {plain_preview}",
            flush=True,
        )
        if fout:
            print(f"  fout: {fout}", flush=True)

        rows.append(
            {
                "product_id": product["id"].rsplit("/", 1)[-1],
                "handle": product["handle"],
                "titel": product["title"],
                "vendor": product["vendor"],
                "type": product["type"],
                "tags": ", ".join(product["tags"]),
                "status": product["status"],
                "kind": product["kind"],
                "ymm": product["ymm"],
                "huidige_body": product["body_html"],
                "voorstel_marketing": generated,
                "voorstel_body": final,
                "bron": bron,
                "fout": fout,
                "url": product["url"],
            }
        )
        if len(rows) >= wanted:
            break

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"ktm_body_texts_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = list(rows[0].keys()) if rows else ["handle"]
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)

    # Leesbaar HTML-rapport voor review
    report = ROOT / "output" / f"ktm_body_texts_{stamp}.html"
    blocks = []
    for r in rows:
        blocks.append(
            "<section style='border-top:1px solid #ccc;padding:1rem 0'>"
            f"<h2>{html.escape(r['titel'])}</h2>"
            f"<p><code>{html.escape(r['handle'])}</code> · {html.escape(r['kind'])} · "
            f"{html.escape(r['type'])} · {html.escape(r['bron'])}</p>"
            f"<h3>Was</h3><div>{r['huidige_body'] or '<em>(leeg)</em>'}</div>"
            f"<h3>Voorstel</h3><div>{r['voorstel_body'] or '<em>(mislukt)</em>'}</div>"
            "</section>"
        )
    report.write_text(
        "<!doctype html><meta charset=utf-8><title>KTM bodyteksten batch</title>"
        + f"<p>{len(rows)} producten · {mode} · {html.escape(model)}</p>"
        + "".join(blocks),
        encoding="utf-8",
    )

    print(f"\nProducten: {len(rows)}  geschreven: {written if args.apply else 0}", flush=True)
    print(f"CSV: {out}", flush=True)
    print(f"HTML: {report}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
