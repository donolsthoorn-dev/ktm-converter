#!/usr/bin/env python3
"""
Motox: SEO-titel, AI-omschrijving en alt-tekst voor e-bihr-producten.

Leest producten van de Motox-shop. De SEO-titel en de alt-tekst zijn een
vaste regel (merk + productnaam, zonder maat, max. 70 tekens, geen afgekapt
woord). De omschrijving komt van het model, max. 160 tekens. Shopify wordt
alleen bij --apply bijgewerkt.

--batch priority (standaard) doet eerst actieve producten met voorraad,
daarna de rest. in-stock en rest beperken tot één groep.

OEM-merken (KTM, Husqvarna, GasGas, WP) worden overgeslagen.
Een product wordt overgeslagen als titel, omschrijving én alt-teksten al
gevuld zijn, tenzij --overwrite. Ontbreekt alleen titel of alt, dan geen
nieuwe modelaanroep.

API-sleutel (niet committen), in .env.motox of de omgeving:
  OPENAI_API_KEY=...
  OPENAI_MODEL=gpt-4o-mini          # optioneel
  OPENAI_BASE_URL=https://api.openai.com/v1   # optioneel, OpenAI-compatible

  python3 scripts/motox_ebihr_seo_descriptions.py --limit 5
  python3 scripts/motox_ebihr_seo_descriptions.py --handle 1000058 --handle 1018667
  python3 scripts/motox_ebihr_seo_descriptions.py --limit 5 --apply

Cache: cache/motox/ebihr_seo_descriptions.json
Zelfde brontekst + model wordt niet opnieuw bij het model aangeboden.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
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

from modules.motox_shopify_cache import load_motox_env  # noqa: E402
from modules.seo_completeness import SEO_DESC_MAX, SEO_TITLE_MAX, collapse_ws, strip_html  # noqa: E402

_REQUEST_TIMEOUT = (15, 60)
PROMPT_VERSION = "4"
CACHE_PATH = ROOT / "cache" / "motox" / "ebihr_seo_descriptions.json"
OEM_VENDORS = frozenset({"ktm", "husqvarna", "gasgas", "gas gas", "wp"})
_YEAR = re.compile(r"\b(?:19|20)\d{2}\b")
_ABBREV = re.compile(r"(?i)\bT/C\b|%")
_SIZE = re.compile(
    r"(?i)(?:\s*[-–—,/|]\s*)?\b(?:size|maat|taille|pointure)\s*[:=]?\s*"
    r"(?:xxxs|xxs|xs|s|m|l|xl|xxl|xxxl|[2-5]xl|\d{1,2})\b"
    r"|\b(?:xxxs|xxs|xs|xxl|xxxl|[2-5]xl|xl)\b"
)
_QUOTES = "\"'“”‘’"
_DANGLING = frozenset(
    {"en", "of", "met", "van", "voor", "de", "het", "een", "in", "op", "aan", "te", "tot", "als"}
)

_SYSTEM = """Je schrijft het eerste deel van een Nederlandse SEO-meta-omschrijving voor een motorwinkel.
Regels:
- Alleen feiten uit de aangeleverde velden. Verzin geen materialen, jaren of compatibiliteit.
- Geen kleding- of variantenmaat (S, M, L, XL, XXL, maat 42, size). Die wisselt per variant.
- Technische maat die het artikel zelf is, zoals een cilinderdiameter in mm, mag wel.
- Schrijf woorden voluit. Geen afkortingen en geen weglatingsteken. De zin moet in zijn geheel binnen het maximum passen; kap geen woord af.
- Negeer tekst die over een hele collectie gaat en niet over dit ene artikel.
- Geen prijs, geen verzending, geen superlatieven.
- Gebruik "origineel" alleen als de bron dat letterlijk over dit artikel zegt.
- Geen HTML. Zet geen aanhalingstekens om het antwoord.
- Zet zelf geen fitmentzin ("Past op …") in de tekst; die plakken wij erachter.
Antwoord alleen met die volledige tekst.

Vormvoorbeeld (kopieer deze feiten niet, tenzij ze in de input staan):
AIRSAL cilinder Ø76,8 mm voor de Honda CRF250R. Aluminium met Scanimet-coating."""

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
      descriptionHtml
      onlineStoreUrl
      seo { title description }
      media(first: 10) { nodes { id alt } }
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
    descriptionHtml
    onlineStoreUrl
    seo { title description }
    media(first: 10) { nodes { id alt } }
    ymm: metafield(namespace: "global", key: "ymm_summary") { value }
  }
}
"""

_MUT_SEO = """
mutation ($product: ProductUpdateInput!) {
  productUpdate(product: $product) {
    product { id seo { title description } }
    userErrors { field message }
  }
}
"""

_MUT_ALT = """
mutation ($productId: ID!, $media: [UpdateMediaInput!]!) {
  productUpdateMedia(productId: $productId, media: $media) {
    media { alt }
    mediaUserErrors { field message }
  }
}
"""

# priority: eerst actief mét voorraad, daarna de rest.
_BATCH_QUERIES = {
    "priority": [
        "status:active inventory_total:>0",
        "status:active inventory_total:<=0",
        "status:draft",
    ],
    "in-stock": ["status:active inventory_total:>0"],
    "rest": ["status:active inventory_total:<=0", "status:draft"],
}


def _parse_env_file(path: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _openai_settings() -> tuple[str, str, str]:
    motox = _parse_env_file(ROOT / ".env.motox")
    root = _parse_env_file(ROOT / ".env")

    def pick(name: str, default: str = "") -> str:
        return (
            os.environ.get(name) or motox.get(name) or root.get(name) or default
        ).strip()

    key = pick("OPENAI_API_KEY")
    model = pick("OPENAI_MODEL", "gpt-4o-mini")
    base = pick("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
    return key, model, base


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, url: str, token: str, query: str, variables: dict) -> dict:
    for attempt in range(8):
        r = sess.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": query, "variables": variables},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(1.5 + attempt)
            continue
        r.raise_for_status()
        body = r.json()
        if body.get("errors"):
            if "Throttl" in str(body["errors"]):
                time.sleep(2 + attempt)
                continue
            raise RuntimeError(body["errors"])
        return body
    raise RuntimeError("GraphQL failed")


def _is_oem(vendor: str) -> bool:
    return collapse_ws(vendor).lower() in OEM_VENDORS


def strip_sizes(text: str) -> str:
    """Haal variantenmaten uit titel of body. Diameter in mm blijft staan."""
    t = _SIZE.sub(" ", text or "")
    t = re.sub(r"\s*[-–—,/|]\s*$", "", t)
    t = re.sub(r"\(\s*\)", "", t)
    return collapse_ws(t)


def mentions_size(text: str) -> bool:
    return bool(_SIZE.search(text or ""))


def propose_seo_title(title: str, vendor: str) -> str:
    """Merk + productnaam, zonder maat. Hele woorden, max. 70 tekens."""
    name = collapse_ws(_ABBREV.sub(" ", strip_sizes(title)))
    brand = collapse_ws(vendor)
    if brand and name.lower().startswith(brand.lower()):
        base = name
    elif brand and name:
        base = f"{brand} {name}"
    else:
        base = name or brand
    words = base.split()
    while words and len(" ".join(words)) > SEO_TITLE_MAX:
        words.pop()
    while words and words[-1].lower().strip(".,;") in _DANGLING:
        words.pop()
    return " ".join(words)


def fitment_clause(ymm: str) -> str:
    """Volledige 'Past op …'-zin. Bij meerdere merken alleen het eerste, onverkort."""
    text = collapse_ws(ymm)
    if not text:
        return ""
    clause = f"Past op {text}."
    if len(clause) <= 90:
        return clause
    first = collapse_ws(text.split("|")[0])
    if "|" in text:
        shorter = f"Past op {first} en andere merken."
        if len(shorter) <= 90:
            return shorter
    one = f"Past op {first}."
    if len(one) <= 90:
        return one
    return ""


def fit_complete_sentences(text: str, max_len: int) -> str:
    """Houd alleen hele zinnen die samen binnen max_len passen. Kap geen zin af."""
    s = collapse_ws(text).strip(_QUOTES)
    parts = [p.strip() for p in re.split(r"(?<=[.!?])\s+", s) if p.strip()]
    if not parts:
        return ""
    chosen: list[str] = []
    total = 0
    for part in parts:
        sentence = part if part[-1] in ".!?" else f"{part}."
        if _incomplete(sentence):
            break
        extra = len(sentence) if not chosen else len(sentence) + 1
        if total + extra > max_len:
            break
        chosen.append(sentence)
        total += extra
    return " ".join(chosen)


def _incomplete(text: str) -> bool:
    s = collapse_ws(text)
    if not s or "…" in s or s.endswith(("…", "-", "–", ",")):
        return True
    word = s.rstrip(".").split()[-1].lower().strip(".,;")
    return word in _DANGLING


def compose_description(intro: str, clause: str, *, max_len: int = SEO_DESC_MAX) -> str:
    """Plak intro en fitment. Lege string als de zin niet in zijn geheel past."""
    clause = collapse_ws(clause)
    intro = collapse_ws(intro).strip(_QUOTES).rstrip(".")
    if clause:
        bare = clause.rstrip(".")
        intro = collapse_ws(intro.replace(clause, " ").replace(bare, " ")).rstrip(".")
    if clause and len(clause) > max_len:
        return ""
    tail = f". {clause}" if clause else "."
    if not intro:
        return clause if clause and len(clause) <= max_len else ""
    if len(intro) > max_len - len(tail):
        return ""
    text = f"{intro}{tail}"
    if len(text) > max_len or _incomplete(text):
        return ""
    return text


def _clean_model_text(raw: str) -> str:
    text = collapse_ws(raw)
    text = text.strip(_QUOTES)
    for prefix in ("SEO-omschrijving:", "SEO:", "Omschrijving:"):
        if text.lower().startswith(prefix.lower()):
            text = text[len(prefix) :].strip()
    return text.split("\n", 1)[0].strip()


def _invented_years(text: str, source: str) -> bool:
    return bool(set(_YEAR.findall(text)) - set(_YEAR.findall(source)))


def _source_hash(payload: dict) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()


def _load_cache(path: Path) -> dict:
    if not path.exists():
        return {"version": PROMPT_VERSION, "items": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {"version": PROMPT_VERSION, "items": {}}
    data.setdefault("items", {})
    return data


def _save_cache(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _call_model(
    sess: requests.Session,
    *,
    base_url: str,
    api_key: str,
    model: str,
    user: str,
) -> str:
    url = f"{base_url}/chat/completions"
    payload = {
        "model": model,
        "temperature": 0.2,
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
            timeout=_REQUEST_TIMEOUT,
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


def _user_prompt(*, title: str, vendor: str, product_type: str, body: str, budget: int) -> str:
    return (
        f"Maximum tekens voor jouw deel: {budget}\n"
        f"Merk: {vendor or '-'}\n"
        f"Titel: {title or '-'}\n"
        f"Type: {product_type or '-'}\n"
        f"Brontekst:\n{body or '(geen omschrijving)'}"
    )


def _fallback_description(title: str, vendor: str, product_type: str, clause: str) -> str:
    options = []
    bare = strip_sizes(title)
    if bare:
        options.append(bare)
    typed = collapse_ws(f"{vendor} {product_type}")
    if typed and typed not in options:
        options.append(typed)
    if vendor and vendor not in options:
        options.append(vendor)
    for intro in options:
        text = compose_description(intro, clause)
        if text and not mentions_size(text):
            return text
    if clause and len(clause) <= SEO_DESC_MAX and not _incomplete(clause):
        return clause
    return ""


def propose_description(
    *,
    title: str,
    vendor: str,
    product_type: str,
    body_html: str,
    ymm: str,
    model: str,
    call,
) -> tuple[str, str]:
    """Return (description, bron) where bron is ai of template."""
    plain = collapse_ws(_ABBREV.sub(" ", strip_sizes(strip_html(body_html)[:700])))
    shown_title = collapse_ws(_ABBREV.sub(" ", strip_sizes(title)))
    clause = fitment_clause(ymm)
    tail = f". {clause}" if clause else "."
    budget = max(SEO_DESC_MAX - len(tail), 0)
    source_blob = " ".join([title, vendor, product_type, plain, ymm])
    template = _fallback_description(title, vendor, product_type, clause)

    if budget < 24:
        return template, "template"

    user = _user_prompt(
        title=shown_title, vendor=vendor, product_type=product_type, body=plain, budget=budget
    )
    last_err = ""
    for _ in range(3):
        try:
            raw = call(user + (f"\nVorige poging afgekeurd: {last_err}" if last_err else ""))
        except SystemExit:
            raise
        except Exception as exc:
            print(f"AI-aanroep mislukt, template gebruikt: {exc}"[:240], flush=True)
            return template, "template"
        intro = strip_sizes(_clean_model_text(raw))
        if not intro:
            last_err = "lege tekst"
            continue
        if mentions_size(intro):
            last_err = "bevat een variantenmaat; laat maat weg"
            continue
        if _ABBREV.search(intro):
            last_err = "bevat een afkorting; schrijf het voluit"
            continue
        if _incomplete(intro):
            last_err = "zin is afgekapt; schrijf hem voluit"
            continue
        if _invented_years(intro, source_blob):
            last_err = "jaartal dat niet in de bron staat"
            continue
        if len(intro) > budget:
            fitted = fit_complete_sentences(intro, budget)
            if not fitted:
                last_err = (
                    f"te lang ({len(intro)} tekens, maximum is {budget}). "
                    "Schrijf een kortere volledige zin, zonder woorden af te kappen."
                )
                continue
            intro = fitted.rstrip(".")
        text = compose_description(intro, clause)
        if text and len(text) <= SEO_DESC_MAX and not mentions_size(text):
            return text, "ai"
        last_err = "de zin past niet in zijn geheel"
    if last_err:
        print(f"  template ({last_err})", flush=True)
    return template, "template"


def _product_row(p: dict) -> dict | None:
    if not p or _is_oem(p.get("vendor") or ""):
        return None
    if (p.get("status") or "").upper() == "ARCHIVED":
        return None
    ymm = ((p.get("ymm") or {}) or {}).get("value") or ""
    seo = (p.get("seo") or {}) or {}
    media = []
    for node in ((p.get("media") or {}).get("nodes") or []):
        if not isinstance(node, dict) or not node.get("id"):
            continue
        media.append({"id": node["id"], "alt": collapse_ws(node.get("alt") or "")})
    return {
        "id": p.get("id") or "",
        "handle": p.get("handle") or "",
        "title": p.get("title") or "",
        "vendor": p.get("vendor") or "",
        "type": p.get("productType") or "",
        "status": p.get("status") or "",
        "body_html": p.get("descriptionHtml") or "",
        "ymm": ymm,
        "seo_title": collapse_ws(seo.get("title") or ""),
        "seo_description": collapse_ws(seo.get("description") or ""),
        "media": media,
        "url": p.get("onlineStoreUrl") or "",
    }


def _missing_alt(row: dict) -> bool:
    return any(not m["alt"] for m in row.get("media") or [])


def _needs_seo(row: dict, overwrite: bool) -> bool:
    if overwrite:
        return True
    return not row["seo_title"] or not row["seo_description"] or _missing_alt(row)


def iter_products(
    sess,
    url,
    token,
    *,
    handles: list[str],
    limit: int,
    overwrite: bool,
    batch: str,
):
    yielded = 0
    target = None if limit <= 0 else limit
    seen: set[str] = set()

    def _take(row: dict | None):
        nonlocal yielded
        if row is None or row["id"] in seen:
            return False
        if not _needs_seo(row, overwrite):
            return False
        seen.add(row["id"])
        yielded += 1
        return True

    if handles:
        for handle in handles:
            if target is not None and yielded >= target:
                return
            body = _gql(sess, url, token, _QUERY_HANDLE, {"h": handle})
            row = _product_row(((body.get("data") or {}).get("productByHandle")))
            if row is None:
                print(f"Overgeslagen {handle} (niet gevonden, OEM of gearchiveerd)", flush=True)
                continue
            if not _needs_seo(row, overwrite):
                print(f"Overgeslagen {handle} (SEO-titel, omschrijving en alt bestaan al)", flush=True)
                continue
            yielded += 1
            yield row
        return

    for query in _BATCH_QUERIES[batch]:
        cursor = None
        while target is None or yielded < target:
            body = _gql(sess, url, token, _QUERY_PAGE, {"c": cursor, "q": query})
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


def _write_seo(sess, url, token, product_gid: str, title: str, description: str) -> str:
    seo = {}
    if title:
        seo["title"] = title
    if description:
        seo["description"] = description
    body = _gql(
        sess,
        url,
        token,
        _MUT_SEO,
        {"product": {"id": product_gid, "seo": seo}},
    )
    payload = ((body.get("data") or {}).get("productUpdate")) or {}
    errs = payload.get("userErrors") or []
    if errs:
        return str(errs)[:300]
    return ""


def _write_alts(sess, url, token, product_gid: str, files: list[dict]) -> str:
    pending = [{"id": f["id"], "alt": f["alt"]} for f in files if f.get("id") and f.get("alt")]
    for start in range(0, len(pending), 10):
        chunk = pending[start : start + 10]
        body = _gql(
            sess,
            url,
            token,
            _MUT_ALT,
            {"productId": product_gid, "media": chunk},
        )
        payload = ((body.get("data") or {}).get("productUpdateMedia")) or {}
        errs = payload.get("mediaUserErrors") or []
        if errs:
            return str(errs)[:300]
    return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--limit", type=int, default=5, help="Aantal producten (0 = alle lege)")
    ap.add_argument("--handle", action="append", default=[], help="Alleen deze handle (herhaalbaar)")
    ap.add_argument(
        "--batch",
        choices=tuple(_BATCH_QUERIES),
        default="priority",
        help="priority = eerst voorraad, daarna de rest",
    )
    ap.add_argument("--apply", action="store_true", help="Schrijf SEO-titel, omschrijving en alt naar Motox")
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Ook producten die titel, omschrijving en alt al hebben",
    )
    ap.add_argument("--sleep", type=float, default=0.3, help="Pauze tussen AI-aanroepen")
    args = ap.parse_args()

    api_key, model, base_url = _openai_settings()
    if not api_key:
        print(
            "OPENAI_API_KEY ontbreekt. Zet die in .env.motox (niet committen) en draai opnieuw.\n"
            "Voorbeeld: python3 scripts/motox_ebihr_seo_descriptions.py --limit 5",
            file=sys.stderr,
        )
        return 2

    domain, token, api = load_motox_env()
    shop_url = f"https://{domain}/admin/api/{api}/graphql.json"
    sess = _session()
    cache = _load_cache(CACHE_PATH)
    items: dict = cache.setdefault("items", {})

    mode = "APPLY" if args.apply else "dry-run"
    print(
        f"{mode}  shop={domain}  model={model}  batch={args.batch}  limit={args.limit}",
        flush=True,
    )

    def call(user: str) -> str:
        return _call_model(sess, base_url=base_url, api_key=api_key, model=model, user=user)

    rows: list[dict] = []
    written = 0
    for product in iter_products(
        sess,
        shop_url,
        token,
        handles=args.handle,
        limit=args.limit,
        overwrite=args.overwrite,
        batch=args.batch,
    ):
        if args.overwrite or not product["seo_title"]:
            seo_title = propose_seo_title(product["title"], product["vendor"])
        else:
            seo_title = product["seo_title"]

        if product["seo_description"] and not args.overwrite:
            description = product["seo_description"]
            bron = "bestaand"
        else:
            plain = strip_html(product["body_html"])[:700]
            clause = fitment_clause(product["ymm"])
            payload = {
                "v": PROMPT_VERSION,
                "model": model,
                "title": product["title"],
                "vendor": product["vendor"],
                "type": product["type"],
                "body": plain,
                "fitment": clause,
            }
            digest = _source_hash(payload)
            cached = items.get(digest) or {}
            if cached.get("description"):
                description = cached["description"]
                bron = "cache"
            else:
                description, bron = propose_description(
                    title=product["title"],
                    vendor=product["vendor"],
                    product_type=product["type"],
                    body_html=product["body_html"],
                    ymm=product["ymm"],
                    model=model,
                    call=call,
                )
                if bron == "ai":
                    items[digest] = {
                        "description": description,
                        "handle": product["handle"],
                        "model": model,
                    }
                    _save_cache(CACHE_PATH, cache)
                time.sleep(args.sleep)

        alt = seo_title
        if args.overwrite:
            alt_targets = list(product["media"])
        else:
            alt_targets = [m for m in product["media"] if not m["alt"]]

        fout = ""
        if args.apply:
            fout = _write_seo(sess, shop_url, token, product["id"], seo_title, description)
            if not fout and alt_targets:
                fout = _write_alts(
                    sess,
                    shop_url,
                    token,
                    product["id"],
                    [{"id": m["id"], "alt": alt} for m in alt_targets],
                )
            if not fout:
                written += 1
            time.sleep(0.25)

        status = "geschreven" if args.apply and not fout else ("fout" if fout else "voorstel")
        print(
            f"\n[{status}] {product['title']}\n"
            f"  handle {product['handle']}  titel {len(seo_title)} tekens\n"
            f"  {seo_title}\n"
            f"  omschrijving {len(description)} tekens  {bron}\n"
            f"  {description}\n"
            f"  alt {len(alt_targets)} beelden\n"
            f"  {alt}",
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
                "ymm": product["ymm"],
                "huidige_seo_titel": product["seo_title"],
                "voorstel_titel": seo_title,
                "titel_tekens": len(seo_title),
                "huidige_seo": product["seo_description"],
                "voorstel": description,
                "tekens": len(description),
                "alt": alt,
                "alt_beelden": len(alt_targets),
                "bron": bron,
                "fout": fout,
                "url": product["url"],
            }
        )

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"motox_ebihr_seo_descriptions_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "product_id",
        "handle",
        "titel",
        "vendor",
        "type",
        "ymm",
        "huidige_seo_titel",
        "voorstel_titel",
        "titel_tekens",
        "huidige_seo",
        "voorstel",
        "tekens",
        "alt",
        "alt_beelden",
        "bron",
        "fout",
        "url",
    ]
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(rows)

    print(f"Producten: {len(rows)}  geschreven: {written if args.apply else 0}", flush=True)
    print(f"CSV: {out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
