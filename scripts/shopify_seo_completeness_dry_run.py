#!/usr/bin/env python3
"""
Dry-run: SEO-titel, meta-description, image-alt en optioneel GTIN.

Titel en alt zijn vaste regels. De meta-omschrijving komt van hetzelfde model
als de Motox e-bihr-job, zonder "Past op". HOMNN in de producttekst blijft staan.

Vult alleen voorstellen voor lege velden (fill-if-empty). Geen writes naar Shopify.

  python3 scripts/shopify_seo_completeness_dry_run.py
  python3 scripts/shopify_seo_completeness_dry_run.py --limit 15 --diverse
  python3 scripts/shopify_seo_completeness_dry_run.py --shop ktm --limit 15

Output: output/shopify_seo_completeness_dry_run_{shop}_{stamp}.csv
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import sys
import time
from collections import Counter
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

from modules.seo_completeness import build_proposal, strip_homnn_plain, strip_html  # noqa: E402

_motox_seo_path = ROOT / "scripts" / "motox_ebihr_seo_descriptions.py"
_spec = importlib.util.spec_from_file_location("motox_ebihr_seo_descriptions", _motox_seo_path)
if _spec is None or _spec.loader is None:
    raise SystemExit(f"Kan {_motox_seo_path} niet laden")
motox_seo = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(motox_seo)

_REQUEST_TIMEOUT = (15, 120)

QUERY = """
query ($c: String) {
  products(first: 50, after: $c, query: "status:active") {
    pageInfo { hasNextPage endCursor }
    nodes {
      id
      handle
      title
      vendor
      productType
      descriptionHtml
      onlineStoreUrl
      seo { title description }
      category { fullName }
      ymm: metafield(namespace: "global", key: "ymm_summary") { value }
      media(first: 8) {
        nodes {
          ... on MediaImage { image { altText } }
        }
      }
      variants(first: 8) {
        nodes { sku barcode }
      }
    }
  }
}
"""

_BUCKET_ORDER = [
    "partstream",
    "seat",
    "sticker",
    "graphic",
    "carbon",
    "jacket",
    "helmet",
    "glove",
    "boot",
    "bicycle",
    "bag",
    "luggage",
    "electrical",
    "suspension",
    "engine",
    "brake",
    "chain",
]


def _load_dotenv(path: Path) -> dict[str, str]:
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


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _gql(sess: requests.Session, shop: str, token: str, api: str, variables: dict) -> dict:
    url = f"https://{shop}/admin/api/{api}/graphql.json"
    for attempt in range(8):
        r = sess.post(
            url,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            json={"query": QUERY, "variables": variables},
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        if r.status_code == 429:
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


def _bucket(p: dict) -> str:
    blob = " ".join(
        [
            str(p.get("productType") or ""),
            str(((p.get("category") or {}) or {}).get("fullName") or ""),
            str(p.get("title") or ""),
        ]
    ).lower()
    for key in _BUCKET_ORDER:
        if key in blob:
            return key
    return "other"


def _load_barcode_index() -> dict[str, str]:
    try:
        from modules.pricing_loader import load_price_index

        _price, barcodes, _status = load_price_index()
        return {str(k).strip().upper(): str(v).strip() for k, v in barcodes.items() if v}
    except FileNotFoundError:
        print("0150 prijsbestand niet gevonden; GTIN-voorstellen blijven leeg.", flush=True)
        return {}
    except Exception as exc:
        print(f"GTIN-index overgeslagen: {exc}", flush=True)
        return {}


def _row_from_product(p: dict, barcode_index: dict[str, str]) -> dict:
    variants = ((p.get("variants") or {}).get("nodes") or [])
    sku = ""
    barcode = ""
    for v in variants:
        sku = sku or str(v.get("sku") or "").strip()
        barcode = barcode or str(v.get("barcode") or "").strip()
    src_bc = barcode_index.get(sku.upper(), "") if sku else ""
    alts = []
    for m in (p.get("media") or {}).get("nodes") or []:
        img = m.get("image") or {}
        alts.append(str(img.get("altText") or ""))
    ymm = ((p.get("ymm") or {}) or {}).get("value") or ""
    seo = p.get("seo") or {}
    prop = build_proposal(
        title=p.get("title") or "",
        vendor=p.get("vendor") or "KTM",
        body_html=p.get("descriptionHtml") or "",
        ymm_summary=ymm,
        current_seo_title=seo.get("title") or "",
        current_seo_description=seo.get("description") or "",
        current_alts=alts,
        current_barcode=barcode,
        source_barcode=src_bc,
    )
    changes = []
    if prop.change_seo_title:
        changes.append("seo_title")
    if prop.change_seo_description:
        changes.append("seo_desc")
    if prop.change_image_alt:
        changes.append("alt")
    if prop.change_barcode:
        changes.append("gtin")
    body_plain = strip_homnn_plain(strip_html(p.get("descriptionHtml") or ""))
    current_desc = seo.get("description") or ""
    return {
        "product_id": (p.get("id") or "").split("/")[-1],
        "handle": p.get("handle") or "",
        "url": p.get("onlineStoreUrl") or "",
        "title": p.get("title") or "",
        "vendor": p.get("vendor") or "",
        "product_type": p.get("productType") or "",
        "category": ((p.get("category") or {}) or {}).get("fullName") or "",
        "sku": sku,
        "ymm_summary": ymm,
        "huidige_seo_title": seo.get("title") or "",
        "voorgestelde_seo_title": prop.seo_title,
        "huidige_seo_desc": current_desc,
        "voorgestelde_seo_desc": "" if prop.change_seo_description else current_desc,
        "omschrijving_bron": "leeg" if prop.change_seo_description else "bestaand",
        "huidige_alt": alts[0] if alts else "",
        "voorgestelde_alt": prop.image_alt,
        "aantal_media_zonder_alt": sum(1 for a in alts if not (a or "").strip()),
        "huidige_barcode": barcode,
        "bron_gtin_0150": src_bc,
        "voorgestelde_barcode": prop.proposed_barcode if prop.change_barcode else "",
        "body_nu": body_plain[:400],
        "wijzigingen": ",".join(changes),
        "bucket": _bucket(p),
        "_body_for_model": body_plain[:700],
    }


def _fill_descriptions(
    rows: list[dict],
    sess: requests.Session,
    *,
    api_key: str,
    model: str,
    base_url: str,
    cache_path: Path,
    sleep_s: float,
) -> None:
    """Zelfde modelpad als Motox. Alleen lege meta-omschrijvingen. Geen Past op."""
    cache = motox_seo._load_cache(cache_path)
    items: dict = cache.setdefault("items", {})

    def call(user: str) -> str:
        return motox_seo._call_model(
            sess, base_url=base_url, api_key=api_key, model=model, user=user
        )

    for row in rows:
        if "seo_desc" not in (row.get("wijzigingen") or "").split(","):
            continue
        plain = row.get("_body_for_model") or ""
        payload = {
            "v": motox_seo.PROMPT_VERSION,
            "model": model,
            "title": row["title"],
            "vendor": row["vendor"],
            "type": row["product_type"],
            "body": plain,
        }
        digest = motox_seo._source_hash(payload)
        cached = items.get(digest) or {}
        if cached.get("description"):
            description = cached["description"]
            bron = "cache"
        else:
            description, bron = motox_seo.propose_description(
                title=row["title"],
                vendor=row["vendor"] or "KTM",
                product_type=row["product_type"],
                body_html=plain,
                ymm="",
                model=model,
                call=call,
            )
            if bron == "ai" and description:
                items[digest] = {
                    "description": description,
                    "handle": row["handle"],
                    "model": model,
                }
                motox_seo._save_cache(cache_path, cache)
            time.sleep(sleep_s)
        row["voorgestelde_seo_desc"] = description
        row["omschrijving_bron"] = bron
        print(
            f"  {row['handle']}  {bron}  {len(description)} tekens\n  {description}",
            flush=True,
        )


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--shop", choices=("ktm", "motox"), default="ktm")
    ap.add_argument("--limit", type=int, default=15, help="Aantal producten in de sample")
    ap.add_argument(
        "--diverse",
        action="store_true",
        default=True,
        help="Spreid over productsoorten (default aan)",
    )
    ap.add_argument("--no-diverse", action="store_true", help="Eerste N actieve producten")
    ap.add_argument("--scan-max", type=int, default=800, help="Max te scannen voor diverse sample")
    args = ap.parse_args()
    diverse = not args.no_diverse

    env_path = ROOT / (".env.motox" if args.shop == "motox" else ".env")
    env = _load_dotenv(env_path)
    shop = env.get("SHOPIFY_SHOP_DOMAIN") or os.environ.get("SHOPIFY_SHOP_DOMAIN", "")
    token = env.get("SHOPIFY_ACCESS_TOKEN") or os.environ.get("SHOPIFY_ACCESS_TOKEN", "")
    api = env.get("SHOPIFY_ADMIN_API_VERSION") or os.environ.get(
        "SHOPIFY_ADMIN_API_VERSION", "2024-10"
    )
    if not shop or not token:
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreekt.", file=sys.stderr)
        return 1

    barcode_index = _load_barcode_index()
    sess = _session()
    print(f"Shop: {shop}  limit={args.limit} diverse={diverse}", flush=True)

    picked: list[dict] = []
    seen_buckets: set[str] = set()
    extras: list[dict] = []
    cursor = None
    scanned = 0

    while scanned < args.scan_max and len(picked) < args.limit:
        body = _gql(sess, shop, token, api, {"c": cursor})
        conn = body["data"]["products"]
        for p in conn["nodes"]:
            scanned += 1
            row = _row_from_product(p, barcode_index)
            if not diverse:
                picked.append(row)
                if len(picked) >= args.limit:
                    break
                continue
            b = row["bucket"]
            if b not in seen_buckets and b != "other":
                seen_buckets.add(b)
                picked.append(row)
            else:
                extras.append(row)
            if len(picked) >= args.limit:
                break
        if not diverse and len(picked) >= args.limit:
            break
        if not conn["pageInfo"]["hasNextPage"]:
            break
        cursor = conn["pageInfo"]["endCursor"]
        time.sleep(0.25)

    if len(picked) < args.limit:
        seen_titles = {r["title"] for r in picked}
        for row in extras:
            if len(picked) >= args.limit:
                break
            if row["title"] in seen_titles:
                continue
            seen_titles.add(row["title"])
            picked.append(row)
    if len(picked) < args.limit:
        for row in extras:
            if len(picked) >= args.limit:
                break
            if row in picked:
                continue
            picked.append(row)

    api_key, model, base_url = motox_seo._openai_settings()
    needs_model = any("seo_desc" in (row.get("wijzigingen") or "").split(",") for row in picked)
    if needs_model and not api_key:
        print(
            "OPENAI_API_KEY ontbreekt. Zet die in .env of .env.motox en draai opnieuw.",
            file=sys.stderr,
        )
        return 2
    if needs_model:
        cache_path = ROOT / "cache" / args.shop / "seo_completeness_descriptions.json"
        print(f"Meta-omschrijvingen via {model}", flush=True)
        _fill_descriptions(
            picked,
            sess,
            api_key=api_key,
            model=model,
            base_url=base_url,
            cache_path=cache_path,
            sleep_s=0.3,
        )
    for row in picked:
        row.pop("_body_for_model", None)

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "output" / f"shopify_seo_completeness_dry_run_{args.shop}_{stamp}.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    fields = list(picked[0].keys()) if picked else ["handle"]
    with out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, delimiter=";", quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        w.writerows(picked)

    change_counts = Counter()
    for row in picked:
        for part in (row.get("wijzigingen") or "").split(","):
            if part:
                change_counts[part] += 1

    print(f"Gescand: {scanned}  sample: {len(picked)}", flush=True)
    print(f"Wijzigingen in sample: {dict(change_counts)}", flush=True)
    print(f"CSV: {out}", flush=True)

    preview = ROOT / "output" / "shopify_seo_completeness_dry_run_preview.json"
    preview.write_text(json.dumps(picked, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"JSON-preview: {preview}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
