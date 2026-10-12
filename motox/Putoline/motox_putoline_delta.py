#!/usr/bin/env python3
"""Nachtelijke Putoline-delta: Kroon-assortiment naar Motox.

Draait op de VPS (149.210.206.111). De Kroon-API accepteert alleen dat
uitgaande IP-adres. Bestaande producten: prijs, EAN en gewicht als die
afwijken. Nieuwe artikelnummers met een prijs worden aangemaakt en op de
online winkel gezet. Voorraadcodes en verdwenen artikelen worden alleen
gelogd.

De vijf oude kassa-artikelen die niet in de Kroon-lijst staan blijven met rust:
70630, 74016, 74026, 74036, 74089.
"""

from __future__ import annotations

import argparse
import fcntl
import html
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

try:
    import requests
except ImportError:
    requests = None
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from putoline_listing import listing_for
from zoneinfo import ZoneInfo

PROTECTED_SKUS = {"70630", "74016", "74026", "74036", "74089"}
ONLINE_STORE = "gid://shopify/Publication/41879699541"
API_HOST = "api.kroon-oil.com"


def load_env(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def env_path(name: str, candidates: list[Path]) -> Path | None:
    override = os.environ.get(name, "").strip()
    if override:
        return Path(override)
    for path in candidates:
        if path.is_file():
            return path
    return None


def money(value) -> Decimal | None:
    text = str(value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    if amount <= 0:
        return None
    return amount.quantize(Decimal("0.01"))


def kilos(value) -> Decimal | None:
    if isinstance(value, float):
        text = f"{value:.4f}"
    else:
        text = str(value or "").strip().replace(",", ".")
    if not text:
        return None
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    if amount <= 0:
        return None
    return amount.quantize(Decimal("0.001"))


def rows_from_payload(payload) -> list[dict]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        for key in ("items", "data", "assortment", "results", "rows"):
            value = payload.get(key)
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        if isinstance(payload.get("response"), list):
            return [row for row in payload["response"] if isinstance(row, dict)]
        if payload.get("code") or payload.get("id"):
            return [payload]
    raise RuntimeError("assortiment-antwoord heeft geen rijen")


def kroon_php(*args: str) -> subprocess.CompletedProcess:
    script = Path(__file__).with_name("motox_putoline_fetch.php")
    return subprocess.run(["php", str(script), *args], capture_output=True)


def fetch_assortment() -> list[dict]:
    run = kroon_php("assortment")
    raw = run.stdout
    if run.returncode != 0:
        detail = (run.stderr or raw or b"").decode("utf-8", "replace")[:300]
        raise RuntimeError(f"assortiment ophalen mislukt: {detail}")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"assortiment is geen JSON: {raw[:180]!r}") from exc
    rows = rows_from_payload(payload)
    if len(rows) < 50:
        raise RuntimeError(f"assortiment te klein ({len(rows)} rijen), afgebroken")
    return rows


def image_url(row: dict) -> str:
    for key in ("image_large", "image_medium", "image_small"):
        text = str(row.get(key) or "").strip()
        if not text:
            continue
        if text.startswith("http"):
            return text
        return "https://api.kroon-oil.com" + (text if text.startswith("/") else f"/{text}")
    return ""


def download_image(url: str, dest: Path) -> None:
    if API_HOST in url:
        run = kroon_php("download", url, str(dest))
        if run.returncode != 0:
            detail = (run.stderr or b"").decode("utf-8", "replace")[:200]
            raise RuntimeError(f"foto download: {detail}")
    else:
        run = subprocess.run(
            ["curl", "-fsSL", "--max-time", "120", "-o", str(dest), url],
            capture_output=True,
        )
        if run.returncode != 0:
            raise RuntimeError(f"foto download {run.returncode}")
    if dest.stat().st_size < 500:
        raise RuntimeError("foto te klein")


class Shopify:
    def __init__(self) -> None:
        shop = os.environ.get("SHOPIFY_SHOP_DOMAIN") or os.environ.get("SHOPIFY_MOTOX_SHOP_DOMAIN")
        token = os.environ.get("SHOPIFY_ACCESS_TOKEN") or os.environ.get("SHOPIFY_MOTOX_ACCESS_TOKEN")
        version = os.environ.get("SHOPIFY_ADMIN_API_VERSION") or os.environ.get("SHOPIFY_MOTOX_ADMIN_API_VERSION") or "2024-10"
        if not shop or not token:
            raise RuntimeError("Shopify-gegevens ontbreken")
        self.url = f"https://{shop}/admin/api/{version}/graphql.json"
        self.token = token
        self.session = requests.Session() if requests else None
        if self.session:
            self.session.trust_env = False

    def gql(self, query: str, variables: dict | None = None) -> dict:
        payload = {"query": query, "variables": variables or {}}
        last_error = ""
        for attempt in range(6):
            try:
                if self.session:
                    response = self.session.post(
                        self.url,
                        json=payload,
                        headers={"X-Shopify-Access-Token": self.token},
                        timeout=60,
                    )
                    if response.status_code in (429, 500, 502, 503, 504):
                        time.sleep(2 + attempt * 2)
                        last_error = f"HTTP {response.status_code}"
                        continue
                    response.raise_for_status()
                    body = response.json()
                else:
                    request = urllib.request.Request(
                        self.url,
                        data=json.dumps(payload).encode(),
                        headers={
                            "Content-Type": "application/json",
                            "X-Shopify-Access-Token": self.token,
                        },
                    )
                    with urllib.request.urlopen(request, timeout=60) as response:
                        body = json.loads(response.read().decode())
            except urllib.error.HTTPError as exc:
                last_error = exc.read().decode("utf-8", "replace")[:300]
                if exc.code in (429, 500, 502, 503, 504):
                    time.sleep(2 + attempt * 2)
                    continue
                raise RuntimeError(f"Shopify HTTP {exc.code}: {last_error}") from exc
            errors = body.get("errors") or []
            throttled = any("THROTTLED" in json.dumps(err) for err in errors)
            if throttled:
                time.sleep(2 + attempt * 2)
                last_error = "THROTTLED"
                continue
            return body
        raise RuntimeError(f"Shopify bleef afwijzen: {last_error}")


def putoline_variants(shop: Shopify) -> dict[str, dict]:
    found: dict[str, dict] = {}
    cursor = None
    query = """
    query ($cursor: String) {
      products(first: 50, after: $cursor, query: "vendor:Putoline") {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          title
          status
          productType
          descriptionHtml
          category { id fullName }
          seo { title description }
          media(first: 5) { nodes { id alt mediaContentType } }
          variants(first: 20) {
            nodes {
              id
              sku
              price
              barcode
              inventoryItem { id measurement { weight { value } } }
            }
          }
        }
      }
    }
    """
    while True:
        body = shop.gql(query, {"cursor": cursor})
        if body.get("errors"):
            raise RuntimeError(str(body.get("errors"))[:300])
        products = ((body.get("data") or {}).get("products")) or {}
        for product in products.get("nodes") or []:
            for variant in ((product.get("variants") or {}).get("nodes")) or []:
                sku = (variant.get("sku") or "").strip()
                if not sku or sku in found:
                    continue
                weight = (((variant.get("inventoryItem") or {}).get("measurement") or {}).get("weight") or {}).get("value")
                found[sku] = {
                    "product_id": product["id"],
                    "title": product.get("title") or "",
                    "status": product.get("status"),
                    "product_type": product.get("productType") or "",
                    "body": product.get("descriptionHtml") or "",
                    "category_id": ((product.get("category") or {}).get("id")) or "",
                    "category_name": ((product.get("category") or {}).get("fullName")) or "",
                    "seo_title": ((product.get("seo") or {}).get("title")) or "",
                    "seo_description": ((product.get("seo") or {}).get("description")) or "",
                    "images": [
                        {"id": media.get("id"), "alt": media.get("alt") or "", "type": media.get("mediaContentType")}
                        for media in ((product.get("media") or {}).get("nodes") or [])
                    ],
                    "variant_id": variant["id"],
                    "price": variant.get("price"),
                    "barcode": variant.get("barcode") or "",
                    "inventory_item_id": (variant.get("inventoryItem") or {}).get("id"),
                    "weight": weight,
                }
        page = products.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
    return found


def variant_update(shop: Shopify, product_id: str, variant_id: str, price: str | None, barcode: str | None) -> str:
    variant: dict = {"id": variant_id}
    if price is not None:
        variant["price"] = price
    if barcode is not None:
        variant["barcode"] = barcode
    body = shop.gql(
        """
        mutation ($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            userErrors { field message }
          }
        }
        """,
        {"productId": product_id, "variants": [variant]},
    )
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    errors = (((body.get("data") or {}).get("productVariantsBulkUpdate")) or {}).get("userErrors") or []
    return str(errors)[:300] if errors else ""


def weight_update(shop: Shopify, inventory_item_id: str, value: str) -> str:
    body = shop.gql(
        """
        mutation ($id: ID!, $input: InventoryItemInput!) {
          inventoryItemUpdate(id: $id, input: $input) {
            userErrors { field message }
          }
        }
        """,
        {"id": inventory_item_id, "input": {"measurement": {"weight": {"value": float(value), "unit": "KILOGRAMS"}}}},
    )
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    errors = (((body.get("data") or {}).get("inventoryItemUpdate")) or {}).get("userErrors") or []
    return str(errors)[:300] if errors else ""


def image_type(path: Path) -> tuple[str, str]:
    header = path.read_bytes()[:8]
    if header.startswith(b"\x89PNG"):
        return "image/png", "png"
    if header.startswith(b"\xff\xd8"):
        return "image/jpeg", "jpg"
    if header.startswith(b"RIFF"):
        return "image/webp", "webp"
    return "image/jpeg", "jpg"


def stage_image(shop: Shopify, path: Path) -> str:
    data = path.read_bytes()
    mime, _ext = image_type(path)
    body = shop.gql(
        """
        mutation ($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets { url resourceUrl parameters { name value } }
            userErrors { field message }
          }
        }
        """,
        {"input": [{
            "filename": path.name,
            "mimeType": mime,
            "httpMethod": "POST",
            "resource": "PRODUCT_IMAGE",
            "fileSize": str(len(data)),
        }]},
    )
    if body.get("errors"):
        raise RuntimeError(str(body.get("errors"))[:300])
    payload = ((body.get("data") or {}).get("stagedUploadsCreate")) or {}
    if payload.get("userErrors"):
        raise RuntimeError(str(payload.get("userErrors"))[:300])
    target = (payload.get("stagedTargets") or [None])[0]
    if not target:
        raise RuntimeError("geen staged target")
    cmd = ["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}", "-X", "POST", target["url"]]
    for item in target.get("parameters") or []:
        cmd += ["-F", f"{item['name']}={item['value']}"]
    cmd += ["-F", f"file=@{path};type={mime}"]
    run = subprocess.run(cmd, capture_output=True)
    code = (run.stdout or b"").decode().strip()
    if run.returncode != 0 or code not in ("200", "201", "204"):
        raise RuntimeError(f"foto-upload {code or run.returncode}")
    return target["resourceUrl"]


def create_product(shop: Shopify, row: dict, price: Decimal, barcode: str, weight: Decimal | None, staged_image: str) -> str:
    sku = str(row.get("code") or "").strip()
    title = str(row.get("name") or sku).strip()
    raw_description = str(row.get("desc_short") or "").strip()
    description = html.escape(raw_description)
    listing = listing_for(title=title, product_type="", body_html=raw_description)
    variant: dict = {
        "optionValues": [{"optionName": "Title", "name": "Default Title"}],
        "sku": sku,
        "price": f"{price:.2f}",
        "barcode": barcode,
        "inventoryPolicy": "DENY",
    }
    if weight is not None:
        variant["inventoryItem"] = {"measurement": {"weight": {"value": float(weight), "unit": "KILOGRAMS"}}}
    product_input = {
        "title": title,
        "descriptionHtml": f"<p>{description}</p>" if description else "",
        "vendor": "Putoline",
        "productType": listing.product_type,
        "category": listing.category_id,
        "seo": {"title": listing.seo_title, "description": listing.seo_description},
        "status": "ACTIVE",
        "tags": ["Putoline"],
        "productOptions": [{"name": "Title", "values": [{"name": "Default Title"}]}],
        "variants": [variant],
    }
    if staged_image:
        product_input["files"] = [{"originalSource": staged_image, "contentType": "IMAGE", "alt": listing.seo_title[:250]}]
    body = shop.gql(
        """
        mutation ($input: ProductSetInput!, $synchronous: Boolean!) {
          productSet(input: $input, synchronous: $synchronous) {
            product { id }
            userErrors { field message }
          }
        }
        """,
        {"input": product_input, "synchronous": True},
    )
    if body.get("errors"):
        return str(body.get("errors"))[:300]
    payload = ((body.get("data") or {}).get("productSet")) or {}
    if payload.get("userErrors"):
        return str(payload.get("userErrors"))[:300]
    product_id = ((payload.get("product") or {}).get("id")) or ""
    if not product_id:
        return "nieuw product zonder id"
    published = shop.gql(
        """
        mutation ($id: ID!, $input: [PublicationInput!]!) {
          publishablePublish(id: $id, input: $input) {
            userErrors { field message }
          }
        }
        """,
        {"id": product_id, "input": [{"publicationId": ONLINE_STORE}]},
    )
    if published.get("errors"):
        return str(published.get("errors"))[:300]
    errors = (((published.get("data") or {}).get("publishablePublish")) or {}).get("userErrors") or []
    return str(errors)[:300] if errors else ""


def snapshot_map(rows: list[dict]) -> dict[str, dict]:
    mapped = {}
    for row in rows:
        sku = str(row.get("code") or "").strip()
        if not sku:
            continue
        mapped[sku] = {
            "price": str(money(row.get("price_per_packaging")) or ""),
            "ean": str(row.get("eancode") or "").strip(),
            "weight": str(kilos(row.get("packaging_weight")) or ""),
            "stock_id": str(row.get("stock_id") if row.get("stock_id") is not None else ""),
        }
    return mapped


def listing_plan(have: dict, *, overwrite: bool) -> tuple[dict, list[dict], object]:
    proposal = listing_for(
        title=have.get("title") or "",
        product_type=have.get("product_type") or "",
        body_html=have.get("body") or "",
    )
    product: dict = {}
    if have.get("category_id") != proposal.category_id:
        product["category"] = proposal.category_id
    if (have.get("product_type") or "") != proposal.product_type:
        product["productType"] = proposal.product_type
    seo: dict = {}
    if overwrite or not (have.get("seo_title") or "").strip():
        if (have.get("seo_title") or "") != proposal.seo_title:
            seo["title"] = proposal.seo_title
    if overwrite or not (have.get("seo_description") or "").strip():
        if (have.get("seo_description") or "") != proposal.seo_description:
            seo["description"] = proposal.seo_description
    if seo:
        product["seo"] = seo
    alts = []
    for image in have.get("images") or []:
        if image.get("type") != "IMAGE" or not image.get("id"):
            continue
        alt = image.get("alt") or ""
        if alt == proposal.seo_title:
            continue
        if overwrite or not alt or alt == (have.get("title") or ""):
            alts.append({"id": image["id"], "alt": proposal.seo_title[:250]})
    return product, alts, proposal


def write_listing(shop: Shopify, product_id: str, product: dict, alts: list[dict]) -> str:
    if product:
        body = shop.gql(
            """
            mutation ($product: ProductUpdateInput!) {
              productUpdate(product: $product) {
                userErrors { field message }
              }
            }
            """,
            {"product": {"id": product_id, **product}},
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        errors = (((body.get("data") or {}).get("productUpdate")) or {}).get("userErrors") or []
        if errors:
            return str(errors)[:300]
    if alts:
        body = shop.gql(
            """
            mutation ($productId: ID!, $media: [UpdateMediaInput!]!) {
              productUpdateMedia(productId: $productId, media: $media) {
                mediaUserErrors { field message }
              }
            }
            """,
            {"productId": product_id, "media": alts},
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        errors = (((body.get("data") or {}).get("productUpdateMedia")) or {}).get("mediaUserErrors") or []
        if errors:
            return str(errors)[:300]
    return ""


def sync_listings(shop: Shopify, existing: dict[str, dict], *, overwrite: bool, apply: bool) -> tuple[int, int]:
    seen: set[str] = set()
    changed = failed = 0
    shown = 0
    for sku, have in sorted(existing.items()):
        if sku in PROTECTED_SKUS or have["product_id"] in seen:
            continue
        seen.add(have["product_id"])
        product, alts, proposal = listing_plan(have, overwrite=overwrite)
        if not product and not alts:
            continue
        changed += 1
        if shown < 8:
            shown += 1
            bits = []
            if "category" in product:
                bits.append(f"categorie {have.get('category_name') or 'leeg'}")
            if "productType" in product:
                bits.append(f"type {product['productType']}")
            if "seo" in product:
                bits.append(f"seo {proposal.seo_title}")
            print(f"{'schrijf' if apply else 'zou'} {sku} {have.get('title')} | {', '.join(bits)}", flush=True)
            if "seo" in product:
                print(f"  {proposal.seo_description}", flush=True)
        if not apply:
            continue
        err = write_listing(shop, have["product_id"], product, alts)
        if err:
            print(f"FAIL listing {sku} {err}", flush=True)
            failed += 1
            changed -= 1
        time.sleep(0.15)
    print(f"listing {'geschreven' if apply else 'voorstel'} {changed} mislukt {failed}", flush=True)
    return changed, failed


def main() -> int:
    parser = argparse.ArgumentParser(description="Verwerk de Putoline-delta naar Motox")
    parser.add_argument("--apply", action="store_true", help="schrijf naar Shopify")
    parser.add_argument("--listing", action="store_true", help="zet categorie en SEO op bestaande producten")
    args = parser.parse_args()

    # Cron staat op UTC. Beide mogelijke uren starten het script; alleen 02:15
    # in Amsterdam gaat door, zomer en winter.
    if os.environ.get("MOTOX_PUTOLINE_SCHEDULED") == "1":
        now = datetime.now(ZoneInfo("Europe/Amsterdam"))
        if not (now.hour == 2 and now.minute <= 45):
            print(f"buiten 02:15 Amsterdam ({now:%H:%M}), overgeslagen")
            return 0

    root = Path(__file__).resolve().parent
    state_dir = Path(os.environ.get("PUTOLINE_STATE_DIR") or (root / "state"))
    state_dir.mkdir(parents=True, exist_ok=True)
    lock_file = open(state_dir / "motox-putoline.lock", "a")
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("motox-putoline draait al, deze ronde slaat over")
        return 0

    kroon_file = env_path("KROON_ENV", [Path("/home/motoxshop/.env.kroon"), root.parents[1] / ".env.kroon"])
    motox_file = env_path("MOTOX_ENV", [Path("/home/motoxshop/.env.motox"), root.parents[1] / ".env.motox"])
    if kroon_file:
        load_env(kroon_file)
    if motox_file:
        load_env(motox_file)

    if args.listing:
        shop = Shopify()
        print("Motox-varianten ophalen", flush=True)
        existing = putoline_variants(shop)
        print(f"Motox vendor Putoline: {len(existing)}", flush=True)
        _changed, failed = sync_listings(shop, existing, overwrite=True, apply=args.apply)
        return 1 if failed else 0

    print("Assortiment ophalen", flush=True)
    rows = fetch_assortment()
    current = snapshot_map(rows)
    by_sku = {str(row.get("code") or "").strip(): row for row in rows if str(row.get("code") or "").strip()}
    print(f"Assortiment: {len(current)} artikelnummers", flush=True)

    previous_path = state_dir / "assortment_last.json"
    previous = json.loads(previous_path.read_text()) if previous_path.is_file() else {}
    removed = sorted(set(previous) - set(current) - PROTECTED_SKUS)
    stock_changed = sorted(
        sku for sku, item in current.items()
        if sku in previous and item["stock_id"] != previous[sku].get("stock_id")
    )

    shop = Shopify()
    print("Motox-varianten ophalen", flush=True)
    existing = putoline_variants(shop)
    print(f"Motox vendor Putoline: {len(existing)}", flush=True)

    price_n = ean_n = weight_n = created_n = failed = 0
    for sku, item in sorted(current.items()):
        if sku in PROTECTED_SKUS:
            continue
        row = by_sku[sku]
        price = money(row.get("price_per_packaging"))
        ean = str(row.get("eancode") or "").strip()
        weight = kilos(row.get("packaging_weight"))
        have = existing.get(sku)
        if not have:
            if price is None:
                print(f"nieuw zonder prijs, overgeslagen {sku}", flush=True)
                continue
            print(f"{'nieuw' if args.apply else 'zou nieuw'} {sku} {row.get('name')}", flush=True)
            if not args.apply:
                created_n += 1
                continue
            try:
                staged = ""
                photo = image_url(row)
                if photo:
                    with tempfile.TemporaryDirectory() as tmp:
                        dest = Path(tmp) / f"{sku}.jpg"
                        download_image(photo, dest)
                        staged = stage_image(shop, dest)
                err = create_product(shop, row, price, ean, weight, staged)
            except Exception as exc:
                err = str(exc)[:300]
            if err:
                print(f"FAIL nieuw {sku} {err}", flush=True)
                failed += 1
            else:
                created_n += 1
            continue

        new_price = f"{price:.2f}" if price is not None and price != money(have["price"]) else None
        new_ean = ean if ean and ean != (have["barcode"] or "") else None
        current_weight = kilos(have["weight"])
        new_weight = f"{weight:.3f}" if weight is not None and weight != current_weight and have.get("inventory_item_id") else None
        if not any((new_price, new_ean, new_weight)):
            continue
        label = []
        if new_price:
            label.append(f"prijs {have['price']} -> {new_price}")
            price_n += 1
        if new_ean:
            label.append("ean")
            ean_n += 1
        if new_weight:
            label.append(f"gewicht {have['weight']} -> {new_weight}")
            weight_n += 1
        print(f"{'bijgewerkt' if args.apply else 'zou bijwerken'} {sku} {', '.join(label)}", flush=True)
        if not args.apply:
            continue
        if new_price or new_ean:
            err = variant_update(shop, have["product_id"], have["variant_id"], new_price, new_ean)
            if err:
                print(f"FAIL {sku} variant {err}", flush=True)
                failed += 1
                continue
        if new_weight:
            err = weight_update(shop, have["inventory_item_id"], new_weight)
            if err:
                print(f"FAIL {sku} gewicht {err}", flush=True)
                failed += 1

    listing_changed, listing_failed = sync_listings(shop, existing, overwrite=False, apply=args.apply)
    failed += listing_failed

    if removed:
        print("Niet meer in het assortiment, niet aangepast: " + ", ".join(removed), flush=True)
    if stock_changed:
        print(f"Voorraadindicatie veranderd bij {len(stock_changed)} artikelen, niet als aantal weggeschreven", flush=True)

    summary = (
        f"delta prijs={price_n} ean={ean_n} gewicht={weight_n} "
        f"nieuw={created_n} listing={listing_changed} weg={len(removed)} "
        f"voorraadindicatie={len(stock_changed)} mislukt={failed}"
    )
    print(summary, flush=True)
    if args.apply and failed == 0:
        previous_path.write_text(json.dumps(current, indent=2, sort_keys=True))
        (state_dir / "last_run.json").write_text(json.dumps({
            "summary": summary,
            "removed": removed,
            "stock_changed": stock_changed,
        }, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
