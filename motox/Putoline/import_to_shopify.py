#!/usr/bin/env python3
"""Importeer Putoline-verpakkingen naar Motox.

Bestaand artikelnummer wordt bijgewerkt en houdt zijn status.
Een nieuw artikelnummer wordt een concept.

  python motox/Putoline/import_to_shopify.py /pad/naar/cache
"""

from __future__ import annotations

import csv
import html
import json
import re
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from modules.ebihr.motox_publish import MotoxAdmin  # noqa: E402

CSV_PATH = ROOT / "motox" / "Putoline" / "overview" / "putoline_producten.csv"
STATE_PATH = Path("/tmp/putoline_shopify_import_state.json")
TYPICAL_START = "Dichtheid bij 15 °C, kg/l"


def load_rows() -> tuple[list[str], list[dict[str, str]]]:
    with CSV_PATH.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        fields = list(reader.fieldnames or [])
        return fields, list(reader)


def slug_title(row: dict[str, str]) -> str:
    name = (row.get("product_naam") or "").strip()
    pack = (row.get("verpakking") or "").strip()
    full = (row.get("verpakking_naam") or "").strip()
    if name and pack:
        return f"{name} {pack}"
    return full or name or row["artikelnummer"]


def youtube_url(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return ""
    match = re.search(r"(?:youtu\.be/|v=)([\w-]{6,})", raw)
    if not match:
        return raw
    return f"https://www.youtube.com/watch?v={match.group(1)}"


def split_description(text: str) -> tuple[str, list[str]]:
    text = (text or "").replace("\r\n", "\n").strip()
    marker = "Aanwijzingen voor gebruik:"
    if marker not in text:
        return text, []
    head, rest = text.split(marker, 1)
    bullets = [part.strip(" -\n") for part in rest.split(" - ") if part.strip(" -\n")]
    return head.strip(), bullets


def description_html(row: dict[str, str], fields: list[str], sds_url: str, pi_url: str) -> str:
    body, bullets = split_description(row.get("verpakking_omschrijving") or row.get("product_omschrijving") or "")
    parts: list[str] = []
    if body:
        for block in re.split(r"\n\s*\n", body):
            parts.append(f"<p>{html.escape(block).replace(chr(10), '<br>')}</p>")
    if bullets:
        items = "".join(f"<li>{html.escape(item)}</li>" for item in bullets)
        parts.append("<p><strong>Aanwijzingen voor gebruik</strong></p>")
        parts.append(f"<ul>{items}</ul>")
    colli = (row.get("colli") or "").strip()
    pack = (row.get("verpakking") or "").strip()
    if colli and pack:
        parts.append(f"<p><strong>Verpakking</strong><br>{html.escape(colli)} X {html.escape(pack)}</p>")
    toepassing = (row.get("verpakking_toepassing") or row.get("toepassing") or "").strip()
    if toepassing:
        parts.append(f"<h4>Toepassing</h4><p>{html.escape(toepassing)}</p>")
    links = []
    if sds_url:
        links.append(f'<a target="_blank" rel="noopener noreferrer" href="{html.escape(sds_url, quote=True)}">Veiligheidsblad</a>')
    if pi_url:
        links.append(f'<a target="_blank" rel="noopener noreferrer" href="{html.escape(pi_url, quote=True)}">Productinformatieblad</a>')
    if links:
        parts.append("<p>" + "<br>".join(links) + "</p>")
    start = fields.index(TYPICAL_START) if TYPICAL_START in fields else len(fields)
    typical_rows = []
    for key in fields[start:]:
        value = (row.get(key) or "").strip()
        if value:
            typical_rows.append(f"<tr><td>{html.escape(key)}</td><td>{html.escape(value)}</td></tr>")
    if typical_rows:
        parts.append("<table><tbody>" + "".join(typical_rows) + "</tbody></table>")
    return "".join(parts)


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"pdfs": {}, "done": []}


def save_state(state: dict) -> None:
    STATE_PATH.write_text(json.dumps(state))


def existing_by_sku(admin: MotoxAdmin, skus: list[str]) -> dict[str, dict]:
    found: dict[str, dict] = {}
    query = """
    query ($q: String!) {
      productVariants(first: 50, query: $q) {
        nodes {
          id
          sku
          price
          barcode
          selectedOptions { name value }
          product { id status }
        }
      }
    }
    """
    for offset in range(0, len(skus), 20):
        chunk = skus[offset:offset + 20]
        q = " OR ".join(f"sku:{sku}" for sku in chunk)
        body = admin.gql(query, {"q": q})
        if body.get("errors"):
            raise SystemExit(f"SKU-zoekfout: {body.get('errors')}")
        for node in ((body.get("data") or {}).get("productVariants") or {}).get("nodes") or []:
            sku = (node.get("sku") or "").strip()
            if sku:
                found[sku] = node
    return found


def stage_file(admin: MotoxAdmin, path: Path, mime: str, resource: str) -> str:
    data = path.read_bytes()
    body = admin.gql(
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
            "resource": resource,
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
    params = {item["name"]: item["value"] for item in target.get("parameters") or []}
    response = admin.sess.post(
        target["url"],
        data=params,
        files={"file": (path.name, data, mime)},
        timeout=180,
        proxies={"http": None, "https": None},
    )
    if response.status_code not in (200, 201, 204):
        raise RuntimeError(f"upload {response.status_code}")
    return target["resourceUrl"]


def shopify_pdf(admin: MotoxAdmin, state: dict, source_url: str, local_path: Path, alt: str) -> str:
    cached = (state.get("pdfs") or {}).get(source_url)
    if cached:
        return cached
    staged = stage_file(admin, local_path, "application/pdf", "FILE")
    created = admin.gql(
        """
        mutation ($files: [FileCreateInput!]!) {
          fileCreate(files: $files) {
            files { id }
            userErrors { field message }
          }
        }
        """,
        {"files": [{"originalSource": staged, "contentType": "FILE", "alt": alt[:250]}]},
    )
    if created.get("errors"):
        raise RuntimeError(str(created.get("errors"))[:300])
    payload = ((created.get("data") or {}).get("fileCreate")) or {}
    if payload.get("userErrors"):
        raise RuntimeError(str(payload.get("userErrors"))[:300])
    file_id = (payload.get("files") or [{}])[0].get("id")
    if not file_id:
        raise RuntimeError("pdf zonder id")
    for _ in range(20):
        nodes = admin.gql(
            """
            query ($id: ID!) { node(id: $id) { ... on GenericFile { url fileStatus } } }
            """,
            {"id": file_id},
        )
        node = ((nodes.get("data") or {}).get("node")) or {}
        if node.get("url"):
            state.setdefault("pdfs", {})[source_url] = node["url"]
            save_state(state)
            return node["url"]
        time.sleep(2)
    raise RuntimeError("pdf url niet klaar")


def upsert(admin: MotoxAdmin, row: dict[str, str], fields: list[str], existing: dict | None, image_src: str, html_body: str, video: str) -> str:
    title = slug_title(row)
    files = [{"originalSource": image_src, "contentType": "IMAGE", "alt": title[:250]}]
    if video:
        files.append({"originalSource": video, "contentType": "EXTERNAL_VIDEO", "alt": f"{title} video"[:250]})
    price = (row.get("brutoprijs_verpakking") or "").strip()
    barcode = (row.get("ean") or "").strip()
    weight = (row.get("gewicht_kg") or "").strip()
    variant: dict = {
        "optionValues": [{"optionName": "Title", "name": "Default Title"}],
        "sku": row["artikelnummer"],
    }
    if existing:
        options = existing.get("selectedOptions") or []
        if options:
            variant["optionValues"] = [{"optionName": opt["name"], "name": opt["value"]} for opt in options]
        variant["id"] = existing["id"]
        variant["price"] = price or str(existing.get("price") or "0.00")
        variant["barcode"] = barcode or (existing.get("barcode") or "")
        status = ((existing.get("product") or {}).get("status")) or "ACTIVE"
        product_id = existing["product"]["id"]
    else:
        variant["price"] = price or "0.00"
        variant["barcode"] = barcode
        variant["inventoryPolicy"] = "DENY"
        status = "DRAFT"
        product_id = None
    if weight:
        try:
            kilos = float(weight)
        except ValueError:
            kilos = 0
        if kilos > 0:
            variant["inventoryItem"] = {"measurement": {"weight": {"value": kilos, "unit": "KILOGRAMS"}}}
    product_input = {
        "title": title,
        "descriptionHtml": html_body,
        "vendor": "Putoline",
        "productType": (row.get("categorieen") or "").strip(),
        "status": status,
        "files": files,
        "variants": [variant],
    }
    options = (existing or {}).get("selectedOptions") or [{"name": "Title", "value": "Default Title"}]
    grouped: dict[str, list[str]] = {}
    for opt in options:
        grouped.setdefault(opt["name"], [])
        if opt["value"] not in grouped[opt["name"]]:
            grouped[opt["name"]].append(opt["value"])
    product_input["productOptions"] = [
        {"name": name, "values": [{"name": value} for value in values]}
        for name, values in grouped.items()
    ]
    if product_id:
        product_input["id"] = product_id
    else:
        product_input["tags"] = ["Putoline"]
    body = admin.gql(
        """
        mutation ($input: ProductSetInput!, $synchronous: Boolean!) {
          productSet(input: $input, synchronous: $synchronous) {
            product { id handle status }
            userErrors { field message code }
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
    return ""


def main() -> int:
    if len(sys.argv) < 2:
        print("Geef de cachemap met manifest.json", file=sys.stderr)
        return 1
    cache = Path(sys.argv[1])
    manifest = json.loads((cache / "manifest.json").read_text())
    fields, rows = load_rows()
    state = load_state()
    done = set(state.get("done") or [])
    admin = MotoxAdmin()
    skus = [row["artikelnummer"] for row in rows]
    print(f"Zoek {len(skus)} artikelnummers in Motox", flush=True)
    have = existing_by_sku(admin, skus)
    print(f"Bestaand: {len(have)}. Al klaar: {len(done)}", flush=True)
    created = updated = failed = 0
    for index, row in enumerate(rows, 1):
        sku = row["artikelnummer"]
        if sku in done:
            continue
        image_url = (row.get("verpakking_foto_groot") or "").strip()
        image_name = manifest.get(image_url)
        if not image_name:
            print(f"FAIL {sku} geen foto", flush=True)
            failed += 1
            continue
        try:
            image_src = stage_file(admin, cache / image_name, "image/png", "PRODUCT_IMAGE")
            sds_url = (row.get("product_veiligheidsblad") or "").strip()
            pi_url = (row.get("product_informatieblad") or "").strip()
            sds_link = pi_link = ""
            if sds_url and manifest.get(sds_url):
                sds_link = shopify_pdf(admin, state, sds_url, cache / manifest[sds_url], f"Veiligheidsblad {sku}")
            if pi_url and manifest.get(pi_url):
                pi_link = shopify_pdf(admin, state, pi_url, cache / manifest[pi_url], f"Productinformatieblad {sku}")
            video = youtube_url(row.get("verpakking_instructievideo") or row.get("instructievideo") or "")
            err = upsert(admin, row, fields, have.get(sku), image_src, description_html(row, fields, sds_link, pi_link), video)
        except Exception as exc:
            err = str(exc)[:300]
        if err:
            print(f"FAIL {sku} {err}", flush=True)
            failed += 1
            continue
        done.add(sku)
        state["done"] = sorted(done)
        save_state(state)
        if sku in have:
            updated += 1
            action = "bijgewerkt"
        else:
            created += 1
            action = "nieuw"
        print(f"{index}/{len(rows)} {action} {sku} {slug_title(row)}", flush=True)
    save_state(state)
    print(f"Klaar. Nieuw {created}, bijgewerkt {updated}, mislukt {failed}, overgeslagen {len(rows) - created - updated - failed}", flush=True)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
