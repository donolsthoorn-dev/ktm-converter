#!/usr/bin/env python3
"""Pilot: verrijk één fietsproduct met data van ktm-bikes.at.

Doelproduct: variant SKU 1260002138 (MACINA PROWLER ELITE Di2, Royal Teal, L/48).

Schrijft product-metafields (battery_wh, motor, weight_kg, geometry, marketing_text),
corrigeert SEO description_tag, en voegt lifestyle-/kleurfoto's toe.

Gebruik:
  python3 scripts/enrich_bike_pilot_1260002138.py
  python3 scripts/enrich_bike_pilot_1260002138.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import ssl
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import config  # noqa: E402

SKU = "1260002138"
NS = "custom"

MARKETING_NL = (
    "ALL IN. ALL OUT. Opnieuw hebben de KTM-engineers hun expertise gebundeld om met de "
    "nieuwe PROWLER-modellen recht te doen aan de bewezen slogan ‘All in. All Out.’ "
    "Technische precisie en doordacht design maken de PROWLER tot een echte "
    "high-performance endurofiets.\n\n"
    "De geoptimaliseerde geometrie, gecombineerd met het bewezen STRAIGHT-LINE-LINK "
    "(SLL)-concept, zorgt voor een gevoelige en directe chassisrespons. Dankzij "
    "180 mm veerweg voor en 170 mm achter voelt de PROWLER zich thuis op veeleisende "
    "endurotrails.\n\n"
    "De Dimension Mix (DiMMiX) biedt een beslissend voordeel in lastige situaties: "
    "29\" wielen voor voor optimale rijstabiliteit en stuurprecisie, gecombineerd met "
    "27,5\" banden achter voor optimale krachtoverbrenging en tractie.\n\n"
    "De Bosch Performance Line CX (BDU384Y) tilt rijdynamiek en wendbaarheid naar een "
    "nieuw niveau. Krachtig, licht en dynamisch (120 Nm max. koppel / 750 watt max. "
    "motorvermogen / 600% max. ondersteuningsfactor).\n\n"
    "De Power Tube-accu biedt een capaciteit van 800 Wh. Ook hier kiest KTM opnieuw "
    "voor het bewezen POWER TUBE TOP LOADER (PTTL)-concept: de door KTM ontwikkelde "
    "accu-uitname naar boven uit de onderbuis, zodat verwijderen en terugplaatsen "
    "kinderspel is."
)

MOTOR = "Bosch PERFORMANCE CX BDU3840 up to 120Nm"
BATTERY_WH = 800
WEIGHT_KG = "26.2"
MAX_SYSTEM_WEIGHT_KG = "144"

GEOMETRY = {
    "M": {
        "Size": "43",
        "Seattube length": "430",
        "Toptube length": "602",
        "Seat angle": "75,577 (73,9°)",
        "Head angle": "64,1°",
        "Headtube length": "100",
        "Chainstay length": "448",
        "Wheelbase": "1237",
        "Stack": "618",
        "Reach": "443",
        "Standover height": "836",
        "BB drop": "-1",
    },
    "L": {
        "Size": "48",
        "Seattube length": "450",
        "Toptube length": "622",
        "Seat angle": "75,593 (73,9°)",
        "Head angle": "64,1°",
        "Headtube length": "110",
        "Chainstay length": "448",
        "Wheelbase": "1259",
        "Stack": "627",
        "Reach": "461",
        "Standover height": "840",
        "BB drop": "0",
    },
    "XL": {
        "Size": "53",
        "Seattube length": "480",
        "Toptube length": "647",
        "Seat angle": "75,613 (74°)",
        "Head angle": "64,1°",
        "Headtube length": "125",
        "Chainstay length": "448",
        "Wheelbase": "1287",
        "Stack": "641",
        "Reach": "483",
        "Standover height": "844",
        "BB drop": "0",
    },
}

IMAGE_URLS = [
    # Bewust leeg: productfoto's niet ophalen/vervangen vanaf ktm-bikes.at.
]

SUBTITLE = "1X11 SHIMANO DEORE Di2 LG"

DEFINITIONS = [
    {
        "name": "Batterij (Wh)",
        "key": "battery_wh",
        "type": "number_integer",
        "description": "Accucapaciteit in Wh voor e-bike badge op collectie- en productpagina.",
    },
    {
        "name": "Motor",
        "key": "motor",
        "type": "single_line_text_field",
        "description": "E-bike motoraanduiding (bijv. Bosch PERFORMANCE CX …).",
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
        "description": "Toelaatbaar totaalgewicht (fiets + berijder + bagage) in kg.",
    },
    {
        "name": "Geometrie",
        "key": "geometry",
        "type": "json",
        "description": "Geometrietabel per framemaat (JSON object met keys M/L/XL/…).",
    },
    {
        "name": "Marketingtekst",
        "key": "marketing_text",
        "type": "multi_line_text_field",
        "description": "Korte marketingtekst voor de productpagina (NL).",
    },
    {
        "name": "Fiets ondertitel",
        "key": "bike_subtitle",
        "type": "single_line_text_field",
        "description": "Korte drivetrain/ondertitel onder de titel (bijv. 1X12 SRAM XX TRANSMISSION).",
    },
]

SSL_CTX = ssl._create_unverified_context()


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
            print(f"  metafield bestaat: {NS}.{item['key']}")
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
        print(f"    → {(result.get('createdDefinition') or {}).get('id')}")


def find_product() -> dict:
    data = gql(
        """
        query($q: String!) {
          productVariants(first: 1, query: $q) {
            nodes {
              id
              sku
              product {
                id
                title
                handle
                descriptionHtml
                media(first: 20) {
                  nodes {
                    ... on MediaImage {
                      id
                      alt
                      image { url }
                    }
                  }
                }
                metafields(first: 30) {
                  nodes { namespace key value }
                }
              }
            }
          }
        }
        """,
        {"q": f"sku:{SKU}"},
    )
    nodes = ((data.get("data") or {}).get("productVariants") or {}).get("nodes") or []
    if not nodes:
        raise RuntimeError(f"Geen variant met SKU {SKU}")
    return nodes[0]["product"]


def set_metafields(product_id: str, dry_run: bool) -> None:
    metafields = [
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "battery_wh",
            "type": "number_integer",
            "value": str(BATTERY_WH),
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "motor",
            "type": "single_line_text_field",
            "value": MOTOR,
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "weight_kg",
            "type": "number_decimal",
            "value": WEIGHT_KG,
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "max_system_weight_kg",
            "type": "number_decimal",
            "value": MAX_SYSTEM_WEIGHT_KG,
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "geometry",
            "type": "json",
            "value": json.dumps(GEOMETRY, ensure_ascii=False),
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "marketing_text",
            "type": "multi_line_text_field",
            "value": MARKETING_NL,
        },
        {
            "ownerId": product_id,
            "namespace": NS,
            "key": "bike_subtitle",
            "type": "single_line_text_field",
            "value": SUBTITLE,
        },
        {
            "ownerId": product_id,
            "namespace": "global",
            "key": "description_tag",
            "type": "string",
            "value": (
                "Ontdek de KTM E-MTB Fully – elektrische mountainbike met Bosch CX-motor, "
                "800 Wh accu en volledige vering. Perfect voor offroad, bergen en trails."
            ),
        },
    ]
    print(f"  metafields zetten ({len(metafields)})")
    if dry_run:
        for mf in metafields:
            preview = mf["value"]
            if len(preview) > 80:
                preview = preview[:77] + "..."
            print(f"    {mf['namespace']}.{mf['key']} = {preview}")
        return
    result = gql(
        """
        mutation($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            metafields { id key namespace }
            userErrors { field message }
          }
        }
        """,
        {"metafields": metafields},
    )
    payload = ((result.get("data") or {}).get("metafieldsSet")) or {}
    errors = payload.get("userErrors") or result.get("errors") or []
    if errors:
        raise RuntimeError(errors)
    print(f"    → {len(payload.get('metafields') or [])} metafields ok")


def existing_image_urls(product: dict) -> set[str]:
    urls: set[str] = set()
    for node in ((product.get("media") or {}).get("nodes") or []):
        image = node.get("image") or {}
        url = (image.get("url") or "").split("?", 1)[0]
        if url:
            urls.add(url.lower())
        alt = (node.get("alt") or "").lower()
        if "royal teal" in alt or "sfeerbeeld" in alt:
            urls.add(alt)
    return urls


def add_images(product_id: str, product: dict, dry_run: bool) -> None:
    # Productfoto's bewust niet ophalen vanaf ktm-bikes.at.
    print("  afbeeldingen: overgeslagen")
    return


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Alleen tonen, niets schrijven")
    args = parser.parse_args()

    print(f"Shop: {config.SHOPIFY_SHOP_DOMAIN}")
    print(f"Pilot SKU: {SKU}")
    print("1) Metafield-definities")
    ensure_definitions(args.dry_run)

    print("2) Product zoeken")
    product = find_product()
    print(f"  {product['title']} ({product['handle']})")
    print(f"  id={product['id']}")

    print("3) Metafields + SEO")
    set_metafields(product["id"], args.dry_run)

    print("4) Afbeeldingen")
    add_images(product["id"], product, args.dry_run)

    print("Klaar." + (" (dry-run)" if args.dry_run else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
