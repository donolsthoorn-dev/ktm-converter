"""Motox Shopify Admin API: create products, update prices, set YMM metafields."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

from modules.motox_shopify_cache import _gql, _session, load_motox_env

log = logging.getLogger("ebihr.motox_publish")

# Motox theme / Metafields Manager (zie motox_ebihr_missing_ymm_imports)
META_NS = "global"
META_FITS_ON = "fits_on"
META_YMM_SUMMARY = "ymm_summary"
META_FITS_ON_YEAR = "fits_on_year"
META_FITS_ON_MAKE = "fits_on_make"
META_FITS_ON_MODEL = "fits_on_model"


class MotoxAdmin:
    def __init__(self):
        self.domain, self.token, self.api = load_motox_env()
        self.url = f"https://{self.domain}/admin/api/{self.api}/graphql.json"
        self.sess = _session()

    def gql(self, query: str, variables: dict | None = None) -> dict:
        return _gql(self.sess, self.url, self.token, query, variables)

    def product_set_create(self, group: dict, *, dry_run: bool = False) -> tuple[str | None, str]:
        """
        Create product via productSet. Returns (product_numeric_id, error_message).
        """
        handle = (group.get("handle") or "").strip()
        title = (group.get("title") or handle).strip()
        variants = group.get("variants") or []
        if not handle or not variants:
            return None, "missing handle/variants"
        if not (group.get("images") or []):
            return None, "no_image"

        option_names = list(group.get("option_names") or [])
        # Shopify requires at least Title option if no options
        if not option_names:
            option_names = ["Title"]
            for v in variants:
                v["option_values"] = ["Default Title"]

        # Collect option values per option
        product_options = []
        for oi, name in enumerate(option_names):
            vals = []
            seen = set()
            for v in variants:
                ovals = v.get("option_values") or []
                val = (ovals[oi] if oi < len(ovals) else "") or "Default"
                if val not in seen:
                    seen.add(val)
                    vals.append({"name": val})
            product_options.append({"name": name, "values": vals or [{"name": "Default"}]})

        set_variants = []
        for v in variants:
            ovals = v.get("option_values") or []
            option_values = []
            for oi, name in enumerate(option_names):
                val = (ovals[oi] if oi < len(ovals) else "") or "Default"
                option_values.append({"optionName": name, "name": val})
            price = (v.get("price") or "").strip() or "0.00"
            inv_policy = (v.get("inventory_policy") or "continue").upper()
            if inv_policy not in ("CONTINUE", "DENY"):
                inv_policy = "CONTINUE"
            entry: dict[str, Any] = {
                "optionValues": option_values,
                "sku": v.get("sku") or "",
                "barcode": v.get("barcode") or "",
                "price": price,
                "inventoryPolicy": inv_policy,
            }
            set_variants.append(entry)

        status = "ACTIVE" if group.get("published") else "DRAFT"
        inp: dict[str, Any] = {
            "title": title,
            "handle": handle,
            "descriptionHtml": group.get("body_html") or "",
            "vendor": group.get("vendor") or "",
            "productType": group.get("product_type") or "",
            "status": status,
            "productOptions": product_options,
            "variants": set_variants,
        }

        if dry_run:
            return "dry-run", ""

        q = """
        mutation MotoxProductSet($input: ProductSetInput!, $synchronous: Boolean!) {
          productSet(input: $input, synchronous: $synchronous) {
            product { id handle }
            userErrors { field message code }
          }
        }
        """
        body = self.gql(q, {"input": inp, "synchronous": True})
        if body.get("errors"):
            return None, str(body.get("errors"))[:500]
        payload = ((body.get("data") or {}).get("productSet")) or {}
        uerr = payload.get("userErrors") or []
        if uerr:
            return None, str(uerr)[:500]
        prod = payload.get("product") or {}
        gid = prod.get("id") or ""
        pid = gid.rsplit("/", 1)[-1] if gid else ""
        if not pid:
            return None, "no product id returned"
        return pid, ""

    def attach_images(self, product_id: str, urls: list[str], *, dry_run: bool = False) -> str:
        """Attach remote image URLs. Returns error string or empty."""
        urls = [u for u in urls if u][:20]
        if not urls:
            return ""
        if dry_run:
            return ""
        media = [
            {"originalSource": u, "mediaContentType": "IMAGE"}
            for u in urls
        ]
        q = """
        mutation MotoxProductCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
          productCreateMedia(productId: $productId, media: $media) {
            media { id }
            mediaUserErrors { field message code }
          }
        }
        """
        gid = f"gid://shopify/Product/{product_id}"
        body = self.gql(q, {"productId": gid, "media": media})
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        payload = ((body.get("data") or {}).get("productCreateMedia")) or {}
        uerr = payload.get("mediaUserErrors") or []
        if uerr:
            return str(uerr)[:300]
        return ""

    def update_variant_prices(
        self,
        product_id: str,
        variant_id_prices: list[tuple[str, str]],
        *,
        dry_run: bool = False,
    ) -> str:
        """variant_id_prices: list of (variant_numeric_id, price). Returns error or ''."""
        if not variant_id_prices:
            return ""
        if dry_run:
            return ""
        q = """
        mutation MotoxVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id }
            userErrors { field message }
          }
        }
        """
        variants = [
            {"id": f"gid://shopify/ProductVariant/{vid}", "price": price}
            for vid, price in variant_id_prices
        ]
        body = self.gql(
            q,
            {
                "productId": f"gid://shopify/Product/{product_id}",
                "variants": variants,
            },
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        payload = ((body.get("data") or {}).get("productVariantsBulkUpdate")) or {}
        uerr = payload.get("userErrors") or []
        if uerr:
            return str(uerr)[:300]
        return ""

    def set_ymm_metafields(
        self,
        product_id: str,
        *,
        fits_on_json: str,
        ymm_summary: str = "",
        fits_on_year: str = "",
        fits_on_make: str = "",
        fits_on_model: str = "",
        dry_run: bool = False,
    ) -> str:
        if not fits_on_json and not ymm_summary:
            return ""
        if dry_run:
            return ""
        owner = f"gid://shopify/Product/{product_id}"
        metafields = []
        if fits_on_json:
            metafields.append(
                {
                    "ownerId": owner,
                    "namespace": META_NS,
                    "key": META_FITS_ON,
                    "type": "json",
                    "value": fits_on_json,
                }
            )
        if ymm_summary:
            metafields.append(
                {
                    "ownerId": owner,
                    "namespace": META_NS,
                    "key": META_YMM_SUMMARY,
                    "type": "single_line_text_field",
                    "value": ymm_summary[:255],
                }
            )
        for key, val, mtype in (
            (META_FITS_ON_YEAR, fits_on_year, "single_line_text_field"),
            (META_FITS_ON_MAKE, fits_on_make, "single_line_text_field"),
            (META_FITS_ON_MODEL, fits_on_model, "single_line_text_field"),
        ):
            if val:
                metafields.append(
                    {
                        "ownerId": owner,
                        "namespace": META_NS,
                        "key": key,
                        "type": mtype,
                        "value": val[:65535],
                    }
                )
        q = """
        mutation MotoxMetafieldsSet($metafields: [MetafieldsSetInput!]!) {
          metafieldsSet(metafields: $metafields) {
            userErrors { field message code }
          }
        }
        """
        body = self.gql(q, {"metafields": metafields})
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        uerr = (((body.get("data") or {}).get("metafieldsSet")) or {}).get("userErrors") or []
        if uerr:
            return str(uerr)[:300]
        return ""


def fits_on_from_map(fits_on: dict, tokens: list[str]) -> dict | None:
    """Merge fits_on JSON objects for handle + variant SKUs."""
    merged: dict = {}
    for tok in tokens:
        raw = fits_on.get(tok) or fits_on.get(tok.upper()) or fits_on.get(tok.lower())
        if not raw:
            continue
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                continue
        if not isinstance(raw, dict):
            continue
        for make, models in raw.items():
            if not isinstance(models, dict):
                continue
            merged.setdefault(make, {})
            for model, years in models.items():
                merged[make].setdefault(model, [])
                for y in years if isinstance(years, list) else []:
                    ys = str(y)
                    if ys not in merged[make][model]:
                        merged[make][model].append(ys)
                merged[make][model] = sorted(set(merged[make][model]))
    return merged or None
