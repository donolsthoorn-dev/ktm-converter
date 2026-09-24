"""Motox Shopify Admin API: create products, update prices, set YMM metafields, publish."""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import requests

from modules.motox_shopify_cache import _gql, _session, load_motox_env

log = logging.getLogger("ebihr.motox_publish")


def _mime_ext_from_magic(head: bytes) -> tuple[str, str] | None:
    if head[:3] == b"\xff\xd8\xff":
        return "image/jpeg", ".jpg"
    if head[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png", ".png"
    if head[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif", ".gif"
    if len(head) >= 12 and head[:4] == b"RIFF" and head[8:12] == b"WEBP":
        return "image/webp", ".webp"
    return None


def _needs_image_rehost(url: str) -> bool:
    """Bihr WebP URLs break Shopify remote fetch (wrong Content-Type / extension)."""
    path = (url or "").split("?", 1)[0].lower()
    return path.endswith(".webp")


def _customs_input(hs_code: str | None, country: str | None) -> dict[str, str]:
    """Shopify InventoryItem-velden. Lege waarden worden niet meegestuurd."""
    out: dict[str, str] = {}
    hs = (hs_code or "").strip()
    co = (country or "").strip().upper()
    if hs:
        out["harmonizedSystemCode"] = hs
    if co:
        out["countryCodeOfOrigin"] = co
    return out


def _sku_label(sku: str, index: int) -> str:
    s = (sku or "").strip()
    if s:
        return s
    return f"Variant {index + 1}"


def _unique_variant_option_rows(group: dict) -> tuple[list[str], list[list[str]]]:
    """
    Build option names + per-variant values that Shopify will accept.

    Bihr often leaves variation attrs empty on sibling SKUs. Filling those with
    the same "Default" / "Default Title" makes productSet return INVALID_VARIANT.
    Empty/duplicate combos are uniquified with the SKU.
    """
    variants = group.get("variants") or []
    option_names = list(group.get("option_names") or [])

    if not option_names:
        option_names = ["Title"]
        if len(variants) <= 1:
            return option_names, [["Default Title"]]
        rows = [[_sku_label(v.get("sku") or "", i)] for i, v in enumerate(variants)]
        return option_names, rows

    rows: list[list[str]] = []
    for v in variants:
        ovals = list(v.get("option_values") or [])
        while len(ovals) < len(option_names):
            ovals.append("")
        row = []
        for oi in range(len(option_names)):
            raw = ovals[oi]
            val = raw.strip() if isinstance(raw, str) else (str(raw) if raw else "")
            row.append(val or "Default")
        rows.append(row)

    seen: dict[tuple[str, ...], int] = {}
    for i, row in enumerate(rows):
        key = tuple(row)
        if key not in seen:
            seen[key] = i
            continue
        label = _sku_label(variants[i].get("sku") or "", i)
        # Prefer renaming the last option so earlier dimensions stay readable.
        new_row = list(row)
        last = new_row[-1]
        if last in ("", "Default", "Default Title"):
            new_row[-1] = label
        else:
            new_row[-1] = f"{last} ({label})"
        # Extremely rare: still colliding after rename
        guard = 0
        while tuple(new_row) in seen and guard < 5:
            new_row[-1] = f"{new_row[-1]}-{guard}"
            guard += 1
        rows[i] = new_row
        seen[tuple(new_row)] = i

    return option_names, rows


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
        self._publication_ids: list[str] | None = None

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

        option_names, value_rows = _unique_variant_option_rows(group)
        # Collect option values per option (post-uniquify)
        product_options = []
        for oi, name in enumerate(option_names):
            vals = []
            seen: set[str] = set()
            for row in value_rows:
                val = row[oi]
                if val not in seen:
                    seen.add(val)
                    vals.append({"name": val})
            product_options.append({"name": name, "values": vals or [{"name": "Default"}]})

        set_variants = []
        for v, row in zip(variants, value_rows):
            option_values = [
                {"optionName": name, "name": row[oi]}
                for oi, name in enumerate(option_names)
            ]
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
            customs = _customs_input(v.get("hs_code"), v.get("country"))
            if customs:
                entry["inventoryItem"] = customs
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
        """
        Attach images to a product. Returns error string or empty.

        Bihr sometimes serves WebP as Content-Type ``application/webp`` (and/or
        ``*.webp`` URLs). Shopify's remote fetch then fails with
        "File extension doesn't match the format of the file". Those are
        re-hosted via stagedUploadsCreate with a correct ``image/webp`` MIME.
        """
        urls = [u for u in urls if u][:20]
        if not urls:
            return ""
        if dry_run:
            return ""

        # Drop prior FAILED media so backfill can retry cleanly.
        self.delete_failed_media(product_id)

        sources: list[str] = []
        errors: list[str] = []
        for i, src in enumerate(urls):
            if _needs_image_rehost(src):
                staged, err = self._rehost_image_url(src, index=i)
                if staged:
                    sources.append(staged)
                else:
                    errors.append(err or f"rehost_failed:{src[:80]}")
            else:
                sources.append(src)
        if not sources:
            return "; ".join(errors)[:300] or "no_images_after_rehost"

        media = [{"originalSource": u, "mediaContentType": "IMAGE"} for u in sources]
        q = """
        mutation MotoxProductCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
          productCreateMedia(productId: $productId, media: $media) {
            media { id status }
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
        if errors:
            return f"partial:{'; '.join(errors)}"[:300]
        return ""

    def _rehost_image_url(self, src: str, *, index: int = 0) -> tuple[str | None, str]:
        """Download remote image and staged-upload with MIME from magic bytes."""
        try:
            r = self.sess.get(
                src,
                timeout=60,
                proxies={"http": None, "https": None},
                headers={"User-Agent": "Motox-eBihr-Sync/1.0"},
            )
            r.raise_for_status()
            data = r.content or b""
        except Exception as e:
            return None, f"download:{e}"[:200]
        if len(data) < 16:
            return None, "download:empty"

        detected = _mime_ext_from_magic(data[:32])
        if not detected:
            return None, "download:unknown_image_type"
        mime, ext = detected
        # Prefer stem from URL path for a stable Shopify filename.
        path = src.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1] or f"bihr-{index}"
        stem = path.rsplit(".", 1)[0] if "." in path else path
        filename = f"{stem}{ext}"

        stage_q = """
        mutation MotoxStagedUploads($input: [StagedUploadInput!]!) {
          stagedUploadsCreate(input: $input) {
            stagedTargets {
              url
              resourceUrl
              parameters { name value }
            }
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            stage_q,
            {
                "input": [
                    {
                        "filename": filename,
                        "mimeType": mime,
                        "httpMethod": "POST",
                        "resource": "PRODUCT_IMAGE",
                        "fileSize": str(len(data)),
                    }
                ]
            },
        )
        if body.get("errors"):
            return None, str(body.get("errors"))[:200]
        payload = ((body.get("data") or {}).get("stagedUploadsCreate")) or {}
        uerr = payload.get("userErrors") or []
        if uerr:
            return None, str(uerr)[:200]
        targets = payload.get("stagedTargets") or []
        if not targets:
            return None, "staged:no_target"
        target = targets[0]
        post_url = target.get("url") or ""
        resource_url = target.get("resourceUrl") or ""
        params = {
            p["name"]: p["value"]
            for p in (target.get("parameters") or [])
            if p.get("name")
        }
        if not post_url or not resource_url:
            return None, "staged:missing_urls"
        try:
            files = {"file": (filename, data, mime)}
            pr = self.sess.post(
                post_url,
                data=params,
                files=files,
                timeout=120,
                proxies={"http": None, "https": None},
            )
        except Exception as e:
            return None, f"staged_post:{e}"[:200]
        if pr.status_code not in (200, 201, 204):
            return None, f"staged_post:{pr.status_code}"[:200]
        return resource_url, ""

    def delete_failed_media(self, product_id: str) -> str:
        """Remove FAILED MediaImage nodes so backfill can retry. Returns error or ''."""
        q = """
        query MotoxFailedMedia($id: ID!) {
          product(id: $id) {
            media(first: 50) {
              nodes {
                id
                ... on MediaImage { status }
              }
            }
          }
        }
        """
        body = self.gql(q, {"id": f"gid://shopify/Product/{product_id}"})
        if body.get("errors"):
            return str(body.get("errors"))[:200]
        prod = ((body.get("data") or {}).get("product")) or {}
        nodes = ((prod.get("media") or {}).get("nodes")) or []
        failed_ids = [
            n["id"]
            for n in nodes
            if isinstance(n, dict) and n.get("status") == "FAILED" and n.get("id")
        ]
        if not failed_ids:
            return ""
        dq = """
        mutation MotoxDeleteMedia($productId: ID!, $mediaIds: [ID!]!) {
          productDeleteMedia(productId: $productId, mediaIds: $mediaIds) {
            deletedMediaIds
            mediaUserErrors { field message code }
          }
        }
        """
        body = self.gql(
            dq,
            {
                "productId": f"gid://shopify/Product/{product_id}",
                "mediaIds": failed_ids,
            },
        )
        if body.get("errors"):
            return str(body.get("errors"))[:200]
        uerr = (((body.get("data") or {}).get("productDeleteMedia")) or {}).get(
            "mediaUserErrors"
        ) or []
        if uerr:
            return str(uerr)[:200]
        log.info(
            "Deleted %s FAILED media on product %s",
            len(failed_ids),
            product_id,
        )
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

    def update_variant_barcodes(
        self,
        product_id: str,
        variant_id_barcodes: list[tuple[str, str]],
        *,
        dry_run: bool = False,
    ) -> str:
        """variant_id_barcodes: list of (variant_numeric_id, barcode). Returns error or ''."""
        if not variant_id_barcodes:
            return ""
        if dry_run:
            return ""
        q = """
        mutation MotoxVariantsBulkBarcode($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
          productVariantsBulkUpdate(productId: $productId, variants: $variants) {
            productVariants { id }
            userErrors { field message }
          }
        }
        """
        variants = [
            {"id": f"gid://shopify/ProductVariant/{vid}", "barcode": barcode}
            for vid, barcode in variant_id_barcodes
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

    def update_variant_customs(
        self,
        product_id: str,
        variant_customs: list[tuple[str, str, str]],
        *,
        dry_run: bool = False,
    ) -> str:
        """variant_customs: (variant_numeric_id, hs_code, country). Lege velden blijven staan.

        Bij een bulk-fout opnieuw per variant proberen, zodat één slechte HS/COO
        niet de rest van het product blokkeert.
        """
        variants = []
        for vid, hs_code, country in variant_customs:
            customs = _customs_input(hs_code, country)
            if not vid or not customs:
                continue
            variants.append(
                {
                    "id": f"gid://shopify/ProductVariant/{vid}",
                    "inventoryItem": customs,
                }
            )
        if not variants:
            return ""
        if dry_run:
            return ""

        def _bulk(chunk: list[dict]) -> str:
            q = """
            mutation MotoxVariantsBulkCustoms($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
              productVariantsBulkUpdate(productId: $productId, variants: $variants) {
                productVariants { id }
                userErrors { field message }
              }
            }
            """
            body = self.gql(
                q,
                {
                    "productId": f"gid://shopify/Product/{product_id}",
                    "variants": chunk,
                },
            )
            if body.get("errors"):
                return str(body.get("errors"))[:300]
            payload = ((body.get("data") or {}).get("productVariantsBulkUpdate")) or {}
            uerr = payload.get("userErrors") or []
            if uerr:
                return str(uerr)[:300]
            return ""

        err = _bulk(variants)
        if not err:
            return ""
        if len(variants) == 1:
            return err

        # Fallback: write what we can; collect remaining errors.
        failed: list[str] = []
        for one in variants:
            one_err = _bulk([one])
            if one_err:
                failed.append(f"{one['id'].rsplit('/', 1)[-1]}:{one_err}")
            else:
                time.sleep(0.05)
        if not failed:
            log.warning(
                "Douane bulk fail op product %s, maar per-variant OK: %s",
                product_id,
                err,
            )
            return ""
        return f"bulk:{err}; failed:{len(failed)}/{len(variants)} {'; '.join(failed)}"[:500]

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

    def product_has_media(self, product_id: str) -> tuple[bool, str]:
        """Live check: at least one READY image. FAILED-only counts as no media."""
        q = """
        query MotoxProductMedia($id: ID!) {
          product(id: $id) {
            featuredMedia { id }
            media(first: 20) {
              nodes {
                id
                ... on MediaImage { status }
              }
            }
          }
        }
        """
        body = self.gql(q, {"id": f"gid://shopify/Product/{product_id}"})
        if body.get("errors"):
            return False, str(body.get("errors"))[:300]
        prod = ((body.get("data") or {}).get("product")) or {}
        if not prod:
            return False, "product_not_found"
        if prod.get("featuredMedia"):
            return True, ""
        nodes = ((prod.get("media") or {}).get("nodes")) or []
        for n in nodes:
            if not isinstance(n, dict):
                continue
            status = (n.get("status") or "").upper()
            if status in ("READY", "UPLOADED", "PROCESSING"):
                return True, ""
            if not status and n.get("id"):
                # Non-MediaImage or status unavailable — treat as present.
                return True, ""
        return False, ""

    def set_status(self, product_id: str, status: str, *, dry_run: bool = False) -> str:
        """Zet productstatus (ACTIVE/DRAFT/ARCHIVED). Returns error or ''."""
        status = (status or "").strip().upper()
        if status not in ("ACTIVE", "DRAFT", "ARCHIVED"):
            return f"invalid status {status}"
        if dry_run:
            return ""
        q = """
        mutation MotoxProductSetStatus($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id status }
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            q,
            {"input": {"id": f"gid://shopify/Product/{product_id}", "status": status}},
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        uerr = (((body.get("data") or {}).get("productUpdate")) or {}).get("userErrors") or []
        if uerr:
            return str(uerr)[:300]
        return ""

    def ensure_active(self, product_id: str, *, dry_run: bool = False) -> str:
        """Zet productstatus op ACTIVE. Returns error or ''."""
        return self.set_status(product_id, "ACTIVE", dry_run=dry_run)

    def unpublish_from_all_channels(self, product_id: str, *, dry_run: bool = False) -> str:
        """Haal product van alle publications. Returns error or ''."""
        if dry_run:
            return ""
        pub_ids, err = self.list_publication_ids()
        if err or not pub_ids:
            return self._publish_online_store_only_unpublished(product_id) if not err else err
        q = """
        mutation MotoxPublishableUnpublish($id: ID!, $input: [PublicationInput!]!) {
          publishableUnpublish(id: $id, input: $input) {
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            q,
            {
                "id": f"gid://shopify/Product/{product_id}",
                "input": [{"publicationId": pid} for pid in pub_ids],
            },
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        uerr = (
            (((body.get("data") or {}).get("publishableUnpublish")) or {}).get("userErrors") or []
        )
        if uerr:
            return str(uerr)[:400]
        return ""

    def _publish_online_store_only_unpublished(self, product_id: str) -> str:
        q = """
        mutation MotoxUnpublishOnline($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id publishedAt }
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            q,
            {
                "input": {
                    "id": f"gid://shopify/Product/{product_id}",
                    "published": False,
                }
            },
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        uerr = (((body.get("data") or {}).get("productUpdate")) or {}).get("userErrors") or []
        if uerr:
            return str(uerr)[:300]
        return ""

    def list_publication_ids(self, *, force: bool = False) -> tuple[list[str], str]:
        """
        Alle sales-channel publication IDs (Online Store, POS, …).
        Vereist read_publications scope. Cached op de instance.
        """
        if self._publication_ids is not None and not force:
            return self._publication_ids, ""
        q = """
        query MotoxPublications {
          publications(first: 50) {
            nodes { id name }
          }
        }
        """
        body = self.gql(q)
        if body.get("errors"):
            return [], str(body.get("errors"))[:400]
        nodes = (((body.get("data") or {}).get("publications")) or {}).get("nodes") or []
        ids = [n["id"] for n in nodes if n.get("id")]
        self._publication_ids = ids
        log.info(
            "Motox publications: %s",
            ", ".join(f"{n.get('name')}={n.get('id')}" for n in nodes) or "(none)",
        )
        return ids, ""

    def publish_to_all_channels(self, product_id: str, *, dry_run: bool = False) -> str:
        """
        Publiceer product op alle shop-publications (alle kanalen).
        Falls back to productUpdate published:true (Online Store only) als
        publications-scope ontbreekt.
        """
        if dry_run:
            return ""
        pub_ids, err = self.list_publication_ids()
        if err or not pub_ids:
            # Fallback: alleen Online Store via legacy published flag
            log.warning(
                "publications API niet beschikbaar (%s); fallback productUpdate published=true",
                err or "empty",
            )
            return self._publish_online_store_only(product_id)

        q = """
        mutation MotoxPublishablePublish($id: ID!, $input: [PublicationInput!]!) {
          publishablePublish(id: $id, input: $input) {
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            q,
            {
                "id": f"gid://shopify/Product/{product_id}",
                "input": [{"publicationId": pid} for pid in pub_ids],
            },
        )
        if body.get("errors"):
            # Scope missing → fallback
            err_s = str(body.get("errors"))
            if "ACCESS" in err_s.upper() or "publication" in err_s.lower():
                log.warning("publishablePublish geweigerd; fallback Online Store: %s", err_s[:200])
                return self._publish_online_store_only(product_id)
            return err_s[:300]
        uerr = (
            (((body.get("data") or {}).get("publishablePublish")) or {}).get("userErrors") or []
        )
        if uerr:
            # Partial channel errors: log but treat hard failures only
            msg = str(uerr)[:400]
            # If all failed, return error; Shopify often returns per-channel noise
            return msg
        return ""

    def _publish_online_store_only(self, product_id: str) -> str:
        q = """
        mutation MotoxPublishOnline($input: ProductInput!) {
          productUpdate(input: $input) {
            product { id publishedAt }
            userErrors { field message }
          }
        }
        """
        body = self.gql(
            q,
            {
                "input": {
                    "id": f"gid://shopify/Product/{product_id}",
                    "published": True,
                }
            },
        )
        if body.get("errors"):
            return str(body.get("errors"))[:300]
        uerr = (((body.get("data") or {}).get("productUpdate")) or {}).get("userErrors") or []
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
