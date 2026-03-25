import csv
import html
import os
import re

import config
from modules.category_mapper import map_category, map_shopify_product_category

IMAGE_BASE_URL = config.SHOPIFY_CDN_FILES_BASE_URL

HEADER = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Product category",
    "Type",
    "Tags",
    "Published",
    "Option1 Name",
    "Option1 Value",
    "Option2 Name",
    "Option2 Value",
    "Option3 Name",
    "Option3 Value",
    "Variant SKU",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Qty",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Compare At Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "Variant Barcode",
    "Image Src",
    "Image Position",
    "Image Alt Text",
    "Gift Card",
    "SEO Title",
    "SEO Description",
    "Google Shopping / Google Product Category",
    "Google Shopping / Gender",
    "Google Shopping / Age Group",
    "Google Shopping / MPN",
    "Google Shopping / AdWords Grouping",
    "Google Shopping / AdWords Labels",
    "Google Shopping / Condition",
    "Google Shopping / Custom Product",
    "Google Shopping / Custom Label 0",
    "Google Shopping / Custom Label 1",
    "Google Shopping / Custom Label 2",
    "Google Shopping / Custom Label 3",
    "Google Shopping / Custom Label 4",
    "Variant Image",
    "Variant Weight Unit",
    "Variant Tax Code",
    "Cost per item",
]

COL = {h: i for i, h in enumerate(HEADER)}


def setcol(row, name, value):
    row[COL[name]] = value if value else ""


def normalize_image_url(path: str) -> str:
    if not path:
        return ""
    # Al een volledige CDN-URL (bijv. na ensure_image / fileCreate): niet herschrijven.
    if path.startswith("http://") or path.startswith("https://"):
        p = path.split("?", 1)[0].strip()
        if p.lower().startswith(IMAGE_BASE_URL.lower().rstrip("/")) or "/cdn/shop/files/" in p:
            return path.strip()
        filename = os.path.basename(path.split("?", 1)[0])
    else:
        filename = os.path.basename(path)
    if not filename:
        return ""
    return IMAGE_BASE_URL + filename


def strip_html(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"<[^>]+>", " ", text)
    t = html.unescape(t)
    return re.sub(r"\s+", " ", t).strip()


def truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 1].rstrip() + "…"


def normalize_gender(gender: str) -> str:
    g = (gender or "").strip().lower()
    if not g:
        return ""
    if "women" in g or "female" in g or g == "ladies":
        return "Female"
    if "men" in g or "male" in g:
        return "Male"
    if "unisex" in g:
        return "Unisex"
    return gender


def infer_age_group(title: str, tags: str, type_value: str) -> str:
    text = " ".join([title or "", tags or "", type_value or ""]).lower()
    if any(
        k in text for k in ("kids", "kid", "junior", "youth", "child", "children", "boy", "girl")
    ):
        return "Kids"
    return "Adult"


def build_image_alt_text(title: str, option_name: str, option_value: str) -> str:
    if option_name and option_name != "Title" and option_value and option_value != "Default Title":
        return f"{title} - {option_name}: {option_value}"
    return title


# -----------------------------------------------------
# EXPORT
# -----------------------------------------------------


def export(products, filename):

    os.makedirs(os.path.dirname(filename), exist_ok=True)

    with open(filename, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(HEADER)

        # ---------------- GROUP BY HANDLE ----------------

        products_by_handle = {}

        for p in products:
            handle = p.get("handle") or p.get("sku")

            products_by_handle.setdefault(handle, []).append(p)

        # ---------------- EXPORT ----------------

        for handle, items in products_by_handle.items():
            # primary product
            primary = max(items, key=lambda x: len(x.get("title", "")))
            if (primary.get("type") or "").strip() in config.DELTA_EXCLUDED_TYPES:
                continue

            images = primary.get("images", [])

            for idx, p in enumerate(items):
                row = [""] * len(HEADER)

                title = primary.get("title")
                category = primary.get("category")
                type_value = primary.get("type")
                tags_value = primary.get("tags") or category
                body_html = primary.get("description") if idx == 0 else ""
                plain_description = strip_html(primary.get("description") or "")
                seo_title = truncate(title or "", 70)
                seo_description = truncate(plain_description, 320)

                setcol(row, "Handle", handle)
                setcol(row, "Title", title if idx == 0 else "")
                setcol(row, "Body (HTML)", body_html)
                setcol(row, "Vendor", "KTM")
                if idx == 0:
                    setcol(row, "Product category", map_shopify_product_category(category))
                setcol(row, "Type", type_value if idx == 0 else "")
                setcol(row, "Tags", tags_value if idx == 0 else "")
                published = "FALSE" if str(p.get("article_status", "")).strip() == "80" else "TRUE"
                setcol(row, "Published", published)

                setcol(row, "Variant SKU", p.get("sku"))
                setcol(row, "Variant Barcode", p.get("barcode"))

                option1_value = p.get("variant") or "Default Title"
                option1_name = p.get("variant_label") or "Title"
                if option1_value == "Default Title":
                    option1_name = "Title"

                # Shopify: elke variant moet een unieke optie-combinatie hebben. Bij meerdere artikelen
                # onder dezelfde handle (bundle) staan ze vaak allemaal op "Default Title" — dat geeft
                # duplicate "Title / Default Title" en faalt import ("variant Default Title already exists").
                default_title_count = sum(
                    1 for x in items if (x.get("variant") or "Default Title") == "Default Title"
                )
                if (
                    len(items) > 1
                    and default_title_count > 1
                    and (p.get("variant") or "Default Title") == "Default Title"
                ):
                    option1_name = "Article"
                    option1_value = (p.get("sku") or "").strip() or "UNKNOWN"

                setcol(row, "Option1 Name", option1_name)
                setcol(row, "Option1 Value", option1_value)
                setcol(row, "Variant Inventory Tracker", "shopify")
                setcol(row, "Variant Inventory Policy", "continue")
                setcol(row, "Variant Fulfillment Service", "manual")
                setcol(row, "Variant Requires Shipping", "TRUE")
                setcol(row, "Variant Taxable", "TRUE")
                setcol(row, "Variant Weight Unit", "kg")
                setcol(row, "Gift Card", "FALSE")

                setcol(row, "Variant Price", p.get("price"))

                if idx == 0:
                    setcol(row, "Google Shopping / Google Product Category", map_category(category))
                    setcol(row, "SEO Title", seo_title)
                    setcol(row, "SEO Description", seo_description)

                setcol(row, "Google Shopping / MPN", p.get("sku"))
                setcol(row, "Google Shopping / Condition", "new")
                setcol(row, "Google Shopping / Custom Product", "FALSE")
                setcol(
                    row, "Google Shopping / Gender", normalize_gender(primary.get("gender") or "")
                )
                setcol(
                    row,
                    "Google Shopping / Age Group",
                    infer_age_group(title, tags_value, type_value),
                )
                setcol(row, "Google Shopping / AdWords Grouping", type_value)
                setcol(row, "Google Shopping / AdWords Labels", tags_value)

                # image
                if idx == 0 and images:
                    setcol(row, "Image Src", normalize_image_url(images[0]))
                    setcol(row, "Image Position", 1)
                    setcol(
                        row,
                        "Image Alt Text",
                        build_image_alt_text(title, option1_name, option1_value),
                    )

                writer.writerow(row)

            # Shopify accepts extra image-only rows for a handle.
            for img_pos, img in enumerate(images[1:], start=2):
                image_row = [""] * len(HEADER)
                setcol(image_row, "Handle", handle)
                setcol(image_row, "Image Src", normalize_image_url(img))
                setcol(image_row, "Image Position", img_pos)
                setcol(image_row, "Image Alt Text", title)
                writer.writerow(image_row)
