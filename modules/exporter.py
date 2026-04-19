import csv
import html
import json
import os
import re
from pathlib import Path

import requests

import config
from modules.category_mapper import map_category, map_shopify_product_category

IMAGE_BASE_URL = config.SHOPIFY_CDN_FILES_BASE_URL


def _cdn_products_base_from_files_base(files_base: str) -> str:
    """
    Shopify CDN: content-bestanden staan onder .../<shop-path>/files/<name>.
    Product-import (CSV) verwacht vaak .../<shop-path>/products/<name> (zelfde host/pad-prefix).
    """
    fb = (files_base or "").rstrip("/")
    if fb.lower().endswith("/files"):
        return fb[: -len("/files")].rstrip("/") + "/products/"
    return files_base


IMAGE_PRODUCTS_BASE_URL = _cdn_products_base_from_files_base(IMAGE_BASE_URL)
_CSV_IMAGE_CACHE_FILE = Path("cache/csv_image_url_cache.json")
_CSV_IMAGE_CDN_RE = re.compile(
    r"^(https?://cdn\.shopify\.com/s/files/\d+(?:/\d+)+)/(files|products)/([^?#]+)([?#].*)?$",
    re.IGNORECASE,
)
_csv_image_cache_loaded = False
_csv_image_cache_dirty = False
_csv_image_choice_cache: dict[str, str] = {}
_csv_image_reachability_cache: dict[str, bool] = {}
_session: requests.Session | None = None


def _http_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.trust_env = False
    return _session


def _truthy(raw: str) -> bool:
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _legacy_use_products_override() -> str | None:
    raw = os.environ.get("KTM_SHOPIFY_CSV_USE_PRODUCTS_IMAGE_PATH", "").strip()
    if not raw:
        return None
    return "products" if _truthy(raw) else "files"


def _image_url_mode() -> str:
    legacy = _legacy_use_products_override()
    if legacy:
        return legacy
    raw = os.environ.get("KTM_SHOPIFY_CSV_IMAGE_PATH_MODE", "auto").strip().lower()
    if raw in ("files", "products", "auto"):
        return raw
    return "auto"


def _load_csv_image_cache() -> None:
    global _csv_image_cache_loaded, _csv_image_choice_cache, _csv_image_reachability_cache
    if _csv_image_cache_loaded:
        return
    _csv_image_choice_cache = {}
    _csv_image_reachability_cache = {}
    if _CSV_IMAGE_CACHE_FILE.exists():
        try:
            payload = json.loads(_CSV_IMAGE_CACHE_FILE.read_text(encoding="utf-8"))
            choices = payload.get("path_choice")
            reachability = payload.get("url_reachable")
            if isinstance(choices, dict):
                _csv_image_choice_cache = {
                    str(k): str(v)
                    for k, v in choices.items()
                    if isinstance(k, str) and str(v) in ("files", "products")
                }
            if isinstance(reachability, dict):
                _csv_image_reachability_cache = {
                    str(k): bool(v) for k, v in reachability.items() if isinstance(k, str)
                }
        except (OSError, json.JSONDecodeError):
            pass
    _csv_image_cache_loaded = True


def _save_csv_image_cache() -> None:
    global _csv_image_cache_dirty
    if not _csv_image_cache_dirty:
        return
    payload = {
        "path_choice": _csv_image_choice_cache,
        "url_reachable": _csv_image_reachability_cache,
    }
    _CSV_IMAGE_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    _CSV_IMAGE_CACHE_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    _csv_image_cache_dirty = False


def _strip_query(url: str) -> str:
    return (url or "").strip().split("#", 1)[0].split("?", 1)[0]


def _cdn_variant_url(url: str, segment: str) -> str | None:
    m = _CSV_IMAGE_CDN_RE.match((url or "").strip())
    if not m:
        return None
    return f"{m.group(1)}/{segment}/{m.group(3)}{m.group(4) or ''}"


def _reachability_key(url: str) -> str:
    return _strip_query(url).strip().lower()


def _is_url_reachable(url: str) -> bool:
    global _csv_image_cache_dirty
    key = _reachability_key(url)
    if not key:
        return False
    cached = _csv_image_reachability_cache.get(key)
    if isinstance(cached, bool):
        return cached

    sess = _http_session()
    ok = False
    try:
        r = sess.head(
            url,
            timeout=8,
            allow_redirects=True,
            proxies={"http": None, "https": None},
            headers={"User-Agent": "KTM-ETL/1.0"},
        )
        ok = r.status_code in (200, 301, 302, 303, 307, 308)
    except Exception:
        ok = False
    if not ok:
        try:
            r = sess.get(
                url,
                timeout=12,
                stream=True,
                proxies={"http": None, "https": None},
                headers={"User-Agent": "KTM-ETL/1.0"},
            )
            ok = r.status_code == 200
            r.close()
        except Exception:
            ok = False

    _csv_image_reachability_cache[key] = ok
    _csv_image_cache_dirty = True
    return ok


def _candidate_urls(path: str) -> tuple[str, str]:
    s = (path or "").strip()
    if not s:
        return "", ""
    if s.startswith("http://") or s.startswith("https://"):
        files_url = _cdn_variant_url(s, "files")
        products_url = _cdn_variant_url(s, "products")
        if files_url and products_url:
            return files_url, products_url
        # Bijv. oude /cdn/shop/files/ URL: niet herschrijven, direct gebruiken.
        return s, s
    filename = os.path.basename(s)
    if not filename:
        return "", ""
    return IMAGE_BASE_URL + filename, IMAGE_PRODUCTS_BASE_URL + filename


def _cache_choice_key(files_url: str) -> str:
    return os.path.basename(_strip_query(files_url)).strip().lower()


def _pick_best_cdn_url(files_url: str, products_url: str, mode: str) -> str:
    global _csv_image_cache_dirty
    if not files_url:
        return products_url
    if not products_url or files_url == products_url:
        return files_url

    if mode == "files":
        return files_url
    if mode == "products":
        return products_url

    choice_key = _cache_choice_key(files_url)
    if choice_key:
        cached = _csv_image_choice_cache.get(choice_key)
        if cached == "files":
            return files_url
        if cached == "products":
            return products_url

    # Auto-mode: eerst files, dan products.
    if _is_url_reachable(files_url):
        if choice_key:
            _csv_image_choice_cache[choice_key] = "files"
            _csv_image_cache_dirty = True
        return files_url
    if _is_url_reachable(products_url):
        if choice_key:
            _csv_image_choice_cache[choice_key] = "products"
            _csv_image_cache_dirty = True
        return products_url

    # Geen van beide publiek bereikbaar: veilige fallback blijft /files.
    if choice_key:
        _csv_image_choice_cache[choice_key] = "files"
        _csv_image_cache_dirty = True
    return files_url


def _shopify_cdn_content_files_to_products_path(url: str) -> str:
    """
    Herschrijf library-URL (.../s/files/<id-path>/files/<file>) naar product-pad (.../products/<file>).
    Laat query/hash ongemoeid (Shopify voegt ?v= soms pas toe na koppeling aan productmedia).
    """
    s = (url or "").strip()
    if not s.lower().startswith("http"):
        return s
    if re.search(r"/s/files/\d+(?:/\d+)*/products/", s, re.IGNORECASE):
        return s
    m = re.match(
        r"^(https?://cdn\.shopify\.com/s/files/\d+(?:/\d+)+)/files/([^?#]+)([?#].*)?$",
        s,
        re.IGNORECASE,
    )
    if not m:
        return s
    base, fname, tail = m.group(1), m.group(2), m.group(3) or ""
    return f"{base}/products/{fname}{tail}"


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
    _load_csv_image_cache()
    mode = _image_url_mode()
    files_url, products_url = _candidate_urls(path)
    if not files_url and not products_url:
        return ""
    return _pick_best_cdn_url(files_url, products_url, mode)


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

    _save_csv_image_cache()
