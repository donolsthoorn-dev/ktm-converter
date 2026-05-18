import glob
import os
from datetime import datetime

from modules.brand_config import BrandConfig, DEFAULT_BRAND_ID, get_brand_config
from modules.env_loader import load_dotenv

load_dotenv()

# Actief merk (ktm = legacy paden input/ en output/)
_active_brand: BrandConfig = get_brand_config(DEFAULT_BRAND_ID)
BRAND_ID: str = _active_brand.id
HANDLE_PREFIX: str = _active_brand.handle_prefix


def get_active_brand() -> BrandConfig:
    return _active_brand


def apply_brand(brand_id: str | None = None) -> BrandConfig:
    """Herlaad paden en XML-pad voor het opgegeven merk (of BRAND uit env)."""
    global _active_brand, BRAND_ID, HANDLE_PREFIX
    global INPUT_DIR, XML_FILE, BASE_OUTPUT_DIR
    global PRODUCTS_OUTPUT_DIR, IDS_OUTPUT_DIR, YMM_OUTPUT_DIR
    global METAFIELDS_OUTPUT_DIR, LOG_OUTPUT_DIR, OUTPUT_FILE

    raw = brand_id if brand_id is not None else os.environ.get("BRAND", DEFAULT_BRAND_ID)
    _active_brand = get_brand_config(raw)
    BRAND_ID = _active_brand.id
    HANDLE_PREFIX = _active_brand.handle_prefix

    INPUT_DIR = _active_brand.input_dir
    BASE_OUTPUT_DIR = _active_brand.base_output_dir
    PRODUCTS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "products")
    IDS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "ids")
    YMM_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "ymm")
    METAFIELDS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "metafields")
    LOG_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "logs")
    XML_FILE = _resolve_xml_file(_active_brand)
    OUTPUT_FILE = os.path.join(PRODUCTS_OUTPUT_DIR, f"shopify_export_{timestamp}.csv")
    return _active_brand


def apply_handle_prefix(handle: str) -> str:
    """Shopify-handle met merk-prefix (hsq- / wp-); KTM ongewijzigd."""
    h = (handle or "").strip().lower()
    prefix = HANDLE_PREFIX
    if not prefix or not h:
        return h
    if h.startswith(prefix):
        return h
    return f"{prefix}{h}"


def get_image_search_roots() -> list[str]:
    """
    Mappen voor lokale afbeeldingen (PHO/DOK).
    HSQ/WP zoeken ook in gedeelde input/ (KTM-afbeeldingen).
    """
    roots: list[str] = [INPUT_DIR]
    shared = "input"
    if INPUT_DIR != shared and shared not in roots:
        roots.append(shared)
    return roots


def _resolve_xml_file(brand: BrandConfig) -> str:
    """Pad naar export-XML: env per merk, anders nieuwste match op xml_glob in input_dir."""
    raw = os.environ.get(brand.xml_env_var, "").strip()
    if raw:
        if os.path.isabs(raw):
            return raw
        if os.sep in raw or (os.altsep and os.altsep in raw):
            return os.path.normpath(raw)
        return os.path.join(brand.input_dir, raw)

    pattern = os.path.join(brand.input_dir, brand.xml_glob)
    matches = glob.glob(pattern)
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        return max(matches, key=lambda p: os.path.getmtime(p))
    return os.path.join(brand.input_dir, brand.xml_fallback_name)


# ----------------------------------
# Shopify / secrets (zie .env.example)
# ----------------------------------

SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
_shop_raw = os.environ.get("SHOPIFY_SHOP_DOMAIN", "").strip()
SHOPIFY_SHOP_DOMAIN = _shop_raw if _shop_raw else "ktm-shop-nl.myshopify.com"
SHOPIFY_SHOP_SLUG = os.environ.get("SHOPIFY_SHOP_SLUG", "ktm-shop-nl").strip()
_api_ver_raw = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "").strip()
SHOPIFY_ADMIN_API_VERSION = _api_ver_raw if _api_ver_raw else "2024-10"
SHOPIFY_CDN_FILES_BASE_URL = os.environ.get(
    "SHOPIFY_CDN_FILES_BASE_URL",
    "https://cdn.shopify.com/s/files/1/0511/7820/9461/files/",
).strip()
if SHOPIFY_CDN_FILES_BASE_URL and not SHOPIFY_CDN_FILES_BASE_URL.endswith("/"):
    SHOPIFY_CDN_FILES_BASE_URL += "/"

# Timestamp per run (vóór apply_brand — gebruikt in OUTPUT_FILE)
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# ----------------------------------
# INPUT / OUTPUT (merk — default KTM = legacy paden)
# ----------------------------------

apply_brand(os.environ.get("BRAND", DEFAULT_BRAND_ID))

# ----------------------------------
# ALGEMENE INSTELLINGEN
# ----------------------------------

CULTURE = "EN-GB"
VAT_MULTIPLIER = 1.21

DELTA_EXCLUDED_TYPES = frozenset({"Bikes", "Pricelists", "Archiv", "Archive", "Arhive"})
