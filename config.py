import os
from datetime import datetime

from modules.env_loader import load_dotenv

load_dotenv()

# ----------------------------------
# Shopify / secrets (zie .env.example)
# ----------------------------------

SHOPIFY_ACCESS_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip()
SHOPIFY_SHOP_DOMAIN = os.environ.get("SHOPIFY_SHOP_DOMAIN", "ktm-shop-nl.myshopify.com").strip()
SHOPIFY_SHOP_SLUG = os.environ.get("SHOPIFY_SHOP_SLUG", "ktm-shop-nl").strip()
SHOPIFY_ADMIN_API_VERSION = os.environ.get("SHOPIFY_ADMIN_API_VERSION", "2024-10").strip()
SHOPIFY_CDN_FILES_BASE_URL = os.environ.get(
    "SHOPIFY_CDN_FILES_BASE_URL",
    "https://cdn.shopify.com/s/files/1/0511/7820/9461/files/",
).strip()
if SHOPIFY_CDN_FILES_BASE_URL and not SHOPIFY_CDN_FILES_BASE_URL.endswith("/"):
    SHOPIFY_CDN_FILES_BASE_URL += "/"

# ----------------------------------
# INPUT
# ----------------------------------

INPUT_DIR = "input"
XML_FILE = os.path.join(INPUT_DIR, "CBEXPDN_KTM-DN-3008-0.xml")

# ----------------------------------
# OUTPUT STRUCTUUR
# ----------------------------------

BASE_OUTPUT_DIR = "output"
# Product-CSV’s (Shopify-import)
PRODUCTS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "products")
# Handle → Shopify Product Id (product_ids_from_xml*.csv)
IDS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "ids")
# YMM app-import CSV’s
YMM_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "ymm")
# Metafields Manager-export
METAFIELDS_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "metafields")
LOG_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "logs")

# Timestamp per run
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Shopify export bestand
OUTPUT_FILE = os.path.join(PRODUCTS_OUTPUT_DIR, f"shopify_export_{timestamp}.csv")

# ----------------------------------
# ALGEMENE INSTELLINGEN
# ----------------------------------

CULTURE = "EN-GB"
VAT_MULTIPLIER = 1.21

# XML producttypes: uitgesloten van delta-logica (main) en exportfilter (exporter)
DELTA_EXCLUDED_TYPES = frozenset({"Bikes", "Pricelists", "Archiv", "Archive", "Arhive"})
