"""Shared paths and CSV headers for the e-bihr pipeline."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]  # repo root
EBIHR_ROOT = ROOT / "motox" / "e-bihr"
RAW_ROOT = EBIHR_ROOT / "raw"
GENERATED_ROOT = EBIHR_ROOT / "generated"
OUTPUT_ROOT = EBIHR_ROOT / "output"

IMAGE_BASE_URL = "https://api.mybihr.com"

EXCLUDED_BRANDS = frozenset({"rst", "capit", "oxford", "pinlock"})

# Hardparts zonder OFFROAD-fitment die toch mee mogen (parity oude parser).
HARDPART_CATEGORY_ALLOWLIST = frozenset(
    {
        "Smeermiddel & Reinigingsmiddel",
        "Transport - Garage - Paddock",
        "Gereedschap",
    }
)

CSV_HEADER = [
    "Handle",
    "Title",
    "Body (HTML)",
    "Vendor",
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

YMM_CSV_HEADER = ["Product Ids", "Make", "Model", "Year"]

# Shopify Admin import comfort zone
DEFAULT_CHUNK_BYTES = 14 * 1024 * 1024
