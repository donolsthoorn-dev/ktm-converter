import os
from datetime import datetime

# ----------------------------------
# INPUT
# ----------------------------------

INPUT_DIR = "input"
XML_FILE = os.path.join(INPUT_DIR, "CBEXPDN_KTM-DN-3008-0.xml")

# ----------------------------------
# OUTPUT STRUCTUUR
# ----------------------------------

BASE_OUTPUT_DIR = "output"
SHOPIFY_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "shopify")
REPORT_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "reports")
LOG_OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, "logs")

# Timestamp per run
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Shopify export bestand
OUTPUT_FILE = os.path.join(
    SHOPIFY_OUTPUT_DIR,
    f"shopify_export_{timestamp}.csv"
)

# ----------------------------------
# ALGEMENE INSTELLINGEN
# ----------------------------------

CULTURE = "EN-GB"
VAT_MULTIPLIER = 1.21
