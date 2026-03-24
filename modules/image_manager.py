import json
from pathlib import Path
import requests

# -----------------------------
# CONFIG
# -----------------------------

SHOPIFY_STORE = "ktm-shop-nl"
SHOPIFY_TOKEN = "REDACTED_REVOKE_AND_ROTATE"

CDN_BASE = "https://cdn.shopify.com/s/files/1/0511/7820/9461/files/"
UPLOAD_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/files.json"

CACHE_FILE = "cache/image_cache.json"


# -----------------------------
# CACHE
# -----------------------------

def load_cache():
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


# -----------------------------
# URL helpers
# -----------------------------

def build_url(filename):
    return f"{CDN_BASE}{filename}"


def check_cdn(filename):

    url = build_url(filename)

    try:
        r = requests.head(url, timeout=5)

        if r.status_code == 200:
            return True

    except Exception:
        pass

    return False


# -----------------------------
# Upload
# -----------------------------

def upload_image(local_path):

    headers = {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN
    }

    with open(local_path, "rb") as f:

        files = {"file": f}

        r = requests.post(
            UPLOAD_URL,
            headers=headers,
            files=files
        )

    return r.status_code in (200, 201)


# -----------------------------
# Main function
# -----------------------------

def ensure_image(filename, local_path, cache):

    if filename in cache:
        return build_url(filename)

    if check_cdn(filename):
        cache[filename] = True
        return build_url(filename)

    ok = upload_image(local_path)

    if ok:
        cache[filename] = True
        return build_url(filename)

    return None
