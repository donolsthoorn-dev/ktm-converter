from modules.shopify_client import get_all_shopify_skus
from modules.xml_loader import load_products
from modules.pricing_loader import load_price_index
from modules.exporter import export
from modules.image_manager import ensure_image, load_cache, save_cache
from modules.image_resolve import build_basename_index, resolve_local_image

import os
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed


run_id = datetime.now().strftime("%Y%m%d_%H%M%S")


# -----------------------------------------------------
# XML laden
# -----------------------------------------------------

print("XML laden en verwerken...")
products = load_products()
product_count = len(products)


# -----------------------------------------------------
# Shopify SKU's ophalen
# -----------------------------------------------------

print("Shopify SKUs ophalen...")
shopify_skus = get_all_shopify_skus()


# -----------------------------------------------------
# prijzen en barcodes laden
# -----------------------------------------------------

print("Prijzen en barcodes laden...")
price_index, barcode_index, status_index = load_price_index()

price_count = len(price_index)
barcode_count = len(barcode_index)

# -----------------------------------------------------
# prijzen / barcodes / status koppelen op alle producten
# -----------------------------------------------------

for p in products:
    sku = p["sku"]
    p["price"] = price_index.get(sku, "")
    p["barcode"] = barcode_index.get(sku, "")
    p["article_status"] = status_index.get(sku, "")
    p["product_category"] = p.get("category", "")
    p["type"] = p.get("type", "")


# -----------------------------------------------------
# delta bepalen
# -----------------------------------------------------

print("Delta tussen Shopify en nieuwe producten bepalen...")

excluded_types = {"Bikes", "Pricelists", "Archiv", "Arhive"}

products_by_handle = {}

for p in products:
    products_by_handle.setdefault(p["handle"], []).append(p)

delta_products = []

for handle, items in products_by_handle.items():

    new_variant_exists = False

    for p in items:

        sku = p["sku"]
        sku_norm = (sku or "").strip().upper()

        if (
            sku_norm not in shopify_skus
            and float(price_index.get(sku, 0)) > 0
            and p.get("type") not in excluded_types
            and status_index.get(sku) != "80"
        ):
            new_variant_exists = True
            break

    if new_variant_exists:

        for p in items:

            sku = p["sku"]

            if (
                float(price_index.get(sku, 0)) > 0
                and status_index.get(sku) != "80"
            ):
                delta_products.append(p)

print(f"Nieuwe producten gevonden: {len(delta_products)}")


# -----------------------------------------------------
# images voorbereiden
# -----------------------------------------------------

print("Images controleren en uploaden indien nodig...")

cache = load_cache()


# -----------------------------------------------------
# lokale images indexeren
# -----------------------------------------------------

print("Lokale images indexeren...")

input_root = Path("input")
by_basename_exact, by_basename_lower = build_basename_index(input_root)
files_on_disk = sum(len(v) for v in by_basename_exact.values())
print(
    f"{files_on_disk} bestanden onder input/, "
    f"{len(by_basename_exact)} unieke bestandsnamen"
)


# -----------------------------------------------------
# unieke image-paden uit XML (volledige ref, niet alleen basename)
# -----------------------------------------------------

image_refs = set()

for p in delta_products:

    for img in p.get("images", []):
        s = (img or "").strip()
        if s:
            image_refs.add(s)

print(f"{len(image_refs)} unieke image-referenties in delta (uit XML)")


def _norm_ref_key(s: str) -> str:
    return s.strip().replace("\\", "/").lower()


image_url_by_norm: dict[str, str] = {}


# -----------------------------------------------------
# images verwerken
# -----------------------------------------------------

image_url_map = {}

images_resolved = 0
images_no_file = 0
images_local_failed = 0
images_uploaded = 0


def process_image(ref: str):

    local_path = resolve_local_image(ref, input_root, by_basename_exact, by_basename_lower)

    if not local_path:
        return ("no_file", ref)

    cache_name = local_path.name
    url, did_upload = ensure_image(cache_name, local_path, cache, strict_delta=True)

    if url:
        return ("resolved", ref, url, did_upload)
    return ("failed", ref)


# GraphQL file-upload is rate-limited; te veel parallel = THROTTLED
try:
    _image_workers = max(1, min(8, int(os.environ.get("KTM_IMAGE_UPLOAD_WORKERS", "2").strip() or "2")))
except ValueError:
    _image_workers = 2

print(f"Afbeelding-upload workers (parallel): {_image_workers}", flush=True)

with ThreadPoolExecutor(max_workers=_image_workers) as executor:

    futures = [executor.submit(process_image, r) for r in image_refs]

    for future in as_completed(futures):
        try:
            result = future.result()
        except Exception as e:
            # Keep the run alive when Shopify/media API is unreachable.
            images_local_failed += 1
            print(f"Image verwerking fout: {e}")
            continue

        kind = result[0]
        if kind == "no_file":
            images_no_file += 1
        elif kind == "failed":
            images_local_failed += 1
        else:
            _, ref, url, did_upload = result
            images_resolved += 1
            if did_upload:
                images_uploaded += 1
            image_url_map[ref] = url
            image_url_by_norm[_norm_ref_key(ref)] = url


save_cache(cache)


# -----------------------------------------------------
# CDN urls koppelen aan producten
# -----------------------------------------------------

products_by_handle = {}

for p in delta_products:
    products_by_handle.setdefault(p["handle"], []).append(p)

filtered_delta = []

for handle, items in products_by_handle.items():

    group_has_images = False

    for p in items:

        new_images = []

        for img in p.get("images", []):

            raw = (img or "").strip()
            url = image_url_map.get(raw) if raw else None
            if not url and raw:
                url = image_url_by_norm.get(_norm_ref_key(raw))

            if url:
                new_images.append(url)

        p["images"] = new_images

        if new_images:
            group_has_images = True

    if group_has_images:
        filtered_delta.extend(items)


print(f"producten zonder image gefilterd: {len(delta_products) - len(filtered_delta)}")

delta_products = filtered_delta


# -----------------------------------------------------
# image summary
# -----------------------------------------------------

print("\nImage processing summary")
print("------------------------")
print(f"unique image references (XML paths): {len(image_refs)}")
print(f"met geldige URL (CDN/cache of na upload): {images_resolved}")
print(f"nieuw geüpload naar Shopify in deze run: {images_uploaded}")
print(f"geen bestand op schijf onder input/: {images_no_file}")
print(f"lokaal wel bestand, maar geen geldige URL: {images_local_failed}")


# -----------------------------------------------------
# Delta producten herstellen per handle
# -----------------------------------------------------

products_by_handle = {}

for p in delta_products:
    products_by_handle.setdefault(p["handle"], []).append(p)

fixed_delta = []

for handle, items in products_by_handle.items():

    # check of titel ontbreekt
    has_title = any(p.get("title") for p in items)

    if not has_title:

        # pak volledige productgroep uit ALL products
        for p in products:
            if p["handle"] == handle:
                fixed_delta.append(p)

    else:

        fixed_delta.extend(items)

delta_products = fixed_delta


# -----------------------------------------------------
# CSV export
# -----------------------------------------------------

print("CSV export maken...")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

delta_file = f"output/shopify/shopify_export_delta_{timestamp}.csv"
all_file = f"output/shopify/shopify_export_all_{timestamp}.csv"

print("CSV export delta maken...")
export(delta_products, delta_file)

print("CSV export ALL maken...")
export(products, all_file)


print("ETL run voltooid.")
