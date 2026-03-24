from modules.shopify_client import get_all_shopify_skus
from modules.xml_loader import load_products
from modules.pricing_loader import load_price_index
from modules.exporter import export
from modules.image_manager import ensure_image, load_cache, save_cache

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

        if (
            sku not in shopify_skus
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

image_index = {}

for p in Path("input").rglob("*"):

    if p.is_file():
        image_index[p.name] = p

print(f"{len(image_index)} images gevonden op disk")


# -----------------------------------------------------
# unieke image filenames verzamelen
# -----------------------------------------------------

image_filenames = set()

for p in delta_products:

    for img in p.get("images", []):
        filename = Path(img).name
        image_filenames.add(filename)

print(f"{len(image_filenames)} unieke images referenced")


# -----------------------------------------------------
# images verwerken
# -----------------------------------------------------

image_url_map = {}

images_found = 0
images_missing = 0
images_uploaded = 0


def process_image(filename):

    local_path = image_index.get(filename)

    if not local_path:
        return ("missing", filename, None)

    url = ensure_image(filename, local_path, cache)

    return ("ok", filename, url)


with ThreadPoolExecutor(max_workers=20) as executor:

    futures = [executor.submit(process_image, f) for f in image_filenames]

    for future in as_completed(futures):
        try:
            status, filename, url = future.result()
        except Exception as e:
            # Keep the run alive when Shopify/media API is unreachable.
            images_missing += 1
            print(f"Image verwerking fout: {e}")
            continue

        if status == "missing":
            images_missing += 1
        else:
            images_found += 1
            image_url_map[filename] = url


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

            filename = Path(img).name
            url = image_url_map.get(filename)

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
print(f"unique images referenced: {len(image_filenames)}")
print(f"images found locally: {images_found}")
print(f"images uploaded: {images_uploaded}")
print(f"images missing locally: {images_missing}")


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
