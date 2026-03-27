import logging
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import config  # noqa: F401 — laadt .env vóór Shopify-modules
from modules.excluded_report import write_excluded_report
from modules.exporter import export
from modules.image_manager import (
    ensure_image,
    load_cache,
    save_cache_safe,
    try_resolve_image_cache_or_cdn,
)
from modules.image_resolve import build_basename_index, resolve_local_image
from modules.pricing_loader import load_price_index, normalize_sku_key
from modules.xml_loader import load_products

_log = logging.getLogger(__name__)


def _configure_logging() -> None:
    raw = os.environ.get("KTM_LOG_LEVEL", "INFO").strip().upper()
    level = getattr(logging, raw, logging.INFO)
    if not isinstance(level, int):
        level = logging.INFO

    fmt = "%(asctime)s %(levelname)s %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handlers: list[logging.Handler] = [logging.StreamHandler()]

    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.environ.get("KTM_LOG_FILE", "").strip()
    if not log_path:
        os.makedirs(config.LOG_OUTPUT_DIR, exist_ok=True)
        log_path = os.path.join(config.LOG_OUTPUT_DIR, f"ktm_etl_{run_ts}.log")
    else:
        parent = os.path.dirname(log_path)
        if parent:
            os.makedirs(parent, exist_ok=True)
    fh = logging.FileHandler(log_path, encoding="utf-8")
    handlers.append(fh)

    logging.basicConfig(level=level, format=fmt, datefmt=datefmt, handlers=handlers, force=True)
    _log.info("Logbestand: %s", log_path)


def main() -> None:
    _configure_logging()
    log = logging.getLogger(__name__)

    # -----------------------------------------------------
    # XML laden
    # -----------------------------------------------------

    log.info("XML laden en verwerken...")
    products = load_products()

    # -----------------------------------------------------
    # prijzen en barcodes laden
    # -----------------------------------------------------

    log.info("Prijzen en barcodes laden...")
    price_index, barcode_index, status_index = load_price_index()

    # -----------------------------------------------------
    # prijzen / barcodes / status koppelen op alle producten
    # -----------------------------------------------------

    for p in products:
        sku_key = normalize_sku_key(p["sku"])
        p["price"] = price_index.get(sku_key, "")
        p["barcode"] = barcode_index.get(sku_key, "")
        p["article_status"] = status_index.get(sku_key, "")
        p["product_category"] = p.get("category", "")
        p["type"] = p.get("type", "")

    # -----------------------------------------------------
    # delta bepalen
    # -----------------------------------------------------

    log.info("Delta bepalen (zonder Shopify SKU-vergelijking)...")

    excluded_types = config.DELTA_EXCLUDED_TYPES

    products_by_handle = {}

    for p in products:
        products_by_handle.setdefault(p["handle"], []).append(p)

    delta_products = []

    for handle, items in products_by_handle.items():
        # Trigger: minstens één variant met verkoopbare regels (geen Shopify-SKU-check)
        delta_variant_exists = False

        for p in items:
            sku_key = normalize_sku_key(p["sku"])

            if (
                float(price_index.get(sku_key, 0)) > 0
                and p.get("type") not in excluded_types
                and status_index.get(sku_key) != "80"
            ):
                delta_variant_exists = True
                break

        if delta_variant_exists:
            for p in items:
                sku_key = normalize_sku_key(p["sku"])

                if float(price_index.get(sku_key, 0)) > 0 and status_index.get(sku_key) != "80":
                    delta_products.append(p)

    log.info("Producten in delta: %s", len(delta_products))

    delta_initial_skus = {normalize_sku_key(p["sku"]) for p in delta_products if p.get("sku")}

    # -----------------------------------------------------
    # images voorbereiden
    # -----------------------------------------------------

    log.info("Images controleren en uploaden indien nodig...")

    cache = load_cache()

    # -----------------------------------------------------
    # lokale images indexeren
    # -----------------------------------------------------

    log.info("Lokale images indexeren...")

    input_root = Path("input")
    by_basename_exact, by_basename_lower = build_basename_index(input_root)
    files_on_disk = sum(len(v) for v in by_basename_exact.values())
    log.info(
        "%s bestanden onder input/, %s unieke bestandsnamen",
        files_on_disk,
        len(by_basename_exact),
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

    log.info("%s unieke image-referenties in delta (uit XML)", len(image_refs))

    def _norm_ref_key(s: str) -> str:
        return s.strip().replace("\\", "/").lower()

    image_url_by_norm: dict[str, str] = {}

    # -----------------------------------------------------
    # images verwerken (per uniek lokaal bestand — voorkomt race + dubbele upload)
    # -----------------------------------------------------
    # Meerdere XML-paden kunnen naar hetzelfde bestand wijzen. Parallel per ref
    # betekende dat meerdere threads tegelijk cache misten en allemaal uploadden.

    abs_path_to_refs: dict[str, list[str]] = defaultdict(list)
    abs_path_to_local: dict[str, Path] = {}
    refs_no_file: list[str] = []

    for ref in image_refs:
        local_path = resolve_local_image(ref, input_root, by_basename_exact, by_basename_lower)
        if not local_path:
            refs_no_file.append(ref)
            continue
        ap = str(local_path.resolve())
        abs_path_to_refs[ap].append(ref)
        abs_path_to_local[ap] = local_path

    images_no_file = len(refs_no_file)
    unique_local_paths = list(abs_path_to_refs.keys())
    log.info(
        "%s unieke lokale bestanden om te verwerken (na dedup t.o.v. %s refs)",
        len(unique_local_paths),
        len(image_refs),
    )

    image_url_map = {}

    images_resolved = 0
    images_local_failed = 0
    images_uploaded = 0

    def process_one_local_file(abs_path: str) -> tuple[str, list[str], str | None, bool]:
        local_path = abs_path_to_local[abs_path]
        refs = abs_path_to_refs[abs_path]
        cache_name = local_path.name
        url, did_upload = ensure_image(cache_name, local_path, cache, strict_delta=True)
        return ("resolved" if url else "failed", refs, url, did_upload)

    # Eerst synchroon: cache + CDN-HEAD (geen threads, geen GraphQL). Alleen rest naar workers.
    need_slow: list[str] = []
    for ap in unique_local_paths:
        local_path = abs_path_to_local[ap]
        refs = abs_path_to_refs[ap]
        cache_name = local_path.name
        url = try_resolve_image_cache_or_cdn(cache_name, cache)
        if url is not None:
            images_resolved += len(refs)
            for ref in refs:
                image_url_map[ref] = url
                image_url_by_norm[_norm_ref_key(ref)] = url
        else:
            need_slow.append(ap)

    # Fast path kan cache vullen (CDN-HEAD → _store_cache_url); direct naar schijf.
    save_cache_safe(cache)

    log.info(
        "Afbeeldingen: %s via cache/CDN (geen Shopify-lookup/upload); %s nog te controleren",
        len(unique_local_paths) - len(need_slow),
        len(need_slow),
    )

    # GraphQL file-upload is rate-limited; te veel parallel = THROTTLED
    try:
        _image_workers = max(
            1, min(8, int(os.environ.get("KTM_IMAGE_UPLOAD_WORKERS", "2").strip() or "2"))
        )
    except ValueError:
        _image_workers = 2

    log.info("Afbeelding-upload workers (parallel): %s", _image_workers)

    with ThreadPoolExecutor(max_workers=_image_workers) as executor:
        futures = [executor.submit(process_one_local_file, ap) for ap in need_slow]

        for future in as_completed(futures):
            try:
                kind, refs, url, did_upload = future.result()
            except Exception as e:
                # Keep the run alive when Shopify/media API is unreachable.
                images_local_failed += 1
                log.warning("Image verwerking fout: %s", e)
                continue

            if kind == "failed" or not url:
                images_local_failed += len(refs)
                continue

            images_resolved += len(refs)
            if did_upload:
                images_uploaded += 1
            for ref in refs:
                image_url_map[ref] = url
                image_url_by_norm[_norm_ref_key(ref)] = url

            # ensure_image heeft cache bijgewerkt (lookup/upload); niet pas aan het einde wegschrijven.
            save_cache_safe(cache)

    save_cache_safe(cache)

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

    delta_after_images_skus = {
        normalize_sku_key(p["sku"]) for p in filtered_delta if p.get("sku")
    }

    log.info(
        "producten zonder image gefilterd: %s",
        len(delta_products) - len(filtered_delta),
    )

    delta_products = filtered_delta

    # -----------------------------------------------------
    # image summary
    # -----------------------------------------------------

    log.info(
        "Image processing summary\n"
        "------------------------\n"
        "unique image references (XML paths): %s\n"
        "met geldige URL (CDN/cache of na upload): %s\n"
        "nieuw geüpload naar Shopify in deze run: %s\n"
        "geen bestand op schijf onder input/: %s\n"
        "lokaal wel bestand, maar geen geldige URL: %s",
        len(image_refs),
        images_resolved,
        images_uploaded,
        images_no_file,
        images_local_failed,
    )

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

    delta_final_skus = {normalize_sku_key(p["sku"]) for p in delta_products if p.get("sku")}

    # -----------------------------------------------------
    # CSV export
    # -----------------------------------------------------

    log.info("CSV export maken...")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    delta_file = os.path.join(config.PRODUCTS_OUTPUT_DIR, f"shopify_export_delta_{timestamp}.csv")
    all_file = os.path.join(config.PRODUCTS_OUTPUT_DIR, f"shopify_export_all_{timestamp}.csv")
    excluded_file = os.path.join(
        config.PRODUCTS_OUTPUT_DIR, f"shopify_export_excluded_{timestamp}.csv"
    )

    log.info("CSV export delta maken...")
    export(delta_products, delta_file)

    log.info("CSV export ALL maken...")
    export(products, all_file)

    log.info("CSV export uitgesloten (met redenen) maken...")
    write_excluded_report(
        excluded_file,
        products,
        price_index=price_index,
        status_index=status_index,
        delta_initial_skus=delta_initial_skus,
        delta_after_images_skus=delta_after_images_skus,
        delta_final_skus=delta_final_skus,
    )

    log.info("ETL run voltooid.")


if __name__ == "__main__":
    main()
