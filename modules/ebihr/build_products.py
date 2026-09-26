"""Build Shopify valid-products CSV rows from V3 Products + Prices + Stocks."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

from modules.customs_mapping import normalize_country_code, normalize_hs_code
from modules.ebihr.article_numbers import article_payload
from modules.ebihr.constants import (
    EXCLUDED_BRANDS,
    HARDPART_CATEGORY_ALLOWLIST,
    IMAGE_BASE_URL,
)

log = logging.getLogger("ebihr.build_products")


def _iter_json_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    files = sorted(directory.rglob("*.json"))
    return [p for p in files if p.is_file()]


def _load_json(path: Path) -> Any:
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def _as_product_list(data: Any) -> list[dict]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("products", "Products", "items", "Items", "data", "Data"):
        val = data.get(key)
        if isinstance(val, list):
            return [x for x in val if isinstance(x, dict)]
        if isinstance(val, dict):
            # nested list under common keys
            for k2 in ("products", "Products", "items", "Items"):
                if isinstance(val.get(k2), list):
                    return [x for x in val[k2] if isinstance(x, dict)]
    # single product object with partNumber
    if data.get("partNumber") or data.get("PartNumber"):
        return [data]
    return []


def _normalize_product(raw: dict, *, brand: str, group: str, family: str, segment: str, group_code: str, family_code: str, segment_code: str) -> dict:
    """Map V3 catalog PascalCase (+ tree context) to a flat product dict."""
    pics_in = raw.get("Pictures") or raw.get("pictures") or []
    pictures = []
    if isinstance(pics_in, list):
        for p in pics_in:
            if not isinstance(p, dict):
                continue
            url = (p.get("Url") or p.get("url") or "").strip()
            if not url:
                continue
            pictures.append(
                {
                    "url": url,
                    "isDefault": bool(p.get("IsDefault") or p.get("isDefault")),
                }
            )

    attrs_in = raw.get("Attributes") or raw.get("attributes") or []
    attributes = []
    if isinstance(attrs_in, list):
        for a in attrs_in:
            if not isinstance(a, dict):
                continue
            attributes.append(
                {
                    "name": a.get("Name") or a.get("name") or "",
                    "code": a.get("Code") or a.get("code") or "",
                    "value": a.get("Value") if "Value" in a else a.get("value"),
                }
            )

    cats_in = raw.get("SalesCategories") or raw.get("salesCategories") or []
    def _norm_cat(nodes):
        out = []
        if not isinstance(nodes, list):
            return out
        for n in nodes:
            if not isinstance(n, dict):
                continue
            out.append(
                {
                    "code": n.get("Code") or n.get("code") or "",
                    "name": n.get("Name") or n.get("name") or "",
                    "subCategories": _norm_cat(
                        n.get("SubCategories") or n.get("subCategories") or []
                    ),
                }
            )
        return out

    gp = raw.get("GenericProduct") or raw.get("genericProduct")
    generic = None
    if isinstance(gp, dict):
        variations = []
        for v in gp.get("Variations") or gp.get("variations") or []:
            if not isinstance(v, dict):
                continue
            variations.append(
                {
                    "name": v.get("Name") or v.get("name") or "",
                    "code": v.get("Code") or v.get("code") or "",
                    "value": v.get("Value") if "Value" in v else v.get("value"),
                }
            )
        generic = {
            "partNumber": gp.get("PartNumber") or gp.get("partNumber") or "",
            "variations": variations,
        }

    vu = []
    for u in raw.get("VehicleUniverses") or raw.get("vehicleUniverses") or []:
        if isinstance(u, dict):
            vu.append(
                {
                    "code": u.get("Code") or u.get("code") or "",
                    "name": u.get("Name") or u.get("name") or "",
                }
            )

    return {
        "partNumber": str(
            raw.get("PartNumber") or raw.get("partNumber") or ""
        ).strip(),
        "title": str(raw.get("Title") or raw.get("title") or raw.get("Name") or "").strip(),
        "brandName": brand,
        "groupName": group,
        "groupCode": group_code,
        "familyName": family,
        "familyCode": family_code,
        "segmentName": segment,
        "segmentCode": segment_code,
        "htmlDescription": raw.get("HtmlDescription") or raw.get("htmlDescription") or "",
        "barCode": raw.get("BarCode") or raw.get("barCode") or "",
        "oldPartNumber": str(
            raw.get("OldPartNumber") or raw.get("oldPartNumber") or ""
        ).strip(),
        "supplierPartNumber": str(
            raw.get("SupplierPartNumber") or raw.get("supplierPartNumber") or ""
        ).strip(),
        "commodityCode": str(
            raw.get("CommodityCode") or raw.get("commodityCode") or ""
        ).strip(),
        "countryOfOrigin": str(
            raw.get("CountryOfOrigin") or raw.get("countryOfOrigin") or ""
        ).strip(),
        "weight": raw.get("Weight") or raw.get("weight"),
        "pictures": pictures,
        "attributes": attributes,
        "salesCategories": _norm_cat(cats_in),
        "genericProduct": generic,
        "vehicleUniverses": vu,
        "isDiscontinued": bool(raw.get("IsDiscontinued") or raw.get("isDiscontinued")),
    }


def _flatten_catalog_tree(data: dict) -> list[dict]:
    products: list[dict] = []
    for group in data.get("Groups") or data.get("groups") or []:
        if not isinstance(group, dict):
            continue
        g_name = str(group.get("Name") or group.get("name") or "")
        g_code = str(group.get("Code") or group.get("code") or "")
        for family in group.get("Families") or group.get("families") or []:
            if not isinstance(family, dict):
                continue
            f_name = str(family.get("Name") or family.get("name") or "")
            f_code = str(family.get("Code") or family.get("code") or "")
            for segment in family.get("Segments") or family.get("segments") or []:
                if not isinstance(segment, dict):
                    continue
                s_name = str(segment.get("Name") or segment.get("name") or "")
                s_code = str(segment.get("Code") or segment.get("code") or "")
                for brand in segment.get("Brands") or segment.get("brands") or []:
                    if not isinstance(brand, dict):
                        continue
                    b_name = str(brand.get("Name") or brand.get("name") or "")
                    for prod in brand.get("Products") or brand.get("products") or []:
                        if not isinstance(prod, dict):
                            continue
                        # Skip ultra-thin discontinued stubs without description/pictures
                        # still include if they have HtmlDescription or Pictures
                        products.append(
                            _normalize_product(
                                prod,
                                brand=b_name,
                                group=g_name,
                                family=f_name,
                                segment=s_name,
                                group_code=g_code,
                                family_code=f_code,
                                segment_code=s_code,
                            )
                        )
    return products


def load_v3_products(raw_dir: Path) -> list[dict]:
    base = raw_dir / "v3_Products"
    files = _iter_json_files(base if base.is_dir() else raw_dir)
    products: list[dict] = []
    for path in files:
        if "Price" in path.name or "Stock" in path.name:
            continue
        log.info("Laden products JSON %s (%.1f MB)...", path.name, path.stat().st_size / 1e6)
        try:
            data = _load_json(path)
        except json.JSONDecodeError as e:
            log.warning("Skip JSON %s: %s", path, e)
            continue
        if isinstance(data, dict) and (data.get("Groups") or data.get("groups")):
            chunk = _flatten_catalog_tree(data)
        else:
            chunk = [_normalize_product(p, brand="", group="", family="", segment="", group_code="", family_code="", segment_code="") for p in _as_product_list(data)]
        log.info("Products uit %s: %d", path.name, len(chunk))
        products.extend(chunk)
    if not products:
        raise FileNotFoundError(f"Geen V3 Products JSON onder {raw_dir}")
    # Prefer richer duplicates (same partNumber): keep one with html/pictures
    by_pn: dict[str, dict] = {}
    for p in products:
        pn = p.get("partNumber") or ""
        if not pn:
            continue
        prev = by_pn.get(pn)
        if prev is None:
            by_pn[pn] = p
            continue
        score = (1 if p.get("htmlDescription") else 0) + (1 if p.get("pictures") else 0)
        prev_score = (1 if prev.get("htmlDescription") else 0) + (1 if prev.get("pictures") else 0)
        if score >= prev_score:
            by_pn[pn] = p
    out = list(by_pn.values())
    log.info("Totaal V3 products (uniek PartNumber): %d", len(out))
    return out


def _fmt_price(val) -> str:
    if val is None or val == "":
        return ""
    try:
        return f"{float(val):.2f}"
    except (TypeError, ValueError):
        return str(val)


def load_v3_prices(raw_dir: Path) -> dict[str, dict]:
    base = raw_dir / "v3_Prices"
    files = _iter_json_files(base if base.is_dir() else raw_dir)
    prices: dict[str, dict] = {}
    for path in files:
        try:
            data = _load_json(path)
        except json.JSONDecodeError:
            continue
        chunk: list[dict] = []
        if isinstance(data, dict) and isinstance(data.get("Prices") or data.get("prices"), list):
            chunk = [x for x in (data.get("Prices") or data.get("prices")) if isinstance(x, dict)]
        else:
            chunk = _as_product_list(data)
        for row in chunk:
            pn = str(row.get("PartNumber") or row.get("partNumber") or "").strip()
            if not pn:
                continue
            prices[pn] = {
                "partNumber": pn,
                "RecommendedRetailPriceInclTaxes": row.get("RecommendedRetailPriceInclTaxes")
                or row.get("recommendedRetailPriceInclTaxes"),
                "DealerPriceExclTaxes": row.get("DealerPriceExclTaxes")
                or row.get("dealerPriceExclTaxes"),
                "RecommendedRetailPriceExclTaxes": row.get("RecommendedRetailPriceExclTaxes")
                or row.get("recommendedRetailPriceExclTaxes"),
                **{k: v for k, v in row.items()},
            }
        if chunk:
            log.info("Prices uit %s: %d", path.name, len(chunk))
    log.info("Totaal price rows: %d", len(prices))
    return prices


def load_v3_stocks(raw_dir: Path) -> dict[str, dict]:
    base = raw_dir / "v3_Stocks"
    files = _iter_json_files(base if base.is_dir() else raw_dir)
    stocks: dict[str, dict] = {}
    for path in files:
        try:
            data = _load_json(path)
        except json.JSONDecodeError:
            continue
        chunk: list[dict] = []
        if isinstance(data, dict) and isinstance(data.get("Stocks") or data.get("stocks"), list):
            chunk = [x for x in (data.get("Stocks") or data.get("stocks")) if isinstance(x, dict)]
        else:
            chunk = _as_product_list(data)
        for row in chunk:
            pn = str(row.get("PartNumber") or row.get("partNumber") or "").strip()
            if not pn:
                continue
            stocks[pn] = {
                "partNumber": pn,
                "Value": row.get("Value") if "Value" in row else row.get("value"),
                **{k: v for k, v in row.items()},
            }
        if chunk:
            log.info("Stocks uit %s: %d", path.name, len(chunk))
    log.info("Totaal stock rows: %d", len(stocks))
    return stocks


def _part_number(product: dict) -> str:
    return str(
        product.get("partNumber")
        or product.get("PartNumber")
        or product.get("newPartNumber")
        or product.get("NewPartNumber")
        or ""
    ).strip()


def _brand(product: dict) -> str:
    return str(
        product.get("brandName")
        or product.get("BrandName")
        or product.get("Brand")
        or ""
    ).strip()


def _title(product: dict) -> str:
    t = product.get("title") or product.get("Title") or product.get("name") or product.get("Name")
    if isinstance(t, dict):
        t = t.get("nl") or t.get("en") or next(iter(t.values()), "")
    return str(t or "").strip()


def _html_description(product: dict) -> str:
    d = product.get("htmlDescription") or product.get("HtmlDescription") or ""
    if isinstance(d, dict):
        d = d.get("nl") or d.get("en") or next(iter(d.values()), "")
    return str(d or "").strip()


def _picture_urls(product: dict) -> list[str]:
    pics = product.get("pictures") or product.get("Pictures") or []
    urls: list[str] = []
    if not isinstance(pics, list):
        return urls
    # prefer default first
    ordered = sorted(
        [p for p in pics if isinstance(p, dict)],
        key=lambda p: (0 if p.get("isDefault") or p.get("IsDefault") else 1),
    )
    for p in ordered:
        url = (p.get("url") or p.get("Url") or "").strip()
        if not url:
            continue
        if url.startswith("/"):
            url = f"{IMAGE_BASE_URL}{url}"
        elif not url.startswith("http"):
            url = f"{IMAGE_BASE_URL}/{url.lstrip('/')}"
        if url not in urls:
            urls.append(url)
    return urls


def _attributes_map(product: dict) -> dict[str, str]:
    attrs = product.get("attributes") or product.get("Attributes") or []
    out: dict[str, str] = {}
    if isinstance(attrs, dict):
        for k, v in attrs.items():
            if v is None or v == "":
                continue
            out[str(k)] = str(v)
        return out
    if isinstance(attrs, list):
        for a in attrs:
            if not isinstance(a, dict):
                continue
            name = (a.get("name") or a.get("Name") or a.get("code") or "").strip()
            val = a.get("value") if "value" in a else a.get("Value")
            if name and val is not None and str(val) != "":
                out[name] = str(val)
    return out


def _sales_category_names(product: dict) -> list[str]:
    cats = product.get("salesCategories") or product.get("SalesCategories") or []
    names: list[str] = []

    def walk(nodes):
        if not isinstance(nodes, list):
            return
        for n in nodes:
            if not isinstance(n, dict):
                continue
            name = (n.get("name") or n.get("Name") or "").strip()
            if name:
                names.append(name)
            walk(n.get("subCategories") or n.get("SubCategories") or [])

    walk(cats)
    return names


def _main_category(product: dict) -> str:
    names = _sales_category_names(product)
    return names[0] if names else str(product.get("familyName") or product.get("FamilyName") or "").strip()


def _category_path(product: dict) -> str:
    names = _sales_category_names(product)
    if names:
        return " > ".join(names[:4])
    parts = [
        str(product.get("groupName") or product.get("GroupName") or "").strip(),
        str(product.get("familyName") or product.get("FamilyName") or "").strip(),
        str(product.get("segmentName") or product.get("SegmentName") or "").strip(),
    ]
    return " > ".join(p for p in parts if p)


def _is_hardpart(product: dict) -> bool:
    blob = " ".join(
        [
            str(product.get("groupName") or ""),
            str(product.get("groupCode") or ""),
            str(product.get("familyName") or ""),
            str(product.get("familyCode") or ""),
            str(product.get("segmentName") or ""),
            str(product.get("segmentCode") or ""),
        ]
    ).upper()
    # V3 catalog group "A" = RIDER GEAR, typically "B" = HARD PARTS
    gcode = str(product.get("groupCode") or "").upper()
    gname = str(product.get("groupName") or "").upper()
    if gcode == "A" or "RIDER" in gname:
        return False
    if gcode == "B" or ("HARD" in gname and "PART" in gname):
        return True
    if "HARDPART" in blob.replace(" ", ""):
        return True
    if any(x in blob for x in ("RIDER", "GEAR", "APPAREL", "HELMET", "BOOT", "GLOVE")):
        return False
    if any(x in blob for x in ("PART", "OEM", "SPARE", "TECHNICAL")):
        return True
    vu = product.get("vehicleUniverses") or []
    if vu and not any(x in blob for x in ("RIDER", "GEAR")):
        return True
    return False


def _generic_partnumber(product: dict) -> str | None:
    gp = product.get("genericProduct") or product.get("GenericProduct")
    if isinstance(gp, dict):
        pn = (gp.get("partNumber") or gp.get("PartNumber") or "").strip()
        return pn or None
    return None


def _variation_attrs(product: dict) -> dict[str, str]:
    """Map option name → value from genericProduct.variations."""
    gp = product.get("genericProduct") or product.get("GenericProduct") or {}
    variations = []
    if isinstance(gp, dict):
        variations = gp.get("variations") or gp.get("Variations") or []
    out: dict[str, str] = {}
    if isinstance(variations, list):
        for v in variations:
            if not isinstance(v, dict):
                continue
            name = (v.get("name") or v.get("Name") or "").strip()
            val = v.get("value") if "value" in v else v.get("Value")
            if name and val is not None and str(val) != "":
                out[name] = str(val)
    # also fold attributes that look like options
    attrs = _attributes_map(product)
    for k, v in attrs.items():
        if k not in out:
            out[k] = v
    return out


def _price_incl(price_row: dict | None, product: dict) -> str:
    if price_row:
        for k in (
            "RecommendedRetailPriceInclTaxes",
            "recommendedRetailPriceInclTaxes",
            "retailPriceIncludingTax",
            "RetailPriceIncludingTax",
            "rrpIncludingTax",
            "RrpIncludingTax",
            "priceIncludingTax",
            "PriceIncludingTax",
        ):
            if price_row.get(k) is not None and str(price_row.get(k)) != "":
                return _fmt_price(price_row[k])
        for nest in ("retailPrice", "RetailPrice", "recommendedRetailPrice", "price", "Price"):
            obj = price_row.get(nest)
            if isinstance(obj, dict):
                for k in ("includingTax", "IncludingTax", "value", "Value", "amount", "Amount"):
                    if obj.get(k) is not None:
                        return _fmt_price(obj[k])
    for k in ("retailPriceIncludingTax", "RetailPriceIncludingTax"):
        if product.get(k) is not None:
            return _fmt_price(product[k])
    return ""


def _stock_policy(stock_row: dict | None, product: dict) -> str:
    if stock_row is not None:
        val = stock_row.get("Value")
        if val is None:
            val = stock_row.get("value")
        if val is not None:
            try:
                return "continue" if float(val) > 0 else "deny"
            except (TypeError, ValueError):
                pass
        level = str(
            stock_row.get("stockLevel")
            or stock_row.get("StockLevel")
            or stock_row.get("availability")
            or stock_row.get("Availability")
            or ""
        )
        if level.lower() in ("outofstock", "out_of_stock", "unavailable"):
            return "deny"
        if level:
            return "continue"
    level = str(product.get("stockLevel") or product.get("StockLevel") or "")
    if level.lower() in ("outofstock", "out_of_stock", "unavailable"):
        return "deny"
    return "continue"


def _qty(product: dict, stock_row: dict | None) -> str:
    if stock_row is not None:
        val = stock_row.get("Value")
        if val is None:
            val = stock_row.get("value")
        if val is not None and str(val) != "":
            return str(int(float(val))) if str(val).replace(".", "", 1).isdigit() or isinstance(val, (int, float)) else str(val)
    for src in (stock_row or {}, product):
        for k in ("salesMultiple", "SalesMultiple", "quantity", "Quantity", "availableQuantity"):
            if src.get(k) is not None and str(src.get(k)) != "":
                return str(src[k])
    return "0"


def _weight_kg(product: dict) -> str:
    w = product.get("weight") or product.get("Weight")
    try:
        wf = float(w)
    except (TypeError, ValueError):
        return "0.5"
    if wf <= 0:
        return "0.5"
    # V3 weight often already kg; old API used grams. Heuristic: > 50 → grams.
    if wf > 50:
        return str(wf / 1000.0)
    return str(wf)


def _barcode(product: dict) -> str:
    return str(product.get("barCode") or product.get("BarCode") or product.get("barcode") or "").strip()


def _should_include(
    product: dict,
    *,
    offroad_partnumbers: set[str],
) -> bool:
    brand = _brand(product).lower()
    if brand in EXCLUDED_BRANDS:
        return False
    pn = _part_number(product)
    if not pn:
        return False
    if not _is_hardpart(product):
        return True
    # Hardparts: OFFROAD via VSE or allowlisted categories
    if pn in offroad_partnumbers or pn[:7] in {x[:7] for x in offroad_partnumbers if len(x) >= 7}:
        return True
    main = _main_category(product)
    if main in HARDPART_CATEGORY_ALLOWLIST:
        return True
    # also match allowlist as substring in category path
    path = _category_path(product)
    for allowed in HARDPART_CATEGORY_ALLOWLIST:
        if allowed in path:
            return True
    return False


def _group_key(product: dict) -> str:
    gp = _generic_partnumber(product)
    pn = _part_number(product)
    if gp:
        return gp[:7] if len(gp) >= 7 else gp
    return pn[:7] if len(pn) >= 7 else pn


def _unique_option_names(variants: list[dict]) -> list[str]:
    """
    Option axes for a product group.

    Keep a name when values differ across variants, OR when it is only present
    on some siblings (others empty). Dropping those axes left multi-SKU groups
    with no options → Shopify Title/Default Title collisions on create.
    """
    unique: dict[str, set[str]] = {}
    present: dict[str, int] = {}
    for var in variants:
        attrs = _variation_attrs(var)
        for name, val in attrs.items():
            unique.setdefault(name, set()).add(val)
            present[name] = present.get(name, 0) + 1
    n = len(variants)
    names = [
        name
        for name, vals in unique.items()
        if len(vals) > 1 or (n > 1 and 0 < present.get(name, 0) < n)
    ]
    for color_key in ("Kleur", "Kleuren", "Color", "Colour"):
        if color_key in names:
            names.insert(0, names.pop(names.index(color_key)))
            break
    return names[:3]


def _empty_row() -> list[str]:
    return [""] * 47


def _product_to_csv_rows(group_id: str, variants: list[dict], prices: dict, stocks: dict) -> list[list[str]]:
    if not variants:
        return []
    first = variants[0]
    title = _title(first)
    # prefer longest/non-empty title among variants
    for v in variants:
        t = _title(v)
        if t and (not title or len(t) > len(title)):
            title = t
    brand = _brand(first)
    body = _html_description(first)
    ptype = _category_path(first) or _main_category(first)
    option_names = _unique_option_names(variants)

    all_images: list[str] = []
    for v in variants:
        for url in _picture_urls(v):
            if url not in all_images:
                all_images.append(url)

    published = (
        "TRUE"
        if brand.lower() != "leatt" and len(all_images) > 0
        else "FALSE"
    )

    rows: list[list[str]] = []
    for i, var in enumerate(variants):
        pn = _part_number(var)
        price = _price_incl(prices.get(pn), var)
        policy = _stock_policy(stocks.get(pn), var)
        qty = _qty(var, stocks.get(pn))
        attrs = _variation_attrs(var)
        o1 = attrs.get(option_names[0], "") if option_names else ""
        o2 = attrs.get(option_names[1], "") if len(option_names) > 1 else ""
        o3 = attrs.get(option_names[2], "") if len(option_names) > 2 else ""
        var_images = _picture_urls(var)
        row = _empty_row()
        row[0] = group_id  # Handle
        if i == 0:
            row[1] = title
            row[2] = body
            row[3] = brand
            row[4] = ptype
            row[5] = ""  # Tags
            row[6] = published
            row[7] = option_names[0] if option_names else ""
            row[9] = option_names[1] if len(option_names) > 1 else ""
            row[11] = option_names[2] if len(option_names) > 2 else ""
        row[8] = o1
        row[10] = o2
        row[12] = o3
        row[13] = pn  # Variant SKU
        row[14] = _weight_kg(var)
        row[15] = "shopify"
        row[16] = qty
        row[17] = policy
        row[18] = "manual"
        row[19] = price
        row[21] = "TRUE"  # Requires Shipping
        row[22] = "TRUE"  # Taxable
        row[23] = _barcode(var)
        if i == 0 and all_images:
            row[24] = all_images[0]
            row[25] = "1"
        elif var_images:
            row[24] = var_images[0]
            row[25] = str(i + 1)
        row[43] = var_images[0] if var_images else ""  # Variant Image
        row[44] = "kg"
        rows.append(row)

    # Extra image rows (Shopify style): handle + image only
    for pos, url in enumerate(all_images[1:], start=2):
        row = _empty_row()
        row[0] = group_id
        row[24] = url
        row[25] = str(pos)
        rows.append(row)

    return rows


def build_product_groups(
    raw_dir: Path,
    *,
    offroad_partnumbers: set[str] | None = None,
) -> tuple[list[dict], dict, dict, dict]:
    """
    Returns (groups, prices, stocks, stats).

    Each group dict:
      handle, title, vendor, body_html, product_type, published,
      option_names, images, variants: [{sku, barcode, price, inventory_policy,
      qty, grams, option_values, images}]
    """
    offroad = offroad_partnumbers or set()
    products = load_v3_products(raw_dir)
    prices = load_v3_prices(raw_dir)
    stocks = load_v3_stocks(raw_dir)

    grouped: dict[str, list[dict]] = defaultdict(list)
    skipped_brand = 0
    skipped_hardpart = 0
    skipped_no_pn = 0

    for product in products:
        pn = _part_number(product)
        if not pn:
            skipped_no_pn += 1
            continue
        if not _title(product) and not _picture_urls(product) and not _html_description(product):
            skipped_no_pn += 1
            continue
        brand = _brand(product).lower()
        if brand in EXCLUDED_BRANDS:
            skipped_brand += 1
            continue
        if not _should_include(product, offroad_partnumbers=offroad):
            skipped_hardpart += 1
            continue
        grouped[_group_key(product)].append(product)

    groups: list[dict] = []
    for group_id in sorted(grouped.keys()):
        variants_raw = grouped[group_id]
        variants_raw.sort(key=_part_number)
        first = variants_raw[0]
        title = _title(first)
        for v in variants_raw:
            t = _title(v)
            if t and (not title or len(t) > len(title)):
                title = t
        if not title:
            title = f"Product {group_id}"
        brand = _brand(first)
        body = _html_description(first)
        ptype = _category_path(first) or _main_category(first)
        option_names = _unique_option_names(variants_raw)
        all_images: list[str] = []
        for v in variants_raw:
            for url in _picture_urls(v):
                if url not in all_images:
                    all_images.append(url)
        published = brand.lower() != "leatt" and len(all_images) > 0
        variants_out = []
        for var in variants_raw:
            pn = _part_number(var)
            attrs = _variation_attrs(var)
            option_values = [attrs.get(n, "") for n in option_names]
            variants_out.append(
                {
                    "sku": pn,
                    "barcode": _barcode(var),
                    "article_numbers": article_payload(
                        pn,
                        var.get("oldPartNumber"),
                        var.get("supplierPartNumber"),
                    ),
                    "hs_code": normalize_hs_code(var.get("commodityCode")) or "",
                    "country": normalize_country_code(var.get("countryOfOrigin")) or "",
                    "price": _price_incl(prices.get(pn), var),
                    "cost": _fmt_price(
                        (prices.get(pn) or {}).get("DealerPriceExclTaxes")
                    ),
                    "inventory_policy": _stock_policy(stocks.get(pn), var),
                    "qty": _qty(var, stocks.get(pn)),
                    "grams": _weight_kg(var),
                    "option_values": option_values,
                    "images": _picture_urls(var),
                }
            )
        groups.append(
            {
                "handle": group_id,
                "title": title,
                "vendor": brand,
                "body_html": body,
                "product_type": ptype,
                "published": published,
                "option_names": option_names,
                "images": all_images,
                "variants": variants_out,
            }
        )

    stats = {
        "source_products": len(products),
        "groups": len(groups),
        "skipped_brand": skipped_brand,
        "skipped_hardpart_filter": skipped_hardpart,
        "skipped_no_partnumber": skipped_no_pn,
        "prices_indexed": len(prices),
        "stocks_indexed": len(stocks),
        "offroad_skus": len(offroad),
    }
    log.info("Product groups: %s", stats)
    return groups, prices, stocks, stats


def build_product_csv_rows(
    raw_dir: Path,
    *,
    offroad_partnumbers: set[str] | None = None,
) -> tuple[list[list[str]], dict]:
    """Returns (csv_rows, stats)."""
    groups, prices, stocks, stats = build_product_groups(
        raw_dir, offroad_partnumbers=offroad_partnumbers
    )
    # Rebuild raw variant lists for CSV helper via groups
    rows: list[list[str]] = []
    # Use group data directly to CSV
    for g in groups:
        # synthesize minimal product dicts for existing CSV writer path
        # Faster: write from group structure
        option_names = g["option_names"]
        rows.extend(_group_to_csv_rows(g, option_names))
    stats = {**stats, "csv_rows": len(rows)}
    return rows, stats


def _group_to_csv_rows(g: dict, option_names: list[str]) -> list[list[str]]:
    rows: list[list[str]] = []
    all_images = g.get("images") or []
    published = "TRUE" if g.get("published") else "FALSE"
    for i, var in enumerate(g.get("variants") or []):
        row = _empty_row()
        row[0] = g["handle"]
        if i == 0:
            row[1] = g.get("title") or ""
            row[2] = g.get("body_html") or ""
            row[3] = g.get("vendor") or ""
            row[4] = g.get("product_type") or ""
            row[6] = published
            row[7] = option_names[0] if option_names else ""
            row[9] = option_names[1] if len(option_names) > 1 else ""
            row[11] = option_names[2] if len(option_names) > 2 else ""
        ovals = var.get("option_values") or []
        row[8] = ovals[0] if ovals else ""
        row[10] = ovals[1] if len(ovals) > 1 else ""
        row[12] = ovals[2] if len(ovals) > 2 else ""
        row[13] = var.get("sku") or ""
        row[14] = var.get("grams") or "0.5"
        row[15] = "shopify"
        row[16] = var.get("qty") or "0"
        row[17] = var.get("inventory_policy") or "continue"
        row[18] = "manual"
        row[19] = var.get("price") or ""
        row[21] = "TRUE"
        row[22] = "TRUE"
        row[23] = var.get("barcode") or ""
        var_images = var.get("images") or []
        if i == 0 and all_images:
            row[24] = all_images[0]
            row[25] = "1"
        elif var_images:
            row[24] = var_images[0]
            row[25] = str(i + 1)
        row[43] = var_images[0] if var_images else ""
        row[44] = "kg"
        rows.append(row)
    for pos, url in enumerate(all_images[1:], start=2):
        row = _empty_row()
        row[0] = g["handle"]
        row[24] = url
        row[25] = str(pos)
        rows.append(row)
    return rows
