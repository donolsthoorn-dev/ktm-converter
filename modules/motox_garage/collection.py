"""Collectie rasteren op de actieve motor, samen met de Shopify-filters in de URL."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from typing import Callable

import requests

from modules.motox_garage.fitment_index import FitmentIndex

PER_PAGE = 72
_MAX_PAGES = 200
_PAGE_WORKERS = 6
_YMM_CACHE: dict[int, str] = {}


class CollectionFetchError(Exception):
    pass


def normalize_storefront_product(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    try:
        product_id = int(raw.get("id"))
    except (TypeError, ValueError):
        return None
    handle = str(raw.get("handle") or "").strip()
    if not handle:
        return None
    variants = raw.get("variants") if isinstance(raw.get("variants"), list) else []
    available = False
    price = ""
    for variant in variants:
        if not isinstance(variant, dict):
            continue
        if variant.get("available"):
            available = True
            price = str(variant.get("price") or "")
            break
    if not price and variants and isinstance(variants[0], dict):
        price = str(variants[0].get("price") or "")
    images = raw.get("images") if isinstance(raw.get("images"), list) else []
    image = ""
    if images and isinstance(images[0], dict):
        image = str(images[0].get("src") or "")
    tags = raw.get("tags") or []
    if isinstance(tags, str):
        tags = [part.strip() for part in tags.split(",") if part.strip()]
    elif not isinstance(tags, list):
        tags = []
    return {
        "id": product_id,
        "handle": handle,
        "title": str(raw.get("title") or ""),
        "vendor": str(raw.get("vendor") or ""),
        "productType": str(raw.get("product_type") or ""),
        "tags": [str(tag) for tag in tags],
        "price": price,
        "available": available,
        "image": image if image.startswith("https://") else "",
        "url": "/products/" + handle,
    }


def _collection_page(domain: str, handle: str, page: int) -> tuple[int, list[dict]]:
    url = f"https://{domain}/collections/{handle}/products.json?limit=250&page={page}"
    request_headers = {"Accept": "application/json", "User-Agent": "MotoxGarage/1"}
    try:
        response = requests.get(url, timeout=30, headers=request_headers)
    except requests.RequestException as exc:
        raise CollectionFetchError("fetch_failed") from exc
    if response.status_code == 404:
        if page == 1:
            raise CollectionFetchError("not_found")
        return 0, []
    if response.status_code != 200:
        raise CollectionFetchError("fetch_failed")
    try:
        payload = response.json()
    except ValueError as exc:
        raise CollectionFetchError("fetch_failed") from exc
    batch = payload.get("products") if isinstance(payload, dict) else None
    if not isinstance(batch, list) or not batch:
        return 0, []
    items = []
    for raw in batch:
        item = normalize_storefront_product(raw)
        if item:
            items.append(item)
    return len(batch), items


def fetch_collection_products(shop_domain: str, handle: str) -> list[dict]:
    domain = shop_domain.strip().removeprefix("https://").removeprefix("http://").strip("/")
    if not domain:
        raise CollectionFetchError("shop_missing")
    products: list[dict] = []
    page = 1
    while page <= _MAX_PAGES:
        span = min(_PAGE_WORKERS, _MAX_PAGES - page + 1)
        pages = list(range(page, page + span))
        with ThreadPoolExecutor(max_workers=span) as pool:
            batches = list(pool.map(lambda number: _collection_page(domain, handle, number), pages))
        for count, items in batches:
            products.extend(items)
            if count < 250:
                return products
        page += span
    return products


def attach_ymm_summaries(
    shop_domain: str,
    token: str,
    api_version: str,
    products: list[dict],
) -> None:
    """Zet global.ymm_summary op elk product. Faalt stil als de leestoken ontbreekt."""
    for product in products:
        product.setdefault("fits", "")
    domain = shop_domain.strip().removeprefix("https://").removeprefix("http://").strip("/")
    token = token.strip()
    if not domain or not token or not products:
        return
    missing = [product for product in products if int(product["id"]) not in _YMM_CACHE]
    if not missing:
        for product in products:
            product["fits"] = _YMM_CACHE.get(int(product["id"]), "")
        return
    version = api_version.strip() or "2024-10"
    url = f"https://{domain}/admin/api/{version}/graphql.json"
    by_id = {product["id"]: product for product in missing}
    ids = list(by_id)
    for start in range(0, len(ids), 100):
        chunk = ids[start : start + 100]
        query = """
        query MotoxYmm($ids: [ID!]!) {
          nodes(ids: $ids) {
            ... on Product {
              id
              metafield(namespace: "global", key: "ymm_summary") { value }
            }
          }
        }
        """
        try:
            response = requests.post(
                url,
                json={"query": query, "variables": {"ids": [f"gid://shopify/Product/{pid}" for pid in chunk]}},
                headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
                timeout=30,
            )
            payload = response.json()
        except (requests.RequestException, ValueError):
            return
        nodes = ((payload.get("data") or {}).get("nodes")) or []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            gid = str(node.get("id") or "")
            product_id = gid.rsplit("/", 1)[-1]
            if not product_id.isdigit():
                continue
            meta = node.get("metafield") if isinstance(node.get("metafield"), dict) else {}
            text = str((meta or {}).get("value") or "").replace("—", "-").replace("–", "-").strip()
            target = by_id.get(int(product_id))
            if target is None:
                continue
            _YMM_CACHE[int(product_id)] = text
            target["fits"] = text
    for product in products:
        product["fits"] = _YMM_CACHE.get(int(product["id"]), product.get("fits") or "")


def filter_collection(
    products: list[dict],
    index: FitmentIndex,
    vehicle: dict,
    params: dict[str, list[str]],
    *,
    page: int = 1,
    per_page: int = PER_PAGE,
) -> dict:
    allowed = index.allowed_ids([item["id"] for item in products], vehicle)
    chosen = [
        item
        for item in products
        if item["id"] in allowed and item.get("available")
    ]
    ids = [item["id"] for item in products]
    ebike_only = index.ebike_only_ids(ids)
    for item in products:
        if item["id"] in ebike_only:
            item["fits"] = ""
    facets = facet_counts(chosen, params)
    chosen = _apply_storefront_filters(chosen, params)
    chosen = _apply_sort(chosen, _first(params, "sort_by"))
    page_payload = _paginate(chosen, page, per_page)
    page_payload["facets"] = facets
    page_payload["hasFitment"] = index.has_fitted(ids)
    page_payload["hideYmm"] = [item["handle"] for item in products if item["id"] in ebike_only]
    return page_payload


def facet_counts(products: list[dict], params: dict[str, list[str]]) -> dict:
    """Aantallen voor het linkermenu, binnen de motorselectie.

    Een open filter telt zijn eigen keuze niet mee, zodat de andere
    types en merken zichtbaar blijven.
    """
    groups = {
        "filter.p.product_type": "productType",
        "filter.p.vendor": "vendor",
        "filter.p.tag": "tags",
        "filter.v.availability": "available",
    }
    facets: dict = {}
    for param, field in groups.items():
        subset = _apply_storefront_filters(products, _params_without(params, param))
        counts: dict[str, int] = {}
        for product in subset:
            for value in _facet_values(product, field):
                _bump(counts, value)
        facets[param] = counts
    priced = _apply_storefront_filters(products, _params_without(params, "filter.v.price"))
    prices = [_price(product) for product in priced]
    facets["price"] = {
        "min": min(prices) if prices else 0,
        "max": max(prices) if prices else 0,
    }
    return facets


def _params_without(params: dict[str, list[str]], param: str) -> dict[str, list[str]]:
    return {
        key: value
        for key, value in params.items()
        if key != param and not key.startswith(param + ".")
    }


def _facet_values(product: dict, field: str) -> list[str]:
    if field == "available":
        return ["1" if product.get("available") else "0"]
    if field == "tags":
        tags = product.get("tags") or []
        return [str(tag).strip() for tag in tags if str(tag).strip()]
    text = str(product.get(field) or "").strip()
    return [text] if text else []


def _bump(counts: dict[str, int], value: str) -> None:
    folded = value.casefold()
    for key in counts:
        if key.casefold() == folded:
            counts[key] += 1
            return
    counts[value] = 1


def _apply_storefront_filters(products: list[dict], params: dict[str, list[str]]) -> list[dict]:
    vendors = _folded(_values(params, "filter.p.vendor"))
    types = _folded(_values(params, "filter.p.product_type"))
    tags = _folded(_values(params, "filter.p.tag"))
    low = _float(_first(params, "filter.v.price.gte"))
    high = _float(_first(params, "filter.v.price.lte"))
    kept: list[dict] = []
    for product in products:
        if vendors and product["vendor"].casefold() not in vendors:
            continue
        if types and product["productType"].casefold() not in types:
            continue
        if tags:
            have = {tag.casefold() for tag in product.get("tags") or []}
            if not any(tag in have for tag in tags):
                continue
        price = _price(product)
        if low is not None and price < low:
            continue
        if high is not None and price > high:
            continue
        kept.append(product)
    return kept


def _apply_sort(products: list[dict], sort_by: str) -> list[dict]:
    if sort_by == "title-ascending":
        return sorted(products, key=lambda item: item["title"].casefold())
    if sort_by == "title-descending":
        return sorted(products, key=lambda item: item["title"].casefold(), reverse=True)
    if sort_by == "price-ascending":
        return sorted(products, key=_price)
    if sort_by == "price-descending":
        return sorted(products, key=_price, reverse=True)
    return products


def _paginate(products: list[dict], page: int, per_page: int) -> dict:
    per_page = max(1, min(per_page, PER_PAGE))
    total = len(products)
    pages = (total + per_page - 1) // per_page if total else 0
    current = page if page >= 1 else 1
    if pages and current > pages:
        current = pages
    start = (current - 1) * per_page if pages else 0
    visible = products[start : start + per_page] if pages else []
    return {
        "total": total,
        "page": current if pages else 1,
        "pages": pages,
        "perPage": per_page,
        "products": [_public(item) for item in visible],
    }


def _public(product: dict) -> dict:
    return {
        "id": product["id"],
        "title": product["title"],
        "handle": product["handle"],
        "vendor": product["vendor"],
        "url": product["url"],
        "image": product["image"],
        "price": product["price"],
        "fits": product.get("fits") or "",
        "available": bool(product.get("available")),
    }


def _values(params: dict[str, list[str]], key: str) -> list[str]:
    return [value.strip() for value in params.get(key) or [] if value.strip()]


def _first(params: dict[str, list[str]], key: str) -> str:
    values = _values(params, key)
    return values[0] if values else ""


def _folded(values: list[str]) -> set[str]:
    return {value.casefold() for value in values}


def _float(value: str) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _price(product: dict) -> float:
    try:
        return float(product.get("price") or 0)
    except (TypeError, ValueError):
        return 0.0


CollectionLoader = Callable[[str], list[dict]]
