from __future__ import annotations

import json
import time
from typing import Any, Callable

import requests

import config

_REQUEST_TIMEOUT = (15, 120)
_PAGE_SIZE = 80


def _payload_str(payload: dict[str, Any], key: str, default: str) -> str:
    raw = payload.get(key)
    if raw is None:
        return default
    s = str(raw).strip()
    return s if s else default


def _payload_int(payload: dict[str, Any], key: str, default: int) -> int:
    raw = payload.get(key)
    if raw is None:
        return default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return default
    return v


def _payload_bool(payload: dict[str, Any], key: str, default: bool) -> bool:
    raw = payload.get(key)
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    s = str(raw).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _payload_handle_set(payload: dict[str, Any]) -> set[str]:
    raw = payload.get("only_handles")
    if raw is None:
        return set()
    vals: list[str]
    if isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw]
    else:
        vals = [str(raw).strip().lower()]
    return {v for v in vals if v}


def _http_session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.proxies = {"http": None, "https": None}
    return s


def _graphql_url() -> str:
    return f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"


def _graphql(
    sess: requests.Session, query: str, variables: dict[str, Any] | None = None
) -> dict[str, Any]:
    payload: dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    for attempt in range(20):
        r = sess.post(
            _graphql_url(),
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        body = r.json()
        errs = body.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled:
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        if errs:
            raise RuntimeError(json.dumps(errs, ensure_ascii=False)[:2000])
        return body
    raise RuntimeError("GraphQL: te veel THROTTLED-pogingen")


def _split_old_pipe_values(raw: str, max_items: int = 128) -> list[str]:
    vals: list[str] = []
    seen: set[str] = set()
    for part in (raw or "").split("||"):
        v = part.strip().upper()
        if not v or v in seen:
            continue
        seen.add(v)
        vals.append(v)
        if len(vals) >= max_items:
            break
    return vals


def _list_to_mm_value(items: list[str]) -> str:
    # Shopify verwacht voor list.* metafields een JSON-string.
    return json.dumps(items, ensure_ascii=False)


def run_ymm_backfill_missing_fields(
    payload: dict[str, Any] | None = None, log: Callable[[str], None] | None = None
) -> tuple[dict[str, Any], str | None]:
    """
    Backfill missing YMM list metafields from existing legacy pipe-separated metafields.

    Defaults:
      old namespace/key: global/fits_on_make|model|year
      new namespace/key: custom/fits_on_make_new|model_new|year_new
      dry_run: True
      limit: 0 (no limit)

    Optional payload overrides:
      dry_run, limit,
      old_namespace,
      old_make_key, old_model_key, old_year_key,
      new_namespace,
      new_make_key, new_model_key, new_year_key
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    if not config.SHOPIFY_ACCESS_TOKEN:
        return ({}, "SHOPIFY_ACCESS_TOKEN ontbreekt.")

    p = payload or {}
    dry_run = _payload_bool(p, "dry_run", True)
    limit = max(0, _payload_int(p, "limit", 0))

    old_ns = _payload_str(p, "old_namespace", "global")
    old_make_key = _payload_str(p, "old_make_key", "fits_on_make")
    old_model_key = _payload_str(p, "old_model_key", "fits_on_model")
    old_year_key = _payload_str(p, "old_year_key", "fits_on_year")

    new_ns = _payload_str(p, "new_namespace", "custom")
    new_make_key = _payload_str(p, "new_make_key", "fits_on_make_new")
    new_model_key = _payload_str(p, "new_model_key", "fits_on_model_new")
    new_year_key = _payload_str(p, "new_year_key", "fits_on_year_new")
    only_handles = _payload_handle_set(p)

    q_products = f"""
query BackfillProducts($cursor: String) {{
  products(first: {_PAGE_SIZE}, after: $cursor) {{
    pageInfo {{ hasNextPage endCursor }}
    edges {{
      node {{
        id
        legacyResourceId
        handle
        title
        oldMake: metafield(namespace: "{old_ns}", key: "{old_make_key}") {{ value }}
        oldModel: metafield(namespace: "{old_ns}", key: "{old_model_key}") {{ value }}
        oldYear: metafield(namespace: "{old_ns}", key: "{old_year_key}") {{ value }}
        newMake: metafield(namespace: "{new_ns}", key: "{new_make_key}") {{ value }}
        newModel: metafield(namespace: "{new_ns}", key: "{new_model_key}") {{ value }}
        newYear: metafield(namespace: "{new_ns}", key: "{new_year_key}") {{ value }}
      }}
    }}
  }}
}}
""".strip()

    m_set = """
mutation BackfillSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    userErrors {
      field
      message
      code
    }
  }
}
""".strip()

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "scanned_products": 0,
        "candidate_products": 0,
        "updated_products": 0,
        "updated_metafields": 0,
        "pages": 0,
        "limit": limit,
    }

    sess = _http_session()
    cursor: str | None = None

    try:
        while True:
            stats["pages"] += 1
            body = _graphql(sess, q_products, {"cursor": cursor})
            root = ((body.get("data") or {}).get("products") or {})
            page_info = root.get("pageInfo") or {}
            edges = root.get("edges") or []

            for edge in edges:
                node = edge.get("node") or {}
                stats["scanned_products"] += 1

                owner_id = node.get("id")
                if not owner_id:
                    continue
                product_id = str(node.get("legacyResourceId") or "").strip()
                handle = str(node.get("handle") or "").strip()
                title = str(node.get("title") or "").strip()
                if only_handles and handle.lower() not in only_handles:
                    continue

                old_make_raw = ((node.get("oldMake") or {}).get("value") or "").strip()
                old_model_raw = ((node.get("oldModel") or {}).get("value") or "").strip()
                old_year_raw = ((node.get("oldYear") or {}).get("value") or "").strip()
                new_make_raw = ((node.get("newMake") or {}).get("value") or "").strip()
                new_model_raw = ((node.get("newModel") or {}).get("value") or "").strip()
                new_year_raw = ((node.get("newYear") or {}).get("value") or "").strip()

                to_set: list[dict[str, Any]] = []

                if not new_make_raw and old_make_raw:
                    vals = _split_old_pipe_values(old_make_raw, max_items=128)
                    if vals:
                        to_set.append(
                            {
                                "ownerId": owner_id,
                                "namespace": new_ns,
                                "key": new_make_key,
                                "type": "list.single_line_text_field",
                                "value": _list_to_mm_value(vals),
                            }
                        )
                if not new_model_raw and old_model_raw:
                    vals = _split_old_pipe_values(old_model_raw, max_items=128)
                    if vals:
                        to_set.append(
                            {
                                "ownerId": owner_id,
                                "namespace": new_ns,
                                "key": new_model_key,
                                "type": "list.single_line_text_field",
                                "value": _list_to_mm_value(vals),
                            }
                        )
                if not new_year_raw and old_year_raw:
                    vals = _split_old_pipe_values(old_year_raw, max_items=128)
                    if vals:
                        to_set.append(
                            {
                                "ownerId": owner_id,
                                "namespace": new_ns,
                                "key": new_year_key,
                                "type": "list.single_line_text_field",
                                "value": _list_to_mm_value(vals),
                            }
                        )

                if not to_set:
                    continue

                stats["candidate_products"] += 1
                if limit and stats["updated_products"] >= limit:
                    continue

                fields = [f"{m['namespace']}.{m['key']}" for m in to_set]
                _log(
                    "Backfill kandidaat: "
                    f"id={product_id or '?'} handle={handle or '-'} "
                    f"title={title[:80] or '-'} "
                    f"fields={','.join(fields)}"
                )

                if dry_run:
                    stats["updated_products"] += 1
                    stats["updated_metafields"] += len(to_set)
                    continue

                resp = _graphql(sess, m_set, {"metafields": to_set})
                user_errors = (
                    ((resp.get("data") or {}).get("metafieldsSet") or {}).get("userErrors")
                    or []
                )
                if user_errors:
                    raise RuntimeError(json.dumps(user_errors, ensure_ascii=False)[:2000])

                stats["updated_products"] += 1
                stats["updated_metafields"] += len(to_set)
                time.sleep(0.15)

            if limit and stats["updated_products"] >= limit:
                break
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
            time.sleep(0.2)

        mode = "DRY-RUN" if dry_run else "WRITE"
        _log(
            f"YMM backfill klaar ({mode}): scanned={stats['scanned_products']}, "
            f"candidates={stats['candidate_products']}, updated_products={stats['updated_products']}, "
            f"updated_metafields={stats['updated_metafields']}, "
            f"only_handles={len(only_handles)}"
        )
        return (stats, None)
    except (requests.RequestException, RuntimeError, ValueError, KeyError) as e:
        return (stats, str(e)[:4000])
