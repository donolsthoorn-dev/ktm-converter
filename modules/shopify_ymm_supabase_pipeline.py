from __future__ import annotations

import json
import time
from typing import Any, Callable

import requests

import config

_REQUEST_TIMEOUT = (15, 120)

# Shopify list.* metafields: max 128 list items (Admin API validation).
SHOPIFY_LIST_METAFIELD_MAX_ITEMS = 128
# single_line_text_field: stay onder gangbare limiet (~5k).
SHOPIFY_SINGLE_LINE_MAX_CHARS = 5000


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


def _payload_int(payload: dict[str, Any], key: str, default: int) -> int:
    raw = payload.get(key)
    if raw is None:
        return default
    try:
        v = int(raw)
    except (TypeError, ValueError):
        return default
    return v


def _payload_handles(payload: dict[str, Any]) -> set[str]:
    raw = payload.get("only_handles")
    if raw is None:
        return set()
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


def run_refresh_shopify_ymm_projection(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    r = supabase_sess.post(
        f"{rest_base}/rpc/refresh_shopify_ymm_projection",
        headers=supabase_headers,
        json={},
        timeout=_REQUEST_TIMEOUT,
    )
    if not r.ok:
        return ({}, f"Supabase RPC refresh_shopify_ymm_projection failed: {r.text[:1500]}")
    body = r.json()
    stats = body if isinstance(body, dict) else {"result": body}
    _log(f"YMM projection refresh klaar: {json.dumps(stats, ensure_ascii=False)}")
    return (stats, None)


def _fetch_projection_rows(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    limit: int,
    product_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    if product_ids:
        ids = ",".join(str(int(x)) for x in product_ids)
        r = supabase_sess.get(
            f"{rest_base}/shopify_ymm_projection",
            headers=supabase_headers,
            params={
                "select": "shopify_product_id,sku,fits_on_make_old,fits_on_model_old,fits_on_year_old,fits_on_make_new,fits_on_model_new,fits_on_year_new,ymm_summary",
                "shopify_product_id": f"in.({ids})",
                "order": "shopify_product_id.asc",
                "limit": str(max(1, limit) if limit > 0 else max(len(product_ids), 1)),
            },
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        return r.json()

    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 500
    while True:
        batch_limit = page_size
        if limit > 0:
            batch_limit = min(batch_limit, max(limit - len(rows), 0))
            if batch_limit <= 0:
                break
        r = supabase_sess.get(
            f"{rest_base}/shopify_ymm_projection",
            headers=supabase_headers,
            params={
                "select": "shopify_product_id,sku,fits_on_make_old,fits_on_model_old,fits_on_year_old,fits_on_make_new,fits_on_model_new,fits_on_year_new,ymm_summary",
                "order": "shopify_product_id.asc",
                "limit": str(batch_limit),
                "offset": str(offset),
            },
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        rows.extend(batch)
        offset += len(batch)
        if len(batch) < batch_limit:
            break
    return rows


def ymm_json_to_tuples(ymm_json: dict[str, Any]) -> set[tuple[str, str, str]]:
    out: set[tuple[str, str, str]] = set()
    if not isinstance(ymm_json, dict):
        return out
    for make, models in ymm_json.items():
        if str(make).startswith("_"):
            continue
        if not isinstance(models, dict):
            continue
        for model, years in models.items():
            if not isinstance(years, list):
                continue
            for year in years:
                y = str(year or "").strip()
                if y:
                    out.add((str(make).strip(), str(model).strip(), y))
    return out


def _truncate_pipe_value(value: str, max_chars: int = SHOPIFY_SINGLE_LINE_MAX_CHARS) -> str:
    if len(value) <= max_chars:
        return value
    cut = value[:max_chars]
    if "||" in cut:
        cut = cut.rsplit("||", 1)[0]
    return cut


def _cap_list_items(items: list[str], max_items: int = SHOPIFY_LIST_METAFIELD_MAX_ITEMS) -> tuple[list[str], bool]:
    if len(items) <= max_items:
        return items, False
    return items[:max_items], True


def flat_columns_from_ymm_json(ymm_json: dict[str, Any]) -> dict[str, Any]:
    """Platte Metafields-kolommen + ymm_summary uit fits_on JSON."""
    from modules.metafields_manager_export import _pipe_join_sorted, _ymm_summary

    tuples = ymm_json_to_tuples(ymm_json)
    if not tuples:
        return {}
    makes = {t[0].upper() for t in tuples}
    models = {t[1].upper() for t in tuples}
    years = {t[2].upper() for t in tuples}
    return {
        "fits_on_make_old": _pipe_join_sorted(makes),
        "fits_on_model_old": _pipe_join_sorted(models),
        "fits_on_year_old": _pipe_join_sorted(years),
        "fits_on_make_new": sorted(makes),
        "fits_on_model_new": sorted(models),
        "fits_on_year_new": sorted(years),
        "ymm_summary": _ymm_summary(tuples),
        "_counts": {"makes": len(makes), "models": len(models), "years": len(years)},
    }


def flat_columns_for_shopify_push(ymm_json: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    """
    Platte kolommen binnen Shopify-limieten.
    Volledige fitment blijft in fits_on JSON; lijst *_new max 128 items.
    """
    flat = flat_columns_from_ymm_json(ymm_json)
    if not flat:
        return {}, []
    warnings: list[str] = []
    counts = flat.pop("_counts", {}) or {}

    for key in ("fits_on_make_old", "fits_on_model_old", "fits_on_year_old", "ymm_summary"):
        if flat.get(key):
            before = flat[key]
            flat[key] = _truncate_pipe_value(before)
            if len(flat[key]) < len(before):
                warnings.append(f"{key} ingekort tot {len(flat[key])} tekens")

    for list_key in ("fits_on_make_new", "fits_on_model_new", "fits_on_year_new"):
        raw = flat.get(list_key) or []
        capped, truncated = _cap_list_items(raw)
        flat[list_key] = capped
        if truncated:
            warnings.append(
                f"{list_key}: {len(raw)} waarden → {len(capped)} "
                f"(Shopify list-limiet; volledige set in fits_on JSON)"
            )

    return flat, warnings


CANONICAL_FITS_ON_TABLE = "canonical_product_fits_on"


def _fetch_canonical_fits_on_rows(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    *,
    product_ids: list[int] | None,
    limit: int,
    only_diff: bool,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    offset = 0
    page_size = 200
    while True:
        batch_limit = page_size
        if limit > 0:
            batch_limit = min(batch_limit, max(limit - len(rows), 0))
            if batch_limit <= 0:
                break
        params: dict[str, str] = {
            "select": "shopify_product_id,ymm_json,content_hash,pushed_hash",
            "ymm_json": "not.is.null",
            "order": "shopify_product_id.asc",
            "limit": str(batch_limit),
            "offset": str(offset),
        }
        if product_ids:
            params["shopify_product_id"] = f"in.({','.join(str(int(x)) for x in product_ids)})"
        r = supabase_sess.get(
            f"{rest_base}/{CANONICAL_FITS_ON_TABLE}",
            headers=supabase_headers,
            params=params,
            timeout=_REQUEST_TIMEOUT,
        )
        r.raise_for_status()
        batch = r.json()
        if not batch:
            break
        pid_list = [int(row["shopify_product_id"]) for row in batch if row.get("shopify_product_id")]
        handle_by_pid = _handles_for_product_ids(
            supabase_sess, rest_base, supabase_headers, pid_list
        )
        from modules.ymm_content_hash import needs_shopify_push

        for row in batch:
            ymm = row.get("ymm_json")
            if not isinstance(ymm, dict) or not ymm:
                continue
            content_hash = str(row.get("content_hash") or "").strip()
            pushed_hash = row.get("pushed_hash")
            if only_diff and not needs_shopify_push(content_hash, pushed_hash):
                continue
            try:
                pid = int(row["shopify_product_id"])
            except (TypeError, ValueError, KeyError):
                continue
            rows.append(
                {
                    "shopify_product_id": pid,
                    "ymm_json": ymm,
                    "content_hash": content_hash,
                    "handle": handle_by_pid.get(pid, ""),
                }
            )
            if limit > 0 and len(rows) >= limit:
                return rows
        offset += len(batch)
        if len(batch) < batch_limit:
            break
    return rows


def _mark_canonical_pushed(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    shopify_product_id: int,
    content_hash: str,
) -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    r = supabase_sess.patch(
        f"{rest_base}/{CANONICAL_FITS_ON_TABLE}",
        headers=supabase_headers,
        params={"shopify_product_id": f"eq.{shopify_product_id}"},
        json={
            "pushed_hash": content_hash,
            "pushed_at": now,
            "updated_at": now,
        },
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()


def _handles_for_product_ids(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    product_ids: list[int],
) -> dict[int, str]:
    if not product_ids:
        return {}
    ids = ",".join(str(int(x)) for x in product_ids)
    r = supabase_sess.get(
        f"{rest_base}/shopify_products",
        headers=supabase_headers,
        params={
            "select": "shopify_product_id,handle",
            "shopify_product_id": f"in.({ids})",
            "limit": str(len(product_ids) + 10),
        },
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    out: dict[int, str] = {}
    for row in r.json():
        try:
            pid = int(row["shopify_product_id"])
        except (TypeError, ValueError, KeyError):
            continue
        out[pid] = (row.get("handle") or "").strip().lower()
    return out


def _resolve_product_ids_for_handles(
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    handles: set[str],
) -> list[int]:
    if not handles:
        return []
    quoted = ",".join([f"\"{h}\"" for h in sorted(handles)])
    r = supabase_sess.get(
        f"{rest_base}/shopify_products",
        headers=supabase_headers,
        params={
            "select": "shopify_product_id,handle",
            "handle": f"in.({quoted})",
            "limit": "5000",
        },
        timeout=_REQUEST_TIMEOUT,
    )
    r.raise_for_status()
    out: list[int] = []
    seen: set[int] = set()
    for row in r.json():
        try:
            pid = int(row.get("shopify_product_id"))
        except (TypeError, ValueError):
            continue
        if pid in seen:
            continue
        seen.add(pid)
        out.append(pid)
    return out


def run_shopify_ymm_backfill_from_supabase(
    payload: dict[str, Any],
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Write missing Shopify YMM metafields based on public.shopify_ymm_projection.
    """

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    if not config.SHOPIFY_ACCESS_TOKEN:
        return ({}, "SHOPIFY_ACCESS_TOKEN ontbreekt.")

    dry_run = _payload_bool(payload, "dry_run", True)
    limit = max(0, _payload_int(payload, "limit", 0))
    only_handles = _payload_handles(payload)
    progress_every = max(1, _payload_int(payload, "progress_every", 250))

    old_ns = "global"
    new_ns = "custom"
    old_make_key = "fits_on_make"
    old_model_key = "fits_on_model"
    old_year_key = "fits_on_year"
    new_make_key = "fits_on_make_new"
    new_model_key = "fits_on_model_new"
    new_year_key = "fits_on_year_new"
    summary_key = "ymm_summary"

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "scanned_rows": 0,
        "candidate_products": 0,
        "updated_products": 0,
        "updated_metafields": 0,
        "total_rows_loaded": 0,
        "progress_every": progress_every,
    }

    try:
        target_product_ids = _resolve_product_ids_for_handles(
            supabase_sess, rest_base, supabase_headers, only_handles
        )
        rows = _fetch_projection_rows(
            supabase_sess,
            rest_base,
            supabase_headers,
            limit=limit,
            product_ids=target_product_ids if only_handles else None,
        )
    except requests.RequestException as e:
        return (stats, f"Supabase read shopify_ymm_projection failed: {str(e)[:1000]}")

    stats["total_rows_loaded"] = len(rows)
    mode = "DRY-RUN" if dry_run else "WRITE"
    _log(
        f"Supabase YMM backfill start ({mode}): total_rows={stats['total_rows_loaded']}, "
        f"limit={limit}, only_handles={len(only_handles)}"
    )

    shop_sess = _http_session()

    q_existing = f"""
query ExistingYmm($id: ID!) {{
  product(id: $id) {{
    handle
    oldMake: metafield(namespace: "{old_ns}", key: "{old_make_key}") {{ value }}
    oldModel: metafield(namespace: "{old_ns}", key: "{old_model_key}") {{ value }}
    oldYear: metafield(namespace: "{old_ns}", key: "{old_year_key}") {{ value }}
    newMake: metafield(namespace: "{new_ns}", key: "{new_make_key}") {{ value }}
    newModel: metafield(namespace: "{new_ns}", key: "{new_model_key}") {{ value }}
    newYear: metafield(namespace: "{new_ns}", key: "{new_year_key}") {{ value }}
    summary: metafield(namespace: "{old_ns}", key: "{summary_key}") {{ value }}
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

    for row in rows:
        stats["scanned_rows"] += 1
        if (
            stats["scanned_rows"] % progress_every == 0
            or stats["scanned_rows"] == stats["total_rows_loaded"]
        ):
            pct = (
                (100.0 * stats["scanned_rows"] / stats["total_rows_loaded"])
                if stats["total_rows_loaded"] > 0
                else 0.0
            )
            _log(
                f"Voortgang: scanned={stats['scanned_rows']}/{stats['total_rows_loaded']} "
                f"({pct:.1f}%), candidates={stats['candidate_products']}, "
                f"updated={stats['updated_products']}"
            )
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        product_gid = f"gid://shopify/Product/{pid}"

        try:
            existing = _graphql(shop_sess, q_existing, {"id": product_gid})
        except (requests.RequestException, RuntimeError) as e:
            return (stats, f"Shopify metafield read failed for product {pid}: {str(e)[:1000]}")

        p = ((existing.get("data") or {}).get("product") or {})
        handle = str(p.get("handle") or "").strip().lower()
        if only_handles and handle not in only_handles:
            continue

        old_make = ((p.get("oldMake") or {}).get("value") or "").strip()
        old_model = ((p.get("oldModel") or {}).get("value") or "").strip()
        old_year = ((p.get("oldYear") or {}).get("value") or "").strip()
        new_make = ((p.get("newMake") or {}).get("value") or "").strip()
        new_model = ((p.get("newModel") or {}).get("value") or "").strip()
        new_year = ((p.get("newYear") or {}).get("value") or "").strip()
        summary = ((p.get("summary") or {}).get("value") or "").strip()

        proj_old_make = str(row.get("fits_on_make_old") or "").strip()
        proj_old_model = str(row.get("fits_on_model_old") or "").strip()
        proj_old_year = str(row.get("fits_on_year_old") or "").strip()
        proj_summary = str(row.get("ymm_summary") or "").strip()

        proj_new_make = row.get("fits_on_make_new")
        proj_new_model = row.get("fits_on_model_new")
        proj_new_year = row.get("fits_on_year_new")

        to_set: list[dict[str, Any]] = []

        if not old_make and proj_old_make:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": old_ns,
                    "key": old_make_key,
                    "type": "single_line_text_field",
                    "value": proj_old_make,
                }
            )
        if not old_model and proj_old_model:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": old_ns,
                    "key": old_model_key,
                    "type": "single_line_text_field",
                    "value": proj_old_model,
                }
            )
        if not old_year and proj_old_year:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": old_ns,
                    "key": old_year_key,
                    "type": "single_line_text_field",
                    "value": proj_old_year,
                }
            )
        if not summary and proj_summary:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": old_ns,
                    "key": summary_key,
                    "type": "single_line_text_field",
                    "value": proj_summary,
                }
            )
        if not new_make and isinstance(proj_new_make, list) and proj_new_make:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": new_ns,
                    "key": new_make_key,
                    "type": "list.single_line_text_field",
                    "value": json.dumps(proj_new_make, ensure_ascii=False),
                }
            )
        if not new_model and isinstance(proj_new_model, list) and proj_new_model:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": new_ns,
                    "key": new_model_key,
                    "type": "list.single_line_text_field",
                    "value": json.dumps(proj_new_model, ensure_ascii=False),
                }
            )
        if not new_year and isinstance(proj_new_year, list) and proj_new_year:
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": new_ns,
                    "key": new_year_key,
                    "type": "list.single_line_text_field",
                    "value": json.dumps(proj_new_year, ensure_ascii=False),
                }
            )

        if not to_set:
            continue

        stats["candidate_products"] += 1
        if limit and stats["updated_products"] >= limit:
            continue

        fields_list = ",".join([f"{m['namespace']}.{m['key']}" for m in to_set])
        _log(
            f"Supabase backfill kandidaat: id={pid} handle={handle or '-'} "
            f"fields={fields_list}"
        )

        if dry_run:
            stats["updated_products"] += 1
            stats["updated_metafields"] += len(to_set)
            if stats["updated_products"] % progress_every == 0:
                _log(
                    f"Write-voortgang: updated_products={stats['updated_products']}, "
                    f"updated_metafields={stats['updated_metafields']}"
                )
            continue

        try:
            out = _graphql(shop_sess, m_set, {"metafields": to_set})
        except (requests.RequestException, RuntimeError) as e:
            return (stats, f"Shopify metafieldsSet failed for product {pid}: {str(e)[:1000]}")

        user_errors = (((out.get("data") or {}).get("metafieldsSet") or {}).get("userErrors") or [])
        if user_errors:
            return (stats, json.dumps(user_errors, ensure_ascii=False)[:2000])

        stats["updated_products"] += 1
        stats["updated_metafields"] += len(to_set)
        if stats["updated_products"] % progress_every == 0:
            _log(
                f"Write-voortgang: updated_products={stats['updated_products']}, "
                f"updated_metafields={stats['updated_metafields']}"
            )
        time.sleep(0.15)

    _log(
        f"Supabase YMM backfill klaar ({mode}): scanned={stats['scanned_rows']}, "
        f"candidates={stats['candidate_products']}, updated_products={stats['updated_products']}, "
        f"updated_metafields={stats['updated_metafields']}, only_handles={len(only_handles)}"
    )
    return (stats, None)


def run_shopify_ymm_push_from_supabase(
    payload: dict[str, Any],
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """
    Push canonical YMM from public.canonical_product_fits_on → Shopify metafields.

    Writes global.fits_on (JSON) + flat make/model/year + list *_new + ymm_summary.
    only_diff=True (default): alleen waar content_hash <> pushed_hash.
    """
    from modules.shopify_supabase_mirror import _fits_on_ns_key

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    if not config.SHOPIFY_ACCESS_TOKEN:
        return ({}, "SHOPIFY_ACCESS_TOKEN ontbreekt.")

    dry_run = _payload_bool(payload, "dry_run", True)
    overwrite = _payload_bool(payload, "overwrite", True)
    only_diff = _payload_bool(payload, "only_diff", True)
    limit = max(0, _payload_int(payload, "limit", 0))
    only_handles = _payload_handles(payload)
    progress_every = max(1, _payload_int(payload, "progress_every", 50))

    fits_ns, fits_key = _fits_on_ns_key()
    old_ns = "global"
    new_ns = "custom"

    stats: dict[str, Any] = {
        "dry_run": dry_run,
        "overwrite": overwrite,
        "only_diff": only_diff,
        "scanned_rows": 0,
        "skipped_up_to_date": 0,
        "updated_products": 0,
        "updated_metafields": 0,
        "failed_products": 0,
        "only_handles": len(only_handles),
        "errors": [],
    }

    target_product_ids: list[int] | None = None
    if only_handles:
        target_product_ids = _resolve_product_ids_for_handles(
            supabase_sess, rest_base, supabase_headers, only_handles
        )
        if not target_product_ids:
            return (stats, f"Geen shopify_products voor handles: {sorted(only_handles)}")

    try:
        rows = _fetch_canonical_fits_on_rows(
            supabase_sess,
            rest_base,
            supabase_headers,
            product_ids=target_product_ids,
            limit=limit,
            only_diff=only_diff,
        )
    except requests.RequestException as e:
        return (stats, f"Supabase read {CANONICAL_FITS_ON_TABLE} failed: {str(e)[:1000]}")

    mode = "DRY-RUN" if dry_run else "WRITE"
    _log(
        f"YMM push Shopify ({mode}, only_diff={only_diff}): "
        f"{len(rows)} te pushen, limit={limit}, handles={len(only_handles)}"
    )

    shop_sess = _http_session()
    m_set = """
mutation MetafieldsSet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    userErrors { field message code }
  }
}
""".strip()

    for row in rows:
        stats["scanned_rows"] += 1
        pid = row.get("shopify_product_id")
        if pid is None:
            continue
        handle = row.get("handle") or ""
        if only_handles and handle not in only_handles:
            continue
        ymm_json = row.get("ymm_json") or {}
        flat, flat_warnings = flat_columns_for_shopify_push(ymm_json)
        if not flat:
            continue
        for w in flat_warnings:
            _log(f"Waarschuwing {handle or pid}: {w}")

        product_gid = f"gid://shopify/Product/{pid}"
        fits_value = json.dumps(ymm_json, ensure_ascii=False, separators=(",", ":"))

        to_set: list[dict[str, Any]] = [
            {
                "ownerId": product_gid,
                "namespace": fits_ns,
                "key": fits_key,
                "type": "json",
                "value": fits_value,
            },
            {
                "ownerId": product_gid,
                "namespace": old_ns,
                "key": "fits_on_make",
                "type": "single_line_text_field",
                "value": flat["fits_on_make_old"],
            },
            {
                "ownerId": product_gid,
                "namespace": old_ns,
                "key": "fits_on_model",
                "type": "single_line_text_field",
                "value": flat["fits_on_model_old"],
            },
            {
                "ownerId": product_gid,
                "namespace": old_ns,
                "key": "fits_on_year",
                "type": "single_line_text_field",
                "value": flat["fits_on_year_old"],
            },
            {
                "ownerId": product_gid,
                "namespace": new_ns,
                "key": "fits_on_make_new",
                "type": "list.single_line_text_field",
                "value": json.dumps(flat["fits_on_make_new"], ensure_ascii=False),
            },
            {
                "ownerId": product_gid,
                "namespace": new_ns,
                "key": "fits_on_model_new",
                "type": "list.single_line_text_field",
                "value": json.dumps(flat["fits_on_model_new"], ensure_ascii=False),
            },
            {
                "ownerId": product_gid,
                "namespace": new_ns,
                "key": "fits_on_year_new",
                "type": "list.single_line_text_field",
                "value": json.dumps(flat["fits_on_year_new"], ensure_ascii=False),
            },
        ]
        if flat.get("ymm_summary"):
            to_set.append(
                {
                    "ownerId": product_gid,
                    "namespace": old_ns,
                    "key": "ymm_summary",
                    "type": "single_line_text_field",
                    "value": flat["ymm_summary"],
                }
            )

        stats["updated_products"] += 1
        _log(
            f"Push: id={pid} handle={handle or '-'} "
            f"metafields={len(to_set)} (fits_on {fits_ns}.{fits_key})"
        )

        if dry_run:
            stats["updated_metafields"] += len(to_set)
            continue

        try:
            out = _graphql(shop_sess, m_set, {"metafields": to_set})
        except (requests.RequestException, RuntimeError) as e:
            msg = f"Shopify metafieldsSet failed for {pid} ({handle}): {str(e)[:500]}"
            stats["failed_products"] += 1
            stats["errors"].append(msg)
            _log(f"FOUT {msg}")
            continue

        user_errors = (((out.get("data") or {}).get("metafieldsSet") or {}).get("userErrors") or [])
        if user_errors:
            msg = f"{pid} ({handle}): {json.dumps(user_errors, ensure_ascii=False)[:800]}"
            stats["failed_products"] += 1
            stats["errors"].append(msg)
            _log(f"FOUT {msg}")
            continue

        stats["updated_metafields"] += len(to_set)
        content_hash = str(row.get("content_hash") or "").strip()
        if not dry_run and content_hash:
            try:
                _mark_canonical_pushed(
                    supabase_sess,
                    rest_base,
                    supabase_headers,
                    int(pid),
                    content_hash,
                )
            except requests.RequestException as e:
                msg = f"pushed_hash update failed for {pid}: {str(e)[:400]}"
                stats["errors"].append(msg)
                _log(f"Waarschuwing: {msg}")
        if stats["updated_products"] % progress_every == 0:
            _log(
                f"Voortgang push: products={stats['updated_products']}, "
                f"metafields={stats['updated_metafields']}"
            )
        time.sleep(0.2)

    _log(
        f"YMM push klaar ({mode}): ok={stats['updated_products']}, "
        f"failed={stats['failed_products']}, metafields={stats['updated_metafields']}"
    )
    if stats["failed_products"]:
        return (
            stats,
            f"{stats['failed_products']} product(en) mislukt; zie stats.errors",
        )
    return (stats, None)


def run_canonical_ymm_pipeline(
    payload: dict[str, Any],
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """XML → canonical_product_fits_on → projection refresh → Shopify push (diff)."""
    from modules.canonical_ymm_supabase_sync import run_sync_canonical_ymm_to_supabase

    def _log(msg: str) -> None:
        if log:
            log(msg)
        else:
            print(msg, flush=True)

    combined: dict[str, Any] = {"steps": {}}

    _log(f"=== Stap 1/3: canonical YMM → Supabase {CANONICAL_FITS_ON_TABLE} ===")
    s1, err = run_sync_canonical_ymm_to_supabase(
        payload, supabase_sess, rest_base, supabase_headers, log=log
    )
    combined["steps"]["sync"] = s1
    if err:
        return (combined, err)

    _log("=== Stap 2/3: refresh shopify_ymm_projection ===")
    s2, err = run_refresh_shopify_ymm_projection(
        supabase_sess, rest_base, supabase_headers, log=log
    )
    combined["steps"]["projection"] = s2
    if err:
        return (combined, err)

    push_payload = dict(payload)
    push_payload.setdefault("overwrite", True)
    push_payload.setdefault("only_diff", True)
    _log("=== Stap 3/3: Supabase → Shopify (diff push) ===")
    s3, err = run_shopify_ymm_push_from_supabase(
        push_payload, supabase_sess, rest_base, supabase_headers, log=log
    )
    combined["steps"]["push"] = s3
    if err:
        return (combined, err)
    return (combined, None)


def run_shopify_ymm_push_diff_from_supabase(
    payload: dict[str, Any],
    supabase_sess: requests.Session,
    rest_base: str,
    supabase_headers: dict[str, str],
    log: Callable[[str], None] | None = None,
) -> tuple[dict[str, Any], str | None]:
    """Alleen producten waar canonical content_hash <> pushed_hash."""
    merged = dict(payload)
    merged["only_diff"] = True
    return run_shopify_ymm_push_from_supabase(
        merged, supabase_sess, rest_base, supabase_headers, log=log
    )
