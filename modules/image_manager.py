import json
import mimetypes
import os
import re
import threading
import time
from pathlib import Path
from urllib.parse import unquote

import requests

import config

# -----------------------------
# CONFIG
# -----------------------------

SHOPIFY_STORE = config.SHOPIFY_SHOP_SLUG
SHOPIFY_TOKEN = config.SHOPIFY_ACCESS_TOKEN

CDN_BASE = config.SHOPIFY_CDN_FILES_BASE_URL
# GraphQL voor staged upload + fileCreate (REST files.json multipart is onbetrouwbaar)
GRAPHQL_URL = (
    f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/"
    f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
)

CACHE_FILE = "cache/image_cache.json"
_cache_save_lock = threading.Lock()
_REQUEST_TIMEOUT = (12, 180)
_CDN_POLL_ATTEMPTS = 12
_CDN_POLL_SLEEP_SEC = 0.75

_session: requests.Session | None = None
_files_access_denied_notified = False
_throttled_notified = False


def _http_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        _session.trust_env = False
    return _session


def _debug(msg: str) -> None:
    if os.environ.get("KTM_DEBUG_SHOPIFY_IMAGES", "").strip().lower() in ("1", "true", "yes"):
        print(f"[image debug] {msg}", flush=True)


def _env_truthy(name: str, default: bool = True) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes")


def _cache_entry_url(cache: dict, filename: str) -> str | None:
    v = cache.get(filename)
    if isinstance(v, dict):
        u = v.get("url")
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def _normalize_cache_entry(cache: dict, filename: str, url: str) -> None:
    cache[filename] = {"url": url}


def _url_basename(url: str) -> str:
    try:
        return unquote(url.split("?", 1)[0].rsplit("/", 1)[-1])
    except (IndexError, TypeError):
        return ""


def _store_cache_url(cache: dict, local_filename: str, url: str) -> None:
    """
    Sla URL op onder de lokale basename; als de CDN-URL een andere bestandsnaam heeft
    (Shopify voegt soms __v1 toe), ook onder die naam — volgende run vindt de cache.
    """
    _normalize_cache_entry(cache, local_filename, url)
    cdn_name = _url_basename(url)
    if cdn_name and cdn_name.lower() != local_filename.lower():
        _normalize_cache_entry(cache, cdn_name, url)


def _canonical_filename_match(path_last: str, target_filename: str) -> bool:
    """
    Of het laatste URL-segment hetzelfde bestand is als de lokale basename.
    Shopify kan een extra __vN vóór de extensie zetten bij een 'duplicate' upload.
    """
    a = unquote(path_last).lower()
    b = target_filename.lower()
    if a == b:
        return True
    sa, ea = os.path.splitext(a)
    sb, eb = os.path.splitext(b)
    if ea != eb:
        return False
    ra = re.sub(r"__v\d+$", "", sa, count=1, flags=re.IGNORECASE)
    rb = re.sub(r"__v\d+$", "", sb, count=1, flags=re.IGNORECASE)
    return ra == rb


def _filename_variant_keys(filename: str) -> list[str]:
    """Lokale basename + mogelijke Shopify-namen met extra __vN vóór de extensie."""
    if not filename:
        return []
    out = [filename]
    stem, ext = os.path.splitext(filename)
    if not re.search(r"__v\d+$", stem, re.IGNORECASE):
        for n in (1, 2, 3, 4, 5):
            out.append(f"{stem}__v{n}{ext}")
    return out


def _file_lookup_queries(filename: str) -> list[str]:
    """
    Eén GraphQL-query per mogelijke Shopify-bestandsnaam.
    Moet gelijk lopen met _filename_variant_keys: als we alleen exact + __v1 zoeken,
    missen we bestanden die alleen als __v2…__v5 bestaan → dan volgt een dubbele upload.
    """
    return [f"filename:{name}" for name in _filename_variant_keys(filename)]


def _notify_graphql_errors(errs: list) -> None:
    """Eén duidelijke melding voor ACCESS_DENIED / THROTTLED (geen spam per bestand)."""
    global _files_access_denied_notified, _throttled_notified
    if not errs:
        return
    codes = {(e.get("extensions") or {}).get("code") for e in errs}
    if "ACCESS_DENIED" in codes and not _files_access_denied_notified:
        _files_access_denied_notified = True
        print(
            "\n--- Shopify afbeelding-upload geblokkeerd (ACCESS_DENIED) ---\n"
            "Je Admin API-token heeft geen rechten voor `fileCreate`.\n"
            "Voeg minstens één van deze scopes toe aan de app/token: "
            "`write_files` of `write_images` (eventueel `write_themes`).\n"
            "In Shopify Admin: het staff-account moet ook bestanden mogen aanmaken.\n"
            "Zet het token in de omgeving: export SHOPIFY_ACCESS_TOKEN='...'\n"
            "Documentatie: https://shopify.dev/docs/api/usage/access-scopes\n",
            flush=True,
        )
        return
    if "THROTTLED" in codes and not _throttled_notified:
        _throttled_notified = True
        print(
            "\n--- Shopify GraphQL: THROTTLED (te veel requests) ---\n"
            "Verlaag parallelle uploads: export KTM_IMAGE_UPLOAD_WORKERS=1\n"
            "(standaard is dit al lager dan voorheen.)\n",
            flush=True,
        )


# -----------------------------
# CACHE
# -----------------------------


def load_cache():
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_cache(cache):
    """Schrijf image_cache.json (gebruik save_cache_safe vanuit meerdere threads)."""
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)


def save_cache_safe(cache):
    """
    Thread-safe schrijven. Gebruik na elke afbeelding op de slow path (lookup/upload
    vult cache in geheugen); zo raak je bij Ctrl+C/crash niet alle tussentijdse mappen kwijt.
    """
    with _cache_save_lock:
        os.makedirs(os.path.dirname(CACHE_FILE) or ".", exist_ok=True)
        with open(CACHE_FILE, "w") as f:
            json.dump(cache, f, indent=2)


# -----------------------------
# URL helpers
# -----------------------------


def build_url(filename):
    return f"{CDN_BASE}{filename}"


def url_is_reachable(url: str) -> bool:
    """HEAD; bij falen kort GET (sommige CDN's reageren anders op HEAD)."""
    if not url:
        return False
    sess = _http_session()
    try:
        r = sess.head(
            url,
            timeout=10,
            allow_redirects=True,
            proxies={"http": None, "https": None},
            headers={"User-Agent": "KTM-ETL/1.0"},
        )
        if r.status_code == 200:
            return True
        if r.status_code in (301, 302, 303, 307, 308):
            return True
    except Exception:
        pass
    try:
        r = sess.get(
            url,
            timeout=15,
            stream=True,
            proxies={"http": None, "https": None},
            headers={"User-Agent": "KTM-ETL/1.0"},
        )
        ok = r.status_code == 200
        r.close()
        return ok
    except Exception:
        return False


def check_cdn(filename):
    return url_is_reachable(build_url(filename))


_FILE_LOOKUP_GQL = """
query KtmFileLookup($q: String!) {
  files(first: 25, query: $q) {
    edges {
      node {
        __typename
        ... on MediaImage {
          id
          image {
            url
          }
        }
        ... on GenericFile {
          url
        }
      }
    }
  }
}
"""


def _url_from_file_node(node: dict | None) -> str | None:
    if not isinstance(node, dict):
        return None
    tn = node.get("__typename")
    if tn == "MediaImage":
        img = node.get("image")
        if isinstance(img, dict):
            u = img.get("url")
            if isinstance(u, str) and u.startswith("http"):
                return u
    if tn == "GenericFile":
        u = node.get("url")
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def lookup_shopify_file_url_by_basename(filename: str) -> str | None:
    """
    Zoek of dit bestand al in Shopify bestanden (Content → Files) staat.
    Exacte filename:-query vindt geen bestand dat Shopify als …__v1.JPG heeft opgeslagen;
    daarom meerdere zoekpogingen en vergelijking via _canonical_filename_match.
    Vereist scope read_files (of read_images) op het Admin-token.
    """
    if not filename or not SHOPIFY_TOKEN.strip():
        return None
    seen_urls: set[str] = set()
    for q in _file_lookup_queries(filename):
        body = _admin_graphql(_FILE_LOOKUP_GQL, {"q": q})
        if body.get("errors"):
            _debug(f"lookup files errors: {body.get('errors')}")
            continue
        conn = (body.get("data") or {}).get("files") or {}
        edges = conn.get("edges") or []
        for e in edges:
            node = (e or {}).get("node") or {}
            url = _url_from_file_node(node)
            if not url or url in seen_urls:
                continue
            seen_urls.add(url)
            path_last = url.split("?", 1)[0].rsplit("/", 1)[-1]
            if _canonical_filename_match(path_last, filename):
                return url
    return None


# -----------------------------
# GraphQL + staged upload
# -----------------------------


def _admin_graphql(query: str, variables: dict | None = None) -> dict:
    """POST graphql.json; bij THROTTLED exponentiële backoff, daarna laatste response."""
    sess = _http_session()
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    for attempt in range(12):
        r = sess.post(
            GRAPHQL_URL,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": SHOPIFY_TOKEN,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        if not errs:
            return last
        codes = {(e.get("extensions") or {}).get("code") for e in errs}
        if "ACCESS_DENIED" in codes:
            _notify_graphql_errors(errs)
            _debug(f"GraphQL ACCESS_DENIED: {errs}")
            return last
        if "THROTTLED" in codes and attempt < 11:
            delay = min(2.0 * (1.45**attempt), 50.0)
            time.sleep(delay)
            continue
        _notify_graphql_errors(errs)
        _debug(f"GraphQL errors (na retries): {errs}")
        return last
    return last


def _post_staged_binary(
    upload_url: str,
    parameters: list,
    path: Path,
    mime: str,
) -> bool:
    sess = _http_session()
    fields = {p["name"]: p["value"] for p in parameters if p.get("name")}
    try:
        with open(path, "rb") as fh:
            files = {"file": (path.name, fh, mime)}
            r = sess.post(
                upload_url,
                data=fields,
                files=files,
                timeout=_REQUEST_TIMEOUT,
                proxies={"http": None, "https": None},
                headers={"User-Agent": "KTM-ETL/1.0"},
            )
    except OSError as e:
        _debug(f"staged read/post failed: {e}")
        return False

    if r.status_code not in (200, 201, 204):
        _debug(f"staged POST {r.status_code}: {r.text[:500]}")
        return False
    return True


def _extract_created_file_url(file_obj: dict) -> str | None:
    if not isinstance(file_obj, dict):
        return None
    tn = file_obj.get("__typename")
    if tn == "MediaImage":
        img = file_obj.get("image")
        if isinstance(img, dict):
            u = img.get("url")
            if isinstance(u, str) and u.startswith("http"):
                return u
    if tn == "GenericFile":
        u = file_obj.get("url")
        if isinstance(u, str) and u.startswith("http"):
            return u
    return None


def _resolve_public_url_after_create(path: Path, file_obj: dict) -> str | None:
    u = _extract_created_file_url(file_obj)
    if u:
        return u
    guessed = build_url(path.name)
    for _ in range(_CDN_POLL_ATTEMPTS):
        if url_is_reachable(guessed):
            return guessed
        time.sleep(_CDN_POLL_SLEEP_SEC)
    return None


def upload_image(local_path: Path) -> tuple[bool, str | None]:
    """
    Upload via Admin GraphQL: stagedUploadsCreate → POST binary → fileCreate.
    Retourneert (succes, publieke CDN-URL of None).
    """
    path = Path(local_path)
    mime, _ = mimetypes.guess_type(path.name)
    if not mime:
        mime = "application/octet-stream"

    resource = "IMAGE" if mime.startswith("image/") else "FILE"
    content_type_graphql = "IMAGE" if resource == "IMAGE" else "FILE"

    stage_q = """
    mutation stagedUploadsCreate($input: [StagedUploadInput!]!) {
      stagedUploadsCreate(input: $input) {
        stagedTargets {
          url
          resourceUrl
          parameters { name value }
        }
        userErrors { field message }
      }
    }
    """
    try:
        body = _admin_graphql(
            stage_q,
            {
                "input": [
                    {
                        "filename": path.name,
                        "mimeType": mime,
                        "httpMethod": "POST",
                        "resource": resource,
                    }
                ]
            },
        )
    except requests.RequestException as e:
        _debug(f"stagedUploadsCreate HTTP error: {e}")
        return False, None

    if body.get("errors"):
        _notify_graphql_errors(body["errors"])
        _debug(f"stagedUploadsCreate GraphQL errors: {body.get('errors')}")
        return False, None

    su = (body.get("data") or {}).get("stagedUploadsCreate") or {}
    uerr = su.get("userErrors") or []
    if uerr:
        _debug(f"stagedUploadsCreate userErrors: {uerr}")
        return False, None

    targets = su.get("stagedTargets") or []
    if not targets:
        _debug("stagedUploadsCreate: geen stagedTargets")
        return False, None

    t0 = targets[0]
    upload_url = t0.get("url")
    resource_url = t0.get("resourceUrl")
    params = t0.get("parameters") or []
    if not upload_url or not resource_url:
        _debug("staged target mist url of resourceUrl")
        return False, None

    if not _post_staged_binary(upload_url, params, path, mime):
        return False, None

    create_q = """
    mutation fileCreate($files: [FileCreateInput!]!) {
      fileCreate(files: $files) {
        files {
          __typename
          fileStatus
          ... on MediaImage {
            image { url }
          }
          ... on GenericFile {
            url
          }
        }
        userErrors { field message code }
      }
    }
    """
    alt = path.stem[:200] if path.stem else path.name
    try:
        body2 = _admin_graphql(
            create_q,
            {
                "files": [
                    {
                        "alt": alt,
                        "contentType": content_type_graphql,
                        "originalSource": resource_url,
                        "filename": path.name,
                        "duplicateResolutionMode": "REPLACE",
                    }
                ]
            },
        )
    except requests.RequestException as e:
        _debug(f"fileCreate HTTP error: {e}")
        return False, None

    if body2.get("errors"):
        _notify_graphql_errors(body2["errors"])
        _debug(f"fileCreate GraphQL errors: {body2.get('errors')}")
        return False, None

    fc = (body2.get("data") or {}).get("fileCreate") or {}
    ferr = fc.get("userErrors") or []
    if ferr:
        _debug(f"fileCreate userErrors: {ferr}")
        return False, None

    files_out = fc.get("files") or []
    if not files_out:
        _debug("fileCreate: geen files in response")
        return False, None

    public_url = _resolve_public_url_after_create(path, files_out[0])
    if not public_url:
        _debug("Geen publieke URL na fileCreate (poll timeout)")
        return False, None

    return True, public_url


# -----------------------------
# Main function
# -----------------------------


def try_resolve_image_cache_or_cdn(filename: str, cache: dict) -> str | None:
    """
    Alleen cache + CDN-HEAD (zelfde als stap 1–2 van ensure_image).
    Retourneert URL als de afbeelding zonder Shopify lookup/upload te vinden is; anders None.
    """
    if not filename:
        return None
    guessed = build_url(filename)
    for key in _filename_variant_keys(filename):
        if key not in cache:
            continue
        cached = _cache_entry_url(cache, key)
        if cached:
            return cached
    if filename in cache:
        return guessed
    if url_is_reachable(guessed):
        _store_cache_url(cache, filename, guessed)
        return guessed
    return None


def ensure_image(
    filename: str,
    local_path: Path,
    cache: dict,
    *,
    strict_delta: bool = True,
) -> tuple[str | None, bool]:
    """
    Zorgt voor een werkende publieke URL voor dit bestand.

    Retourneert (url_of_None, uploaded_naar_shopify).

    Geen dubbele uploads:
    - Staat de bestandsnaam al in image_cache.json → direct die URL gebruiken (geen HEAD/upload).
    - Anders: CDN-URL testen; zo niet bereikbaar, optioneel GraphQL `files`-zoektocht naar bestaand
      bestand in Shopify; pas daarna upload.

    strict_delta: alleen voor API-compatibiliteit; gedrag is gelijk (cache voorkomt her-upload).
    """
    _ = strict_delta
    if u := try_resolve_image_cache_or_cdn(filename, cache):
        return u, False

    guessed = build_url(filename)

    # 3) Bestand staat al in Shopify (zelfde bestandsnaam), maar andere/preview-URL
    if _env_truthy("KTM_IMAGE_SHOPIFY_FILE_LOOKUP", default=True):
        try:
            existing = lookup_shopify_file_url_by_basename(filename)
        except Exception as e:
            _debug(f"lookup exception: {e}")
            existing = None
        if existing:
            _store_cache_url(cache, filename, existing)
            return existing, False

    # 4) Echt nieuw uploaden
    ok, api_url = upload_image(Path(local_path))
    if not ok or not api_url:
        return None, False

    final = api_url if api_url else guessed
    _store_cache_url(cache, filename, final)
    return final, True


def resolve_image_url_without_upload(
    filename: str,
    local_path: Path,
    cache: dict,
    *,
    use_network: bool = True,
) -> str | None:
    """
    Zelfde stappen 1–3 als ensure_image (cache, CDN-HEAD, Shopify files-lookup), zonder upload.
    Gebruikt voor o.a. sku-probe; past image_cache.json niet aan.
    """
    guessed = build_url(filename)
    for key in _filename_variant_keys(filename):
        if key not in cache:
            continue
        cached = _cache_entry_url(cache, key)
        if cached:
            return cached
    if filename in cache:
        return guessed
    if not use_network:
        return None
    if url_is_reachable(guessed):
        return guessed
    if _env_truthy("KTM_IMAGE_SHOPIFY_FILE_LOOKUP", default=True):
        try:
            existing = lookup_shopify_file_url_by_basename(filename)
        except Exception as e:
            _debug(f"lookup exception: {e}")
            existing = None
        if existing:
            return existing
    return None
