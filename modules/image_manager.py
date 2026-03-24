import json
import mimetypes
import os
import time
from pathlib import Path

import requests

# -----------------------------
# CONFIG
# -----------------------------

SHOPIFY_STORE = "ktm-shop-nl"
SHOPIFY_TOKEN = os.environ.get("SHOPIFY_ACCESS_TOKEN", "").strip() or (
    "REDACTED_REVOKE_AND_ROTATE"
)

CDN_BASE = "https://cdn.shopify.com/s/files/1/0511/7820/9461/files/"
# GraphQL voor staged upload + fileCreate (REST files.json multipart is onbetrouwbaar)
GRAPHQL_URL = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-10/graphql.json"

CACHE_FILE = "cache/image_cache.json"
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

    strict_delta (aan voor delta in main.py):
    - Cache wordt niet blind vertrouwd: URL moet bereikbaar zijn, anders opnieuw uploaden.
    - Upload loopt via GraphQL staged + fileCreate; retour-URL komt van Shopify.
    """
    guessed = build_url(filename)

    if strict_delta and filename in cache:
        if url_is_reachable(guessed):
            return guessed, False
        del cache[filename]

    if not strict_delta and filename in cache:
        return guessed, False

    if url_is_reachable(guessed):
        cache[filename] = True
        return guessed, False

    ok, api_url = upload_image(Path(local_path))
    if not ok or not api_url:
        return None, False

    cache[filename] = True
    return api_url, True
