"""HTTP-app voor de Shopify app-proxy en de lokale preview."""

from __future__ import annotations

import json
import os
import re
import threading
import time
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from modules.motox_garage.catalog import catalog_path, load_catalog
from modules.motox_garage.collection import (
    PER_PAGE,
    CollectionFetchError,
    CollectionLoader,
    attach_ymm_summaries,
    fetch_collection_products,
    filter_collection,
)
from modules.motox_garage.fitment_index import FitmentIndex
from modules.motox_garage.model import coerce_active, merge_garages, normalize_garage, normalize_vehicle
from modules.motox_garage.proxy_auth import params_from_query, verify_proxy_query
from modules.motox_garage.shopify_customer import (
    CustomerNotFound,
    CustomerStore,
    MemoryCustomers,
    ShopifyCustomers,
    ShopifyGarageError,
)

MAX_BODY = 20_000
APP_ROOT = Path(__file__).resolve().parents[2] / "motox" / "garage"
DEFAULT_INDEX = Path(__file__).resolve().parents[2] / "cache" / "motox" / "fitment_index.sqlite"
_HANDLE = re.compile(r"[a-z0-9][a-z0-9-]{0,200}")
_COLLECTION_TTL = 600


@dataclass
class Config:
    secret: str = ""
    api_key: str = ""
    allow_unsigned: bool = False
    shop_domain: str = ""
    token: str = ""
    read_token: str = ""
    api_version: str = "2024-10"
    catalog_file: Path = catalog_path()
    index_file: Path = DEFAULT_INDEX
    preview: bool = False
    memory: bool = False
    max_age: int = 600


def config_from_env() -> Config:
    secret = (os.environ.get("SHOPIFY_API_SECRET") or "").strip()
    api_key = (os.environ.get("SHOPIFY_API_KEY") or "").strip()
    # Niet SHOPIFY_ACCESS_TOKEN: dat is de product-sync-app, die mag geen klanten schrijven.
    token = (os.environ.get("SHOPIFY_MOTOX_GARAGE_TOKEN") or "").strip()
    shop = (
        os.environ.get("SHOPIFY_MOTOX_SHOP_DOMAIN") or os.environ.get("SHOPIFY_SHOP_DOMAIN") or ""
    ).strip()
    allow_unsigned = os.environ.get("MOTOX_GARAGE_ALLOW_UNSIGNED") == "1" or (
        not secret and os.environ.get("MOTOX_GARAGE_PREVIEW") == "1"
    )
    return Config(
        secret=secret,
        api_key=api_key,
        allow_unsigned=allow_unsigned,
        shop_domain=shop,
        token=token,
        read_token=(os.environ.get("SHOPIFY_ACCESS_TOKEN") or "").strip(),
        api_version=(
            os.environ.get("SHOPIFY_MOTOX_ADMIN_API_VERSION")
            or os.environ.get("SHOPIFY_ADMIN_API_VERSION")
            or "2024-10"
        ).strip(),
        preview=os.environ.get("MOTOX_GARAGE_PREVIEW") == "1",
        memory=os.environ.get("MOTOX_GARAGE_MEMORY") == "1",
    )


class GarageApp:
    def __init__(
        self,
        config: Config,
        shopify: CustomerStore | None = None,
        index: FitmentIndex | None = None,
        collection_loader: CollectionLoader | None = None,
    ) -> None:
        self.config = config
        self.memory = MemoryCustomers()
        self.shopify = shopify
        self._index = index
        self._index_loaded = index is not None
        self.collection_loader = collection_loader
        self._collections: dict[str, tuple[float, list[dict]]] = {}
        self._collection_guard = threading.Lock()
        self._collection_wait: dict[str, threading.Event] = {}
        if shopify is None and config.shop_domain and not config.memory:
            # Offline token wint. Client credentials werken niet op de productiewinkel.
            if config.token:
                self.shopify = ShopifyCustomers(config.shop_domain, config.token, config.api_version)
            elif config.api_key and config.secret:
                self.shopify = ShopifyCustomers(
                    config.shop_domain,
                    "",
                    config.api_version,
                    client_id=config.api_key,
                    client_secret=config.secret,
                )

    def handle(
        self,
        method: str,
        path: str,
        query_string: str,
        body: bytes,
    ) -> tuple[int, dict]:
        route = path.rstrip("/") or "/"
        if route == "/proxy/vehicles" and method == "GET":
            denied = self._authorize(query_string)
            if denied:
                return denied
            return 200, load_catalog(self.config.catalog_file)
        if route == "/proxy/collection" and method == "GET":
            # Openbare productdata. De sessieroutes blijven wel onder de handtekening.
            return self._collection(query_string)
        if route in {"/proxy/session", "/proxy/session/merge"}:
            denied = self._authorize(query_string)
            if denied:
                return denied
            customer_id = _customer_id(query_string)
            if not customer_id:
                return 401, {"ok": False, "error": "not_logged_in"}
            store = self._store(query_string)
            if store is None:
                return 503, {"ok": False, "error": "shopify_token_missing"}
            try:
                return self._session(method, route, customer_id, store, body)
            except CustomerNotFound:
                return 404, {"ok": False, "error": "customer_not_found"}
            except ShopifyGarageError as exc:
                return 502, {"ok": False, "error": "shopify", "message": str(exc)}
        return 404, {"ok": False, "error": "not_found"}

    def _collection(self, query_string: str) -> tuple[int, dict]:
        params = params_from_query(query_string)
        handle = _single(params, "handle").strip().lower()
        if not _HANDLE.fullmatch(handle):
            return 400, {"ok": False, "error": "bad_handle"}
        vehicle = normalize_vehicle(
            {
                "make": _single(params, "make"),
                "model": _single(params, "model"),
                "year": _single(params, "year"),
            }
        )
        if vehicle is None:
            return 400, {"ok": False, "error": "bad_vehicle"}
        index = self._fitment_index()
        if index is None:
            return 503, {"ok": False, "error": "index_missing"}
        try:
            page = int(_single(params, "page") or "1")
        except ValueError:
            page = 1
        try:
            products = self._collection_products(handle)
        except CollectionFetchError as exc:
            if str(exc) == "not_found":
                return 404, {"ok": False, "error": "collection_not_found"}
            return 502, {"ok": False, "error": "collection"}
        result = filter_collection(products, index, vehicle, params, page=page, per_page=PER_PAGE)
        hidden = set(result.get("hideYmm") or [])
        visible = [item for item in result.get("products") or [] if item.get("handle") not in hidden]
        attach_ymm_summaries(
            self.config.shop_domain,
            self.config.read_token,
            self.config.api_version,
            visible,
        )
        return 200, {"ok": True, **result}

    def _fitment_index(self) -> FitmentIndex | None:
        if self._index_loaded:
            return self._index
        self._index = FitmentIndex.open(self.config.index_file)
        self._index_loaded = True
        return self._index

    def _fresh_collection(self, handle: str) -> list[dict] | None:
        cached = self._collections.get(handle)
        if cached and time.time() - cached[0] < _COLLECTION_TTL:
            return cached[1]
        return None

    def _collection_products(self, handle: str) -> list[dict]:
        if self.collection_loader is not None:
            return self.collection_loader(handle)
        cached = self._fresh_collection(handle)
        if cached is not None:
            return cached
        with self._collection_guard:
            cached = self._fresh_collection(handle)
            if cached is not None:
                return cached
            pending = self._collection_wait.get(handle)
            owner = pending is None
            if owner:
                pending = threading.Event()
                self._collection_wait[handle] = pending
        if not owner:
            pending.wait(45)
            cached = self._fresh_collection(handle)
            if cached is not None:
                return cached
        try:
            products = fetch_collection_products(self.config.shop_domain, handle)
            self._collections[handle] = (time.time(), products)
            return products
        finally:
            if owner:
                with self._collection_guard:
                    self._collection_wait.pop(handle, None)
                pending.set()

    def _session(
        self,
        method: str,
        route: str,
        customer_id: str,
        store: CustomerStore,
        body: bytes,
    ) -> tuple[int, dict]:
        if route == "/proxy/session" and method == "GET":
            return 200, {"ok": True, "garage": coerce_active(store.get(customer_id))}
        incoming = _parse_body(body)
        if incoming is None:
            return 400, {"ok": False, "error": "bad_json"}
        if route == "/proxy/session/merge" and method == "POST":
            merged = merge_garages(store.get(customer_id), incoming)
            store.put(customer_id, merged)
            return 200, {"ok": True, "garage": merged}
        if route == "/proxy/session" and method == "PUT":
            replacement = coerce_active(incoming)
            store.put(customer_id, replacement)
            return 200, {"ok": True, "garage": coerce_active(store.get(customer_id))}
        return 405, {"ok": False, "error": "method_not_allowed"}

    def _store(self, query_string: str) -> CustomerStore | None:
        signed = bool(self.config.secret) and _signature_ok(query_string, self.config)
        if signed:
            return self.shopify
        if self.config.memory or self.config.allow_unsigned:
            return self.memory
        return None

    def _authorize(self, query_string: str) -> tuple[int, dict] | None:
        if self.config.secret:
            if not _signature_ok(query_string, self.config):
                return 401, {"ok": False, "error": "bad_signature"}
            shop = _first(query_string, "shop").lower()
            expected = self.config.shop_domain.lower()
            if expected and shop and shop != expected:
                return 401, {"ok": False, "error": "shop_mismatch"}
            return None
        if self.config.allow_unsigned:
            return None
        return 401, {"ok": False, "error": "bad_signature"}


def _signature_ok(query_string: str, config: Config) -> bool:
    return verify_proxy_query(query_string, config.secret, max_age=config.max_age)


def _single(params: dict[str, list[str]], key: str) -> str:
    values = params.get(key) or [""]
    return values[0]


def _first(query_string: str, key: str) -> str:
    values = params_from_query(query_string).get(key) or [""]
    return values[0]


def _customer_id(query_string: str) -> str:
    """Laatste numerieke waarde. Shopify hangt zijn id achter een eventuele client-parameter."""
    values = params_from_query(query_string).get("logged_in_customer_id") or []
    for raw in reversed(values):
        text = raw.strip()
        if text.isdigit() and 1 <= len(text) <= 20:
            return text
    return ""


def _parse_body(body: bytes) -> dict | None:
    if len(body) > MAX_BODY:
        return None
    if not body:
        return {}
    try:
        parsed = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(parsed, dict):
        return None
    return normalize_garage(parsed)


PREVIEW_FILES = {
    "/preview": APP_ROOT / "preview" / "index.html",
    "/preview/": APP_ROOT / "preview" / "index.html",
    "/assets/garage.js": APP_ROOT / "extensions" / "motor-garage" / "assets" / "garage.js",
    "/assets/garage.css": APP_ROOT / "extensions" / "motor-garage" / "assets" / "garage.css",
}
CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
}


def make_handler(app: GarageApp) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self) -> None:  # noqa: N802
            self._dispatch()

        def do_POST(self) -> None:  # noqa: N802
            self._dispatch()

        def do_PUT(self) -> None:  # noqa: N802
            self._dispatch()

        def log_message(self, fmt: str, *args) -> None:
            return

        def _dispatch(self) -> None:
            parts = urlsplit(self.path)
            if app.config.preview and parts.path in {"/", ""}:
                self._redirect("/preview")
                return
            if parts.path in {"/", ""}:
                self._html(
                    "<!doctype html><meta charset=utf-8><title>Motox garage</title>"
                    "<p>Motox garage draait. De motorfilter staat op de winkel, niet in dit scherm.</p>"
                )
                return
            preview = PREVIEW_FILES.get(parts.path)
            if app.config.preview and preview is not None:
                self._file(preview)
                return
            query = parts.query
            if (
                app.config.preview
                and "logged_in_customer_id=" not in query
                and "mxg_preview=1" in (self.headers.get("Cookie") or "")
            ):
                query = f"{query}&logged_in_customer_id=1" if query else "logged_in_customer_id=1"
            length = int(self.headers.get("Content-Length") or "0")
            if length > MAX_BODY:
                self._json(413, {"ok": False, "error": "body_too_large"})
                return
            body = self.rfile.read(length) if length else b""
            status, payload = app.handle(self.command, parts.path, query, body)
            self._json(status, payload)

        def _redirect(self, location: str) -> None:
            self.send_response(302)
            self.send_header("Location", location)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _html(self, body: str) -> None:
            data = body.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _file(self, path: Path) -> None:
            if not path.is_file():
                self._json(404, {"ok": False, "error": "not_found"})
                return
            data = path.read_bytes()
            self.send_response(200)
            self.send_header(
                "Content-Type",
                CONTENT_TYPES.get(path.suffix, "application/octet-stream"),
            )
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _json(self, status: int, payload: dict) -> None:
            data = json.dumps(payload).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

    return Handler


def serve(app: GarageApp, host: str, port: int) -> None:
    server = ThreadingHTTPServer((host, port), make_handler(app))
    server.serve_forever()
