"""Garage: samenvoegen, fitment, proxy-handtekening, sessie."""

from __future__ import annotations

import json
from pathlib import Path

from modules.motox_garage.catalog import add_fits_on, catalog_from_tree, load_catalog
from modules.motox_garage.fitment import (
    STATUS_NO,
    STATUS_NONE,
    STATUS_UNIVERSAL,
    STATUS_YES,
    fit_status,
)
from modules.motox_garage.model import merge_garages, normalize_garage
from modules.motox_garage.oauth import verify_oauth_hmac
from modules.motox_garage.proxy_auth import canonical_params, sign, verify_proxy_query
from modules.motox_garage.server import Config, GarageApp
from modules.motox_garage.shopify_customer import MemoryCustomers

BETA = {"id": "BETA::ALP 4.0::2007", "make": "BETA", "model": "ALP 4.0", "year": "2007"}
KTM = {"id": "KTM::EXC 300::2020", "make": "KTM", "model": "EXC 300", "year": "2020"}


def _garage(*vehicles: dict, active: str | None = None) -> dict:
    return {"activeId": active, "vehicles": list(vehicles)}


def test_normalize_drops_junk_and_dupes() -> None:
    raw = {
        "activeId": BETA["id"],
        "vehicles": [
            {"make": " beta ", "model": "alp   4.0", "year": "2007"},
            {"make": "BETA", "model": "ALP 4.0", "year": "2007"},
            {"make": "KTM", "model": "EXC 300", "year": "1899"},
            {"make": "", "model": "EXC", "year": "2020"},
        ],
    }
    garage = normalize_garage(raw)
    assert garage["vehicles"] == [BETA]
    assert garage["activeId"] == BETA["id"]


def test_merge_account_active_wins_and_cookie_bike_is_added() -> None:
    account = _garage(BETA, active=BETA["id"])
    cookie = _garage(KTM, active=KTM["id"])
    merged = merge_garages(account, cookie)
    assert [v["id"] for v in merged["vehicles"]] == [BETA["id"], KTM["id"]]
    assert merged["activeId"] == BETA["id"]


def test_merge_empty_account_keeps_cookie_active() -> None:
    merged = merge_garages({}, _garage(KTM, active=KTM["id"]))
    assert merged["vehicles"] == [KTM]
    assert merged["activeId"] == KTM["id"]


def test_merge_caps_at_twenty() -> None:
    account_vehicles = [
        {"make": "KTM", "model": f"MODEL {i}", "year": "2020"} for i in range(20)
    ]
    cookie = _garage(BETA, active=BETA["id"])
    merged = merge_garages({"vehicles": account_vehicles, "activeId": None}, cookie)
    assert len(merged["vehicles"]) == 20
    assert BETA["id"] not in {v["id"] for v in merged["vehicles"]}


def test_fit_status() -> None:
    fits = {"KTM": {"EXC 300": ["2019", "2020"]}}
    assert fit_status(fits, KTM) == STATUS_YES
    assert fit_status({"ktm": {"exc 300": ["2020"]}}, KTM) == STATUS_YES
    assert fit_status(fits, BETA) == STATUS_NO
    assert fit_status({}, KTM) == STATUS_UNIVERSAL
    assert fit_status(None, KTM) == STATUS_UNIVERSAL
    assert fit_status(fits, None) == STATUS_NONE


def test_catalog_tree_sorts_years_desc() -> None:
    tree: dict = {}
    add_fits_on(tree, {"ktm": {"exc 300": ["2019", "2024", "nope", "2020"]}})
    catalog = catalog_from_tree(tree)
    assert catalog["makes"][0]["make"] == "KTM"
    assert catalog["makes"][0]["models"][0]["years"] == ["2024", "2020", "2019"]


def test_shipped_catalog_loads() -> None:
    catalog = load_catalog()
    makes = {make["make"] for make in catalog["makes"]}
    assert "YAMAHA" in makes
    assert "HONDA" in makes
    assert len(makes) > 3


def test_oauth_hmac_sorts_params_and_ignores_hmac() -> None:
    import hmac
    from hashlib import sha256

    secret = "hush"
    message = "code=abc&shop=ktm-shop-nederland.myshopify.com&timestamp=1337178173"
    digest = hmac.new(secret.encode(), message.encode(), sha256).hexdigest()
    query = f"timestamp=1337178173&hmac={digest}&shop=ktm-shop-nederland.myshopify.com&code=abc"
    assert verify_oauth_hmac(query, secret)
    assert not verify_oauth_hmac(query, "other")


def test_offline_token_wins_over_client_credentials() -> None:
    app = GarageApp(
        Config(
            shop_domain="ktm-shop-nederland.myshopify.com",
            token="offline-token",
            api_key="key",
            secret="secret",
        )
    )
    assert app.shopify is not None
    assert app.shopify.token == "offline-token"
    assert app.shopify.client_id == ""


def test_proxy_signature_matches_shopify_docs() -> None:
    secret = "hush"
    params = {
        "extra": ["1", "2"],
        "shop": "{shop}.myshopify.com",
        "logged_in_customer_id": "1",
        "path_prefix": "/apps/awesome_reviews",
        "timestamp": "1317327555",
    }
    message = (
        "extra=1,2logged_in_customer_id=1path_prefix=/apps/awesome_reviews"
        "shop={shop}.myshopify.comtimestamp=1317327555"
    )
    assert canonical_params(params) == message
    signature = sign(secret, params)
    query = (
        "extra=1&extra=2&shop=%7Bshop%7D.myshopify.com&logged_in_customer_id=1"
        "&path_prefix=%2Fapps%2Fawesome_reviews&timestamp=1317327555&signature=" + signature
    )
    assert verify_proxy_query(query, secret, now=1317327555)
    assert not verify_proxy_query(query, "other", now=1317327555)


def test_session_merge_then_replace() -> None:
    app = GarageApp(Config(allow_unsigned=True, memory=True, catalog_file=Path("missing.json")))
    query = "logged_in_customer_id=42"
    status, payload = app.handle(
        "POST",
        "/proxy/session/merge",
        query,
        json.dumps(_garage(BETA, active=BETA["id"])).encode(),
    )
    assert status == 200
    assert payload["garage"]["activeId"] == BETA["id"]

    account = MemoryCustomers()
    account.put("42", _garage(KTM, active=KTM["id"]))
    app.memory = account
    status, payload = app.handle(
        "POST",
        "/proxy/session/merge",
        query,
        json.dumps(_garage(BETA, active=BETA["id"])).encode(),
    )
    assert status == 200
    assert payload["garage"]["activeId"] == KTM["id"]
    assert {v["id"] for v in payload["garage"]["vehicles"]} == {BETA["id"], KTM["id"]}

    status, payload = app.handle(
        "PUT",
        "/proxy/session",
        query,
        json.dumps(_garage(BETA, active=BETA["id"])).encode(),
    )
    assert status == 200
    assert [v["id"] for v in payload["garage"]["vehicles"]] == [BETA["id"]]


def test_customer_id_uses_the_value_shopify_appends() -> None:
    from modules.motox_garage.server import _customer_id

    assert _customer_id("logged_in_customer_id=999&logged_in_customer_id=42") == "42"
    assert _customer_id("logged_in_customer_id=") == ""


def test_unsigned_rejected_when_secret_required() -> None:
    app = GarageApp(Config(secret="hush", allow_unsigned=False))
    status, payload = app.handle("GET", "/proxy/vehicles", "", b"")
    assert status == 401
    assert payload["error"] == "bad_signature"


def test_fitment_index_hides_pos_clones_and_keeps_universals() -> None:
    import tempfile

    from modules.motox_garage.fitment_index import FitmentIndex

    fits = {"KTM": {"EXC 300": ["2020"]}}
    with tempfile.TemporaryDirectory() as tmp:
        index = FitmentIndex.create(Path(tmp) / "fit.sqlite")
        index.add_product(1, ["synkro-clone-ktm-shop-nederland"], None)
        index.add_product(2, [], None)
        index.add_product(3, ["synkro-clone-ktm-shop-nederland"], fits)
        index.add_product(4, [], {"ktm": {"exc 300": ["2020"]}})
        index.finish()
        assert index.allows(1, KTM) is False
        assert index.allows(2, KTM) is True
        assert index.allows(3, KTM) is True
        assert index.allows(3, BETA) is False
        assert index.allows(4, KTM) is True
        assert index.allows(4, BETA) is False
        assert index.allows(99, KTM) is True
        index.add_product(10, [], {"GAS GAS": {"G ENDURO": ["2023"]}})
        assert index.allows(10, KTM) is True
        assert index.has_fitted([10]) is False
        index._conn.execute("INSERT OR IGNORE INTO fitted (product_id) VALUES (11)")
        index._conn.execute(
            "INSERT OR IGNORE INTO fit (product_id, bike) VALUES (11, 'GAS GAS::G ENDURO::2023')"
        )
        index._conn.commit()
        assert index.ebike_only_ids([11]) == {11}
        assert index.has_fitted([11]) is False
        assert index.allows(11, KTM) is True
        index.close()


def test_collection_filter_pages_and_keeps_vendor_filter() -> None:
    import tempfile

    from modules.motox_garage.collection import facet_counts, filter_collection
    from modules.motox_garage.fitment_index import FitmentIndex

    products = [
        _product(1, "Helm", "SHOEI", available=True),
        _product(2, "Leiding", "VENHILL", available=True, fits="KTM - 2020"),
        _product(3, "Kassakopie", "KTM", available=True),
        _product(4, "Uitverkocht", "VENHILL", available=False),
        _product(5, "Andere leiding", "VENHILL", available=True),
    ]
    with tempfile.TemporaryDirectory() as tmp:
        index = FitmentIndex.create(Path(tmp) / "fit.sqlite")
        index.add_product(2, [], {"KTM": {"EXC 300": ["2020"]}})
        index.add_product(3, ["synkro-clone-ktm-shop-nederland"], None)
        index.add_product(5, [], {"BETA": {"ALP 4.0": ["2007"]}})
        index.finish()
        page = filter_collection(
            products,
            index,
            KTM,
            {"filter.p.vendor": ["VENHILL"]},
            page=1,
            per_page=1,
        )
        assert page["total"] == 1
        assert page["hasFitment"] is True
        assert page["products"][0]["id"] == 2
        assert page["products"][0]["fits"] == "KTM - 2020"
        second = filter_collection(products, index, KTM, {}, page=1, per_page=10)
        assert [item["id"] for item in second["products"]] == [1, 2]
        bare = [
            _product(1, "Helm", "SHOEI", available=True),
            _product(3, "Kassakopie", "KTM", available=True),
        ]
        none = filter_collection(bare, index, KTM, {}, page=1, per_page=10)
        assert none["hasFitment"] is False
        index.close()


def test_facet_counts_keep_other_types_visible() -> None:
    from modules.motox_garage.collection import facet_counts

    products = [
        _product(1, "Leiding", "VENHILL", available=True),
        _product(2, "Keerring", "ALL BALLS", available=True),
    ]
    products[0]["productType"] = "Remleidingen"
    products[1]["productType"] = "Keringen"
    counts = facet_counts(products, {"filter.p.product_type": ["Remleidingen"]})
    assert counts["filter.p.product_type"]["Remleidingen"] == 1
    assert counts["filter.p.product_type"]["Keringen"] == 1
    assert counts["filter.p.vendor"] == {"VENHILL": 1}
    assert counts["filter.v.availability"]["1"] == 1


def test_collection_route_uses_the_index() -> None:
    import tempfile

    from modules.motox_garage.fitment_index import FitmentIndex

    with tempfile.TemporaryDirectory() as tmp:
        index = FitmentIndex.create(Path(tmp) / "fit.sqlite")
        index.add_product(7, [], {"KTM": {"EXC 300": ["2020"]}})
        index.finish()
        products = [
            _product(7, "Past", "VENHILL", available=True),
            _product(8, "Helm", "SHOEI", available=True),
        ]
        app = GarageApp(
            Config(allow_unsigned=True, memory=True, catalog_file=Path("missing.json")),
            index=index,
            collection_loader=lambda handle: products if handle == "leidingen" else [],
        )
        status, payload = app.handle(
            "GET",
            "/proxy/collection",
            "handle=leidingen&make=KTM&model=EXC%20300&year=2020&page=1",
            b"",
        )
        assert status == 200
        assert payload["hasFitment"] is True
        assert payload["total"] == 2
        assert {item["id"] for item in payload["products"]} == {7, 8}
        missing, body = app.handle("GET", "/proxy/collection", "handle=leidingen", b"")
        assert missing == 400
        assert body["error"] == "bad_vehicle"
        locked = GarageApp(
            Config(secret="hush", allow_unsigned=False, catalog_file=Path("missing.json")),
            index=index,
            collection_loader=lambda handle: products,
        )
        open_status, open_body = locked.handle(
            "GET",
            "/proxy/collection",
            "handle=leidingen&make=KTM&model=EXC%20300&year=2020",
            b"",
        )
        assert open_status == 200
        assert open_body["total"] == 2
        session_status, session_body = locked.handle(
            "GET", "/proxy/session", "logged_in_customer_id=1", b""
        )
        assert session_status == 401
        assert session_body["error"] == "bad_signature"
        index.close()


def test_compact_fitment_matches_the_sqlite_index() -> None:
    import tempfile

    from modules.motox_garage.fitment_compact import CompactFitment, build_compact
    from modules.motox_garage.fitment_index import FitmentIndex

    fits = {"KTM": {"EXC 300": ["2020"]}}
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "fit.sqlite"
        index = FitmentIndex.create(path)
        index.add_product(1, ["synkro-clone-ktm-shop-nederland"], None)
        index.add_product(2, [], None)
        index.add_product(3, ["synkro-clone-ktm-shop-nederland"], fits)
        index.add_product(4, [], {"ktm": {"exc 300": ["2020"]}})
        index.add_product(10, [], {"GAS GAS": {"G ENDURO": ["2023"]}})
        index._conn.execute("INSERT OR IGNORE INTO fitted (product_id) VALUES (11)")
        index._conn.execute(
            "INSERT OR IGNORE INTO fit (product_id, bike) VALUES (11, 'GAS GAS::G ENDURO::2023')"
        )
        index.finish()
        out = Path(tmp) / "fit.bin"
        build_compact(path, out)
        compact = CompactFitment.load(out)
        assert compact is not None
        ids = [1, 2, 3, 4, 10, 11, 99]
        for vehicle in (KTM, BETA):
            assert compact.allowed_ids(ids, vehicle) == index.allowed_ids(ids, vehicle)
        assert compact.ebike_only_ids([11]) == index.ebike_only_ids([11])
        assert compact.has_fitted([11]) is index.has_fitted([11])
        assert compact.has_fitted([3, 4]) is index.has_fitted([3, 4])
        index.close()


def _product(product_id: int, title: str, vendor: str, *, available: bool, fits: str = "") -> dict:
    return {
        "id": product_id,
        "handle": f"p-{product_id}",
        "title": title,
        "vendor": vendor,
        "productType": "Onderdeel",
        "tags": [],
        "price": "10.00",
        "available": available,
        "image": "",
        "url": f"/products/p-{product_id}",
        "fits": fits,
    }
