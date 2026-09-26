"""Artikelnummers uit Bihr: labels, zoeklijst en cache."""

from __future__ import annotations

import json
from pathlib import Path

from modules.ebihr.article_numbers import (
    article_payload,
    plan_article_update,
    ref_tag,
)
from modules.ebihr.build_products import _normalize_product
from modules.motox_shopify_cache import _parse_bulk


def test_payload_drops_empty_and_sku_duplicates():
    assert article_payload("8005119012", "8005119012", "") == {}
    assert article_payload("8005119012", "800002460271", "7136205") == {
        "old": "800002460271",
        "supplier": "7136205",
    }
    assert article_payload("8005119012", "800002460271", "800002460271") == {
        "old": "800002460271",
    }


def test_normalize_product_keeps_bihr_numbers():
    product = _normalize_product(
        {
            "PartNumber": "8005119012",
            "OldPartNumber": "800002460271",
            "SupplierPartNumber": "7136205",
            "Title": "Helm",
        },
        brand="BELL",
        group="",
        family="",
        segment="",
        group_code="",
        family_code="",
        segment_code="",
    )
    assert product["oldPartNumber"] == "800002460271"
    assert product["supplierPartNumber"] == "7136205"


def test_plan_writes_labels_codes_and_tags_once():
    variants = [
        {
            "sku": "8005119012",
            "article_numbers": article_payload("8005119012", "800002460271", "7136205"),
        }
    ]
    first = plan_article_update(variants)
    assert first is not None
    assert first["variants"]["8005119012"] == '{"old":"800002460271","supplier":"7136205"}'
    assert first["search_codes"] == ["7136205", "800002460271"]
    assert first["tags"] == [ref_tag("7136205"), ref_tag("800002460271")]

    again = plan_article_update(
        variants,
        stored_codes=first["search_codes"],
        stored_by_sku={"8005119012": first["variants"]["8005119012"]},
        stored_ref_tags=first["tags"],
    )
    assert again is None


def test_plan_keeps_replaced_number_searchable():
    variants = [
        {
            "sku": "8005119012",
            "article_numbers": {"old": "9000", "supplier": "7136205"},
        }
    ]
    update = plan_article_update(
        variants,
        stored_codes=["800002460271"],
        stored_by_sku={"8005119012": '{"old":"800002460271","supplier":"7136205"}'},
        stored_ref_tags=["ref:800002460271", "ref:7136205"],
    )
    assert update is not None
    assert json.loads(update["variants"]["8005119012"])["old"] == "9000"
    assert "800002460271" in update["search_codes"]
    assert "9000" in update["search_codes"]
    assert update["tags"] == ["ref:9000"]


def test_plan_keeps_last_old_number_when_bihr_clears_it():
    variants = [
        {"sku": "8005119012", "article_numbers": {}},
    ]
    assert (
        plan_article_update(
            variants,
            stored_codes=["800002460271"],
            stored_by_sku={"8005119012": '{"old":"800002460271"}'},
            stored_ref_tags=["ref:800002460271"],
        )
        is None
    )


def test_plan_skips_codes_that_match_a_sku():
    variants = [
        {"sku": "111", "article_numbers": {"old": "222"}},
        {"sku": "222", "article_numbers": {}},
    ]
    update = plan_article_update(variants)
    assert update is not None
    assert update["search_codes"] is None
    assert update["tags"] == []
    assert "222" not in json.dumps(update["search_codes"])


def test_parse_bulk_reads_inlined_and_child_article_numbers(tmp_path: Path):
    path = tmp_path / "bulk.jsonl"
    rows = [
        {
            "id": "gid://shopify/Product/1",
            "handle": "8005119",
            "title": "Helm",
            "status": "ACTIVE",
            "tags": ["sale", "ref:800002460271"],
            "searchCodes": {"key": "search_codes", "value": '["800002460271"]'},
            "featuredImage": None,
        },
        {
            "id": "gid://shopify/ProductVariant/9",
            "sku": "8005119012",
            "price": "10.00",
            "articleNumbers": {
                "key": "article_numbers",
                "value": '{"old":"800002460271","supplier":"7136205"}',
            },
            "__parentId": "gid://shopify/Product/1",
        },
        {
            "id": "gid://shopify/Product/2",
            "handle": "oud",
            "title": "Los",
            "status": "ACTIVE",
            "tags": [],
        },
        {
            "id": "gid://shopify/ProductVariant/10",
            "sku": "42",
            "__parentId": "gid://shopify/Product/2",
        },
        {
            "namespace": "custom",
            "key": "article_numbers",
            "value": '{"supplier":"ABC123"}',
            "__parentId": "gid://shopify/ProductVariant/10",
        },
        {
            "namespace": "custom",
            "key": "search_codes",
            "value": '["ABC123"]',
            "__parentId": "gid://shopify/Product/2",
        },
        {
            "namespace": "global",
            "key": "fits_on",
            "value": '{"KTM":{"EXC":["2020"]}}',
            "__parentId": "gid://shopify/Product/2",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    products = _parse_bulk(path)
    helm = products["gid://shopify/Product/1"]
    assert helm["search_codes"] == ["800002460271"]
    assert helm["ref_tags"] == ["ref:800002460271"]
    assert helm["skus"][0]["article"] == '{"old":"800002460271","supplier":"7136205"}'
    other = products["gid://shopify/Product/2"]
    assert other["search_codes"] == ["ABC123"]
    assert other["skus"][0]["article"] == '{"supplier":"ABC123"}'
    assert other["fits_on_hash"]


def test_parse_bulk_still_reads_sku_without_article_fields(tmp_path: Path):
    path = tmp_path / "bulk.jsonl"
    rows = [
        {
            "id": "gid://shopify/Product/1",
            "handle": "8006904",
            "title": "Shirt",
            "status": "ACTIVE",
            "featuredImage": None,
        },
        {
            "id": "gid://shopify/ProductVariant/9",
            "sku": "8006904002",
            "__parentId": "gid://shopify/Product/1",
        },
    ]
    path.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")
    products = _parse_bulk(path)
    assert products["gid://shopify/Product/1"]["skus"][0]["sku"] == "8006904002"
    assert products["gid://shopify/Product/1"]["search_codes"] == []
    assert products["gid://shopify/Product/1"]["ref_tags"] == []
