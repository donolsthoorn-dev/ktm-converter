"""CSV Image Src blijft leeg als de gegokte CDN-URL geen bestand is."""

from __future__ import annotations

from unittest.mock import patch

import modules.exporter as exporter


def test_pick_best_cdn_url_omits_unreachable_guess():
    files_url = "https://cdn.shopify.com/s/files/1/1/1/files/missing.jpg"
    products_url = "https://cdn.shopify.com/s/files/1/1/1/products/missing.jpg"
    exporter._csv_image_choice_cache["missing.jpg"] = "files"
    exporter._csv_image_reachability_cache.clear()

    with patch.object(exporter, "_is_url_reachable", return_value=False):
        assert exporter._pick_best_cdn_url(files_url, products_url, "auto") == ""

    assert "missing.jpg" not in exporter._csv_image_choice_cache


def test_pick_best_cdn_url_keeps_reachable_files_url():
    files_url = "https://cdn.shopify.com/s/files/1/1/1/files/ok.jpg"
    products_url = "https://cdn.shopify.com/s/files/1/1/1/products/ok.jpg"
    exporter._csv_image_choice_cache.clear()
    exporter._csv_image_reachability_cache.clear()

    def reachable(url: str) -> bool:
        return "/files/" in url

    with patch.object(exporter, "_is_url_reachable", side_effect=reachable):
        assert exporter._pick_best_cdn_url(files_url, products_url, "auto") == files_url

    assert exporter._csv_image_choice_cache["ok.jpg"] == "files"


def test_normalize_image_url_skips_fallback_placeholder():
    assert (
        exporter.normalize_image_url(
            "PHO/FALLBACK/pho_fallback_fallback_no_picture__sall__awsg__v1.jpg"
        )
        == ""
    )
    assert (
        exporter.normalize_image_url(
            "https://cdn.shopify.com/s/files/1/1/1/files/pho_fallback_fallback_no_picture__sall__awsg__v1.jpg?v=1"
        )
        == ""
    )
