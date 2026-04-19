"""Legacy image_cache.json entries (boolean true) vs dict {url: ...}."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import modules.image_manager as image_manager


def test_try_resolve_migrates_legacy_true_when_reachable():
    cache = {"foo.jpg": True}
    with patch.object(image_manager, "url_is_reachable", return_value=True):
        u = image_manager.try_resolve_image_cache_or_cdn("foo.jpg", cache)
    assert u and str(u).startswith("http")
    assert isinstance(cache.get("foo.jpg"), dict)
    assert cache["foo.jpg"]["url"] == u


def test_try_resolve_drops_legacy_true_when_unreachable():
    cache = {"foo.jpg": True}
    with patch.object(image_manager, "url_is_reachable", return_value=False):
        u = image_manager.try_resolve_image_cache_or_cdn("foo.jpg", cache)
    assert u is None
    assert "foo.jpg" not in cache


def test_resolve_without_upload_does_not_mutate_cache():
    cache = {"foo.jpg": True}
    with patch.object(image_manager, "url_is_reachable", return_value=True):
        u = image_manager.resolve_image_url_without_upload(
            "foo.jpg", Path("x/foo.jpg"), cache, use_network=True
        )
    assert u and str(u).startswith("http")
    assert cache.get("foo.jpg") is True
