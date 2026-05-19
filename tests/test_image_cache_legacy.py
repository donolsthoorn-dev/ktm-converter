"""Legacy image_cache.json entries (boolean true) vs dict {url: ...}."""

from __future__ import annotations

import json
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


def test_try_resolve_invalidates_dead_dict_url():
    dead = "https://cdn.shopify.com/s/files/1/2/3/files/foo.jpg"
    cache = {"foo.jpg": {"url": dead}}
    with patch.object(image_manager, "url_is_reachable", return_value=False):
        u = image_manager.try_resolve_image_cache_or_cdn("foo.jpg", cache)
    assert u is None
    assert "foo.jpg" not in cache


def test_try_resolve_keeps_dict_url_when_reachable():
    url = "https://cdn.shopify.com/s/files/1/2/3/files/foo.jpg"
    cache = {"foo.jpg": {"url": url}}
    with patch.object(image_manager, "url_is_reachable", return_value=True):
        u = image_manager.try_resolve_image_cache_or_cdn("foo.jpg", cache)
    assert u == url
    assert cache["foo.jpg"]["url"] == url


def test_try_resolve_skip_verify_keeps_dead_dict_url():
    dead = "https://cdn.shopify.com/s/files/1/2/3/files/foo.jpg"
    cache = {"foo.jpg": {"url": dead}}
    with patch.dict(image_manager.os.environ, {"KTM_IMAGE_SKIP_CACHED_URL_VERIFY": "1"}):
        with patch.object(image_manager, "url_is_reachable", return_value=False):
            u = image_manager.try_resolve_image_cache_or_cdn("foo.jpg", cache)
    assert u == dead
    assert cache["foo.jpg"]["url"] == dead


def test_load_cache_quarantines_invalid_json(tmp_path, monkeypatch) -> None:
    cache_file = tmp_path / "image_cache.json"
    monkeypatch.setattr(image_manager, "CACHE_FILE", str(cache_file))
    cache_file.write_text('{"ok.jpg": {"url": "http://x"}\n', encoding="utf-8")
    loaded = image_manager.load_cache()
    assert loaded == {}
    assert not cache_file.exists()
    assert list(tmp_path.glob("image_cache.json.corrupt-*"))


def test_save_cache_safe_while_cache_mutates(tmp_path, monkeypatch) -> None:
    """Parallel workers mogen cache wijzigen; save_cache_safe mag niet crashen."""
    import threading

    cache_file = tmp_path / "image_cache.json"
    monkeypatch.setattr(image_manager, "CACHE_FILE", str(cache_file))
    cache: dict = {"a.jpg": {"url": "https://example.com/a.jpg"}}
    errors: list[BaseException] = []

    def mutator() -> None:
        for n in range(200):
            with image_manager._cache_mut_lock:
                cache[f"k{n}.jpg"] = {"url": f"https://example.com/{n}.jpg"}

    def saver() -> None:
        try:
            for _ in range(30):
                image_manager.save_cache_safe(cache)
        except BaseException as e:
            errors.append(e)

    threads = [threading.Thread(target=mutator), threading.Thread(target=saver)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
        assert not t.is_alive(), "timeout in save_cache_safe concurrency test"
    assert not errors, errors
    assert cache_file.is_file()
    loaded = json.loads(cache_file.read_text(encoding="utf-8"))
    assert isinstance(loaded, dict)


def test_try_resolve_disallow_guessed_cdn_skips_reachable_build_url():
    cache: dict = {}
    with patch.object(image_manager, "url_is_reachable", return_value=True):
        u = image_manager.try_resolve_image_cache_or_cdn(
            "foo.jpg", cache, allow_guessed_cdn=False
        )
    assert u is None
    assert "foo.jpg" not in cache
