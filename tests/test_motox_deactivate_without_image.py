"""Regels voor Motox-deactivatie van producten zonder afbeelding."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "motox_deactivate_products_without_image.py"


def _load():
    spec = importlib.util.spec_from_file_location("motox_deactivate_no_image", SCRIPT)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_only_active_without_featured_image_is_drafted():
    mod = _load()
    assert mod.should_set_draft("ACTIVE", False) is True
    assert mod.should_set_draft("active", True) is False
    assert mod.should_set_draft("DRAFT", False) is False
    assert mod.should_set_draft("ARCHIVED", False) is False
    assert mod.has_featured_image(None) is False
    assert mod.has_featured_image({"id": "gid://shopify/ProductImage/1"}) is True


def test_parse_bulk_jsonl_links_skus(tmp_path: Path):
    mod = _load()
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
        {
            "id": "gid://shopify/Product/2",
            "handle": "with-photo",
            "title": "Helm",
            "status": "ACTIVE",
            "featuredImage": {"id": "gid://shopify/ProductImage/3"},
        },
    ]
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
    products = mod.parse_bulk_jsonl(path)
    assert len(products) == 2
    assert products[0]["skus"] == ["8006904002"]
    assert products[0]["has_image"] is False
    assert products[1]["has_image"] is True
    drafts = [p for p in products if mod.should_set_draft(p["status"], p["has_image"])]
    assert [p["handle"] for p in drafts] == ["8006904"]
