"""Stable content hash for canonical fits_on JSON (diff push naar Shopify)."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def ymm_json_content_hash(ymm_json: dict[str, Any]) -> str:
    """SHA-256 van genormaliseerde JSON (gesorteerde keys)."""
    payload = json.dumps(ymm_json, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def needs_shopify_push(content_hash: str, pushed_hash: str | None) -> bool:
    if not content_hash:
        return False
    if not pushed_hash:
        return True
    return content_hash != pushed_hash
