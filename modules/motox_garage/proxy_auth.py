"""Handtekening van een Shopify app-proxy request.

Zie https://shopify.dev/docs/apps/build/online-store/app-proxies/authenticate-app-proxies
"""

from __future__ import annotations

import hmac
import time
from hashlib import sha256
from urllib.parse import parse_qs


def canonical_params(params: dict[str, str | list[str]]) -> str:
    parts: list[str] = []
    for key in sorted(params):
        if key == "signature":
            continue
        value = params[key]
        rendered = ",".join(value) if isinstance(value, list) else value
        parts.append(f"{key}={rendered}")
    return "".join(parts)


def sign(secret: str, params: dict[str, str | list[str]]) -> str:
    digest = hmac.new(secret.encode("utf-8"), canonical_params(params).encode("utf-8"), sha256)
    return digest.hexdigest()


def params_from_query(query_string: str) -> dict[str, list[str]]:
    parsed = parse_qs(query_string, keep_blank_values=True)
    return {key: values for key, values in parsed.items()}


def verify_proxy_query(
    query_string: str,
    secret: str,
    *,
    now: float | None = None,
    max_age: int = 600,
) -> bool:
    params = params_from_query(query_string)
    signature = (params.get("signature") or [""])[0]
    if not signature or not secret:
        return False
    unsigned = {key: value for key, value in params.items() if key != "signature"}
    expected = sign(secret, unsigned)
    if not hmac.compare_digest(expected, signature):
        return False
    raw_ts = (params.get("timestamp") or [""])[0]
    if not raw_ts.isdigit():
        return False
    current = time.time() if now is None else now
    return abs(current - int(raw_ts)) <= max_age
