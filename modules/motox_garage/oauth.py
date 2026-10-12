"""OAuth-callback van de garage-app: handtekening en code-ruil.

De productiewinkel staat niet in dezelfde Dev Dashboard-organisatie, dus
client credentials geven ``shop_not_permitted``. De offline token komt
via de authorization-code grant.
"""

from __future__ import annotations

import hmac
from hashlib import sha256
from urllib.parse import parse_qsl

import requests


def verify_oauth_hmac(query_string: str, secret: str) -> bool:
    """Shopify OAuth: parameters gesorteerd, met & ertussen, zonder hmac."""
    if not secret:
        return False
    pairs = parse_qsl(query_string, keep_blank_values=True)
    digest = ""
    message_parts: list[str] = []
    for key, value in pairs:
        if key == "hmac":
            digest = value
            continue
        message_parts.append((key, value))
    if not digest:
        return False
    message = "&".join(f"{key}={value}" for key, value in sorted(message_parts))
    expected = hmac.new(secret.encode("utf-8"), message.encode("utf-8"), sha256).hexdigest()
    return hmac.compare_digest(expected, digest)


def exchange_offline_token(
    shop: str,
    client_id: str,
    client_secret: str,
    code: str,
) -> str:
    response = requests.post(
        f"https://{shop}/admin/oauth/access_token",
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "code": code,
        },
        timeout=(15, 60),
    )
    if response.status_code >= 400:
        raise RuntimeError("code-ruil mislukt")
    token = (response.json() or {}).get("access_token") or ""
    if not token:
        raise RuntimeError("geen offline token")
    return token
