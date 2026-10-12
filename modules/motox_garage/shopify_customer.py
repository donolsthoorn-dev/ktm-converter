"""Klant-metafield ``motox.garage`` via de Admin API."""

from __future__ import annotations

import json
import time
from typing import Any, Protocol

import requests

from modules.motox_garage.model import coerce_active, empty_garage, normalize_garage

NAMESPACE = "motox"
KEY = "garage"

CUSTOMER_QUERY = """
query CustomerGarage($id: ID!) {
  customer(id: $id) {
    id
    metafield(namespace: "motox", key: "garage") {
      value
    }
  }
}
"""

SET_MUTATION = """
mutation SetGarage($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    userErrors { field message code }
  }
}
"""

DEFINITION_MUTATION = """
mutation GarageDefinition {
  metafieldDefinitionCreate(
    definition: {
      name: "Motor garage"
      namespace: "motox"
      key: "garage"
      type: "json"
      ownerType: CUSTOMER
    }
  ) {
    createdDefinition { id }
    userErrors { message code }
  }
}
"""


class CustomerNotFound(LookupError):
    pass


class ShopifyGarageError(RuntimeError):
    pass


class CustomerStore(Protocol):
    def get(self, customer_id: str) -> dict[str, Any]: ...

    def put(self, customer_id: str, garage: dict[str, Any]) -> None: ...


class MemoryCustomers:
    def __init__(self) -> None:
        self.data: dict[str, dict[str, Any]] = {}

    def get(self, customer_id: str) -> dict[str, Any]:
        return normalize_garage(self.data.get(customer_id) or empty_garage())

    def put(self, customer_id: str, garage: dict[str, Any]) -> None:
        self.data[customer_id] = normalize_garage(garage)


class ShopifyCustomers:
    def __init__(
        self,
        shop: str,
        token: str,
        api_version: str,
        *,
        client_id: str = "",
        client_secret: str = "",
    ) -> None:
        self.shop = shop
        self.token = token
        self.api_version = api_version
        self.client_id = client_id
        self.client_secret = client_secret
        self._expires_at = 0.0
        self._definition_ready = False
        self._session = requests.Session()
        self._session.trust_env = False

    def _access_token(self) -> str:
        if self.client_id and self.client_secret:
            if not self.token or time.time() >= self._expires_at - 60:
                self._refresh_client_credentials()
        if not self.token:
            raise ShopifyGarageError("geen admin-token")
        return self.token

    def _refresh_client_credentials(self) -> None:
        response = self._session.post(
            f"https://{self.shop}/admin/oauth/access_token",
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=(15, 60),
            proxies={"http": None, "https": None},
        )
        if response.status_code >= 400:
            raise ShopifyGarageError(
                "Garage-app heeft nog geen toegang tot klanten. Installeer de app op Motox."
            )
        payload = response.json()
        token = payload.get("access_token") or ""
        if not token:
            raise ShopifyGarageError("Shopify gaf geen toegangstoken")
        self.token = token
        self._expires_at = time.time() + int(payload.get("expires_in") or 3600)

    def _graphql(self, query: str, variables: dict | None = None) -> dict:
        url = f"https://{self.shop}/admin/api/{self.api_version}/graphql.json"
        body: dict[str, Any] = {"query": query}
        if variables is not None:
            body["variables"] = variables
        response = self._session.post(
            url,
            headers={
                "X-Shopify-Access-Token": self._access_token(),
                "Content-Type": "application/json",
            },
            data=json.dumps(body),
            timeout=(15, 60),
            proxies={"http": None, "https": None},
        )
        if response.status_code == 429:
            raise ShopifyGarageError("Shopify rate limit")
        response.raise_for_status()
        payload = response.json()
        if payload.get("errors"):
            raise ShopifyGarageError(json.dumps(payload["errors"]))
        return payload

    def ensure_definition(self) -> None:
        if self._definition_ready:
            return
        payload = self._graphql(DEFINITION_MUTATION)
        errors = payload["data"]["metafieldDefinitionCreate"]["userErrors"]
        for error in errors:
            code = str(error.get("code") or "").upper()
            message = str(error.get("message") or "").lower()
            taken = code in {"TAKEN", "UNSTRUCTURED_ALREADY_EXISTS"} or any(
                word in message for word in ("taken", "already", "in use", "exists")
            )
            if not taken:
                raise ShopifyGarageError(error.get("message") or "metafield definition")
        self._definition_ready = True

    def get(self, customer_id: str) -> dict[str, Any]:
        gid = f"gid://shopify/Customer/{customer_id}"
        payload = self._graphql(CUSTOMER_QUERY, {"id": gid})
        customer = (payload.get("data") or {}).get("customer")
        if not customer:
            raise CustomerNotFound(customer_id)
        metafield = customer.get("metafield") or {}
        return normalize_garage(metafield.get("value"))

    def put(self, customer_id: str, garage: dict[str, Any]) -> None:
        self.ensure_definition()
        stored = coerce_active(garage)
        gid = f"gid://shopify/Customer/{customer_id}"
        payload = self._graphql(
            SET_MUTATION,
            {
                "metafields": [
                    {
                        "ownerId": gid,
                        "namespace": NAMESPACE,
                        "key": KEY,
                        "type": "json",
                        "value": json.dumps(stored, separators=(",", ":")),
                    }
                ]
            },
        )
        errors = payload["data"]["metafieldsSet"]["userErrors"]
        if errors:
            raise ShopifyGarageError(errors[0].get("message") or "metafieldsSet")
