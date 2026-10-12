"""Shopify Admin GraphQL: collecties met smart-collection ruleSet."""

from __future__ import annotations

import json

import requests

COLLECTIONS_QUERY = """
query CollectionsPage($cursor: String) {
  collections(first: 100, after: $cursor, sortKey: TITLE) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      handle
      updatedAt
      ruleSet {
        appliedDisjunctively
        rules {
          column
          relation
          condition
        }
      }
    }
  }
}
"""


def http_session() -> requests.Session:
    sess = requests.Session()
    sess.trust_env = False
    return sess


def graphql_post(
    shop: str,
    token: str,
    api_version: str,
    query: str,
    variables: dict | None = None,
    sess: requests.Session | None = None,
) -> dict:
    sess = sess or http_session()
    url = f"https://{shop}/admin/api/{api_version}/graphql.json"
    body: dict = {"query": query}
    if variables is not None:
        body["variables"] = variables
    r = sess.post(
        url,
        headers={
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        },
        data=json.dumps(body),
        timeout=(15, 120),
        proxies={"http": None, "https": None},
    )
    if r.status_code == 429:
        raise RuntimeError("GraphQL 429: rate limit — probeer het zo opnieuw.")
    r.raise_for_status()
    out = r.json()
    errs = out.get("errors")
    if errs:
        raise RuntimeError(json.dumps(errs, indent=2))
    return out


def fetch_all_collections(
    shop: str,
    token: str,
    api_version: str,
) -> list[dict]:
    sess = http_session()
    out: list[dict] = []
    cursor: str | None = None
    while True:
        data = graphql_post(
            shop,
            token,
            api_version,
            COLLECTIONS_QUERY,
            {"cursor": cursor},
            sess=sess,
        )
        coll = data.get("data", {}).get("collections")
        if not coll:
            raise RuntimeError(f"Onverwacht antwoord: {json.dumps(data, indent=2)[:2000]}")
        nodes = coll.get("nodes") or []
        for n in nodes:
            out.append(n)
        page = coll.get("pageInfo") or {}
        if not page.get("hasNextPage"):
            break
        cursor = page.get("endCursor")
        if not cursor:
            break
    return out


COLLECTION_UPDATE = """
mutation CollectionUpdate($input: CollectionInput!) {
  collectionUpdate(input: $input) {
    collection {
      id
      title
      handle
      productsCount { count }
      ruleSet {
        appliedDisjunctively
        rules {
          column
          relation
          condition
        }
      }
    }
    userErrors { field message }
  }
}
"""

COLLECTION_CREATE = """
mutation CollectionCreate($input: CollectionInput!) {
  collectionCreate(input: $input) {
    collection {
      id
      title
      handle
      productsCount { count }
      ruleSet {
        appliedDisjunctively
        rules {
          column
          relation
          condition
        }
      }
    }
    userErrors { field message }
  }
}
"""

PUBLISHABLE_PUBLISH = """
mutation PublishablePublish($id: ID!, $input: [PublicationInput!]!) {
  publishablePublish(id: $id, input: $input) {
    userErrors { field message }
  }
}
"""

PRODUCT_PUBLISH_ONLINE = """
mutation ProductPublishOnline($input: ProductInput!) {
  productUpdate(input: $input) {
    product { id publishedAt }
    userErrors { field message }
  }
}
"""

# Shopify smart collections standaard max. 60 voorwaarden.
MAX_SMART_RULES = 60


def rule_to_input(rule: dict) -> dict:
    """Zet een opgehaalde regel om naar CollectionRuleInput."""
    out = {
        "column": rule.get("column"),
        "relation": rule.get("relation"),
        "condition": rule.get("condition") or "",
    }
    oid = rule.get("conditionObjectId")
    if oid:
        out["conditionObjectId"] = oid
    return out


def type_equals_rule(product_type: str) -> dict:
    return {
        "column": "TYPE",
        "relation": "EQUALS",
        "condition": product_type,
    }


def merge_type_equals(
    existing_rules: list[dict],
    extra_types: list[str],
    *,
    limit: int = MAX_SMART_RULES,
) -> tuple[list[dict], list[str], list[str]]:
    """Voeg TYPE EQUALS-regels toe. Retourneert (rules, added, skipped_over_limit)."""
    merged = [rule_to_input(r) for r in existing_rules]
    have = {
        (r["column"], r["relation"], r["condition"])
        for r in merged
    }
    added: list[str] = []
    skipped: list[str] = []
    for t in extra_types:
        key = ("TYPE", "EQUALS", t)
        if key in have:
            continue
        if len(merged) >= limit:
            skipped.append(t)
            continue
        merged.append(type_equals_rule(t))
        have.add(key)
        added.append(t)
    return merged, added, skipped


def collection_update_rules(
    shop: str,
    token: str,
    api_version: str,
    collection_id: str,
    rules: list[dict],
    *,
    applied_disjunctively: bool = True,
    sess: requests.Session | None = None,
) -> dict:
    data = graphql_post(
        shop,
        token,
        api_version,
        COLLECTION_UPDATE,
        {
            "input": {
                "id": collection_id,
                "ruleSet": {
                    "appliedDisjunctively": applied_disjunctively,
                    "rules": rules,
                },
            }
        },
        sess=sess,
    )
    payload = ((data.get("data") or {}).get("collectionUpdate")) or {}
    errs = payload.get("userErrors") or []
    if errs:
        raise RuntimeError(json.dumps(errs, ensure_ascii=False, indent=2))
    return payload.get("collection") or {}
