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
