"""Eén Shopify bulk-export van de productcatalogus.

Shopify staat één lopende bulk-query per shop toe. Geneste verbindingen
komen als aparte JSONL-regels met __parentId; productvelden staan op de
productregel.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from pathlib import Path

import requests

_DOWNLOAD_TIMEOUT = (15, 300)

_BULK_QUERY = """{
  products {
    edges {
      node {
        id
        handle
        title
        status
        productType
        tags
        descriptionHtml
        category { id fullName }
      }
    }
  }
}"""

_GQL_BULK_START = """
mutation CategoryCatalogBulk {
  bulkOperationRunQuery(query: BULK_QUERY_PLACEHOLDER) {
    bulkOperation { id status }
    userErrors { field message }
  }
}
"""

_GQL_CURRENT = """
query {
  currentBulkOperation {
    id
    status
    errorCode
    objectCount
  }
}
"""

_GQL_POLL = """
query CategoryCatalogPoll($id: ID!) {
  node(id: $id) {
    ... on BulkOperation {
      status
      errorCode
      objectCount
      url
    }
  }
}
"""

Gql = Callable[[str, dict | None], dict]


def download_product_catalog(gql: Gql, dest: Path) -> None:
    """Start een bulk-export, wacht tot hij klaar is en schrijf de JSONL naar dest."""
    _wait_current_bulk(gql)
    mutation = _GQL_BULK_START.replace(
        "BULK_QUERY_PLACEHOLDER", json.dumps(_BULK_QUERY)
    )
    op_id = ""
    for attempt in range(5):
        body = gql(mutation, None)
        if body.get("errors"):
            raise RuntimeError(body["errors"])
        run = ((body.get("data") or {}).get("bulkOperationRunQuery")) or {}
        uerr = run.get("userErrors") or []
        if uerr:
            msg = str(uerr)
            if "already in progress" in msg.lower() and attempt < 4:
                print(f"  Bulk bezet, opnieuw proberen ({attempt + 1})", flush=True)
                time.sleep(8)
                _wait_current_bulk(gql)
                continue
            raise RuntimeError(f"Bulk geweigerd: {uerr}")
        op_id = (run.get("bulkOperation") or {}).get("id") or ""
        if op_id:
            break
        time.sleep(3)
    if not op_id:
        raise RuntimeError("Geen bulk operation id.")

    print(f"Bulk-export gestart ({op_id})", flush=True)
    poll_interval = 3.0
    url = ""
    while True:
        poll = gql(_GQL_POLL, {"id": op_id})
        node = ((poll.get("data") or {}).get("node")) or {}
        status = (node.get("status") or "").upper()
        print(f"  {status} — objecten: {node.get('objectCount')}", flush=True)
        if status == "COMPLETED":
            url = node.get("url") or ""
            break
        if status in ("FAILED", "CANCELED", "CANCELLED", "EXPIRED"):
            raise RuntimeError(f"Bulk {status}: {node.get('errorCode')}")
        time.sleep(poll_interval)
        poll_interval = min(poll_interval + 0.5, 15.0)

    dest.parent.mkdir(parents=True, exist_ok=True)
    if not url:
        dest.write_text("", encoding="utf-8")
        print("Bulk klaar, lege catalogus", flush=True)
        return

    sess = requests.Session()
    sess.trust_env = False
    with sess.get(
        url,
        stream=True,
        timeout=_DOWNLOAD_TIMEOUT,
        proxies={"http": None, "https": None},
    ) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(1024 * 256):
                if chunk:
                    f.write(chunk)
    size_mb = dest.stat().st_size / (1024 * 1024)
    print(f"Bulk gedownload: {dest.name} ({size_mb:.1f} MB)", flush=True)


def iter_bulk_products(path: Path) -> Iterator[dict]:
    """Productregels uit de JSONL. Geneste verbindingen hebben __parentId."""
    with path.open(encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"Ongeldige JSONL regel {line_no}: {exc}") from exc
            if obj.get("__parentId"):
                continue
            gid = str(obj.get("id") or "")
            if not gid.startswith("gid://shopify/Product/"):
                continue
            tags = obj.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]
            obj["tags"] = tags
            yield obj


def _wait_current_bulk(gql: Gql) -> None:
    while True:
        body = gql(_GQL_CURRENT, None)
        cur = ((body.get("data") or {}).get("currentBulkOperation")) or {}
        status = (cur.get("status") or "").upper()
        if not status or status in ("COMPLETED", "FAILED", "CANCELED", "CANCELLED"):
            return
        print(
            f"  Wacht op lopende bulk: {status} ({cur.get('objectCount')})",
            flush=True,
        )
        time.sleep(5)
