#!/usr/bin/env python3
"""
Verwijder dubbele **afbeeldingen** (MediaImage) in Shopify Content → Files.

- Alleen bestanden waarvan de **bestandsnaam** (uit de CDN-URL) begint met een prefix
  (default **`pho`**, case-insensitive).
- Alleen bestanden die volgens Shopify **nergens aan gekoppeld** zijn: zoekfilter
  **`used_in:none`** (geen product, pagina, thema, enz. — zie Shopify-docs voor exacte dekking).

Groepering: basename met `__vN` vóór de extensie gestript — zelfde regels als
`modules/image_manager._canonical_filename_match`.

Standaard alleen **dry-run** (rapport). Voeg **`--apply`** toe om echt te verwijderen (definitief).

**Twee modi**

- **Standaard:** alleen **dubbele** ongebruikte bestanden (zelfde canonieke naam, meerdere `__vN`).
- **`--all-unused`:** **alle** ongebruikte bestanden met de prefix (niet alleen dubbele kopieën).

Scopes: `read_files` (of `read_images`) om te lezen; **`write_files`** (of `write_images`) om te verwijderen.

Voorbeelden:

  python3 scripts/shopify_delete_duplicate_images.py
  python3 scripts/shopify_delete_duplicate_images.py --apply --keep oldest
  python3 scripts/shopify_delete_duplicate_images.py --all-unused --apply --limit 200
  python3 scripts/shopify_delete_duplicate_images.py --apply --limit 50 --prefix pho
"""

from __future__ import annotations

import argparse
import os
import re
import sys
import time
from collections import defaultdict
from datetime import datetime
from urllib.parse import unquote

# Project root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config  # noqa: E402 — laadt .env

import requests  # noqa: E402

_REQUEST_TIMEOUT = (12, 180)

_GQL_LIST = """
query KtmFilesPage($cursor: String, $q: String!) {
  files(first: 250, after: $cursor, query: $q, sortKey: CREATED_AT, reverse: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    edges {
      node {
        __typename
        ... on MediaImage {
          id
          createdAt
          image {
            url
          }
        }
      }
    }
  }
}
"""

_GQL_DELETE = """
mutation KtmFileDelete($fileIds: [ID!]!) {
  fileDelete(fileIds: $fileIds) {
    deletedFileIds
    userErrors {
      field
      message
      code
    }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def _graphql_post(
    sess: requests.Session,
    api_url: str,
    token: str,
    query: str,
    variables: dict | None = None,
) -> dict:
    payload: dict = {"query": query}
    if variables is not None:
        payload["variables"] = variables
    last: dict = {}
    for attempt in range(20):
        r = sess.post(
            api_url,
            headers={
                "Content-Type": "application/json",
                "X-Shopify-Access-Token": token,
            },
            json=payload,
            timeout=_REQUEST_TIMEOUT,
            proxies={"http": None, "https": None},
        )
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        throttled = any(
            (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
        )
        if throttled and attempt < 19:
            delay = min(2.0 * (1.45**attempt), 60.0)
            time.sleep(delay)
            continue
        if errs:
            return last
        return last
    return last


def _basename_from_url(url: str) -> str:
    return unquote(url.split("?", 1)[0].rsplit("/", 1)[-1])


def _canonical_key(basename: str) -> str:
    a = basename.lower()
    stem, ext = os.path.splitext(a)
    stem = re.sub(r"__v\d+$", "", stem, count=1, flags=re.IGNORECASE)
    return f"{stem}{ext}"


def _parse_created_at(raw: str | None) -> datetime | None:
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(s)
    except ValueError:
        return None


def _v_suffix_num(basename: str) -> int:
    stem, _ext = os.path.splitext(basename)
    m = re.search(r"__v(\d+)$", stem, re.IGNORECASE)
    return int(m.group(1)) if m else 0


def _pick_keep_index(
    items: list[tuple[str, str, datetime | None]],
    keep: str,
) -> int:
    """
    items: (id, basename, createdAt)
    Return index of item to **keep**; others are candidates for deletion.
    """
    if len(items) == 1:
        return 0

    def metrics(i: int) -> tuple[float, int, str]:
        fid, base, dt = items[i]
        v = _v_suffix_num(base)
        if dt:
            ts = dt.timestamp()
        else:
            ts = float(v)
        return (ts, v, fid)

    if keep == "newest":
        return max(range(len(items)), key=metrics)
    return min(range(len(items)), key=metrics)


def _search_query_unused_with_prefix(prefix: str) -> str:
    """
    Shopify files-zoekfilter: ongebruikte bestanden + optioneel filename-prefix.
    `filename:pho*` beperkt resultaten (minder pagina's); client-side prefix-check blijft nodig als fallback.
    """
    p = (prefix or "").strip()
    if not p:
        return "used_in:none"
    return f"used_in:none filename:{p}*"


def _basename_matches_prefix(basename: str, prefix: str) -> bool:
    return basename.lower().startswith(prefix.lower())


def _fetch_unused_pho_media_images(
    sess: requests.Session,
    api_url: str,
    token: str,
    prefix: str,
) -> list[tuple[str, str, str, datetime | None]]:
    """
    Return list of (id, basename, canonical_key, createdAt) for MediaImage rows that are
    unused (`used_in:none`) and whose basename starts with `prefix`.
    """
    q = _search_query_unused_with_prefix(prefix)
    broad_fallback = False
    cursor: str | None = None
    rows: list[tuple[str, str, str, datetime | None]] = []
    while True:
        body = _graphql_post(
            sess,
            api_url,
            token,
            _GQL_LIST,
            {"cursor": cursor, "q": q},
        )
        if body.get("errors"):
            errs = body["errors"]
            throttled = any(
                (e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs
            )
            if (
                not broad_fallback
                and cursor is None
                and not throttled
            ):
                broad_fallback = True
                q = "used_in:none"
                cursor = None
                rows = []
                print(
                    "Smalle files-query niet bruikbaar — overschakelen naar used_in:none "
                    "met prefix-filter in het script (kan langer duren).",
                    flush=True,
                )
                continue
            print("GraphQL errors:", errs, file=sys.stderr)
            raise SystemExit(1)

        data = (body.get("data") or {}).get("files") or {}
        edges = data.get("edges") or []
        for e in edges:
            node = (e or {}).get("node") or {}
            if node.get("__typename") != "MediaImage":
                continue
            fid = node.get("id")
            img = node.get("image")
            if not fid or not isinstance(img, dict):
                continue
            u = img.get("url")
            if not isinstance(u, str) or not u.startswith("http"):
                continue
            base = _basename_from_url(u)
            if not base or not _basename_matches_prefix(base, prefix):
                continue
            ck = _canonical_key(base)
            cat = _parse_created_at(node.get("createdAt"))
            rows.append((str(fid), base, ck, cat))

        pi = data.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        if not cursor:
            break
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Shopify MediaImage opruimen: prefix + used_in:none. Standaard alleen dubbele "
            "kopieën; met --all-unused alle ongebruikte bestanden met die prefix."
        )
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Voer fileDelete uit (zonder deze flag: alleen rapport).",
    )
    ap.add_argument(
        "--keep",
        choices=("newest", "oldest"),
        default="newest",
        help="Welke versie per groep behouden (default: nieuwste createdAt).",
    )
    ap.add_argument(
        "--limit",
        type=int,
        default=0,
        metavar="N",
        help="Maximaal N bestanden verwijderen (0 = geen limiet). Alleen met --apply.",
    )
    ap.add_argument(
        "--batch-size",
        type=int,
        default=25,
        metavar="N",
        help="Aantal file-IDs per fileDelete-mutatie (default 25).",
    )
    ap.add_argument(
        "--prefix",
        default="pho",
        metavar="STR",
        help="Alleen bestandsnamen die hiermee beginnen (case-insensitive; default: pho).",
    )
    ap.add_argument(
        "--all-unused",
        action="store_true",
        help=(
            "Verwijder alle ongebruikte bestanden met deze prefix (niet alleen dubbele kopieën). "
            "Zonder --apply: rapport; met --apply: definitief. Combineer met --limit om te testen."
        ),
    )
    args = ap.parse_args()
    if args.limit < 0 or args.batch_size < 1:
        print("--limit en --batch-size moeten geldig zijn.", file=sys.stderr)
        return 1

    token = config.SHOPIFY_ACCESS_TOKEN
    if not token:
        print("SHOPIFY_ACCESS_TOKEN ontbreekt (.env).", file=sys.stderr)
        return 1

    shop = config.SHOPIFY_SHOP_SLUG
    ver = config.SHOPIFY_ADMIN_API_VERSION
    api_url = f"https://{shop}.myshopify.com/admin/api/{ver}/graphql.json"

    sess = _session()
    print(
        f"Ongebruikte MediaImage-bestanden ophalen (prefix '{args.prefix}', used_in:none)…",
        flush=True,
    )
    rows = _fetch_unused_pho_media_images(sess, api_url, token, args.prefix)

    by_canon: dict[str, list[tuple[str, str, datetime | None]]] = defaultdict(list)
    for fid, base, ck, cat in rows:
        by_canon[ck].append((fid, base, cat))

    dup_groups = {k: v for k, v in by_canon.items() if len(v) > 1}
    to_delete: list[tuple[str, str, str]] = []  # id, basename, canonical_key

    if args.all_unused:
        for fid, base, ck, _cat in rows:
            to_delete.append((fid, base, ck))
        mode_label = "alle ongebruikte (prefix)"
    else:
        for ck, items in dup_groups.items():
            simplified: list[tuple[str, str, datetime | None]] = [
                (fid, base, cat) for fid, base, cat in items
            ]
            keep_i = _pick_keep_index(simplified, args.keep)
            for i, (fid, base, cat) in enumerate(items):
                if i != keep_i:
                    to_delete.append((fid, base, ck))
        mode_label = f"alleen dubbele kopieën (behoud per groep: {args.keep})"

    to_delete.sort(key=lambda t: (t[2], t[1], t[0]))

    n_extra = len(to_delete)
    print(f"Ongebruikte MediaImage (prefix, met URL): {len(rows)}")
    print(f"Unieke canonieke namen: {len(by_canon)}")
    print(f"Dubbele groepen: {len(dup_groups)}")
    print(f"Modus: {mode_label}")
    print(f"Te verwijderen: {n_extra}")

    if (
        not args.all_unused
        and len(rows) > 0
        and len(dup_groups) == 0
    ):
        print(
            "\nUitleg: elk ongebruikt bestand heeft een andere logische naam (geen tweede "
            "kopie met dezelfde naam na __vN). Er valt dus niets te ‘dedupliceren’.\n"
            "Wil je wél alle ongebruikte pho*-bestanden weghalen? Gebruik dan "
            "`--all-unused` (eventueel met `--limit` om te testen).",
            flush=True,
        )

    if not to_delete:
        return 0

    if not args.apply:
        print("\nDry-run — geen wijzigingen. Voeg --apply toe om te verwijderen.")
        for i, (fid, base, ck) in enumerate(to_delete[:30]):
            print(f"  zou verwijderen: {base}  ({fid})")
        if len(to_delete) > 30:
            print(f"  … en nog {len(to_delete) - 30} andere(s).")
        return 0

    # --apply
    limit = args.limit if args.limit > 0 else len(to_delete)
    ids = [fid for fid, _b, _c in to_delete[:limit]]
    if len(ids) < len(to_delete) and args.limit > 0:
        print(f"Let op: --limit {args.limit} — alleen {len(ids)} van {n_extra} verwijderen.")

    batch = max(1, min(args.batch_size, 250))
    deleted = 0
    errs = 0
    for start in range(0, len(ids), batch):
        chunk = ids[start : start + batch]
        body = _graphql_post(
            sess,
            api_url,
            token,
            _GQL_DELETE,
            {"fileIds": chunk},
        )
        if body.get("errors"):
            print("fileDelete GraphQL errors:", body["errors"], file=sys.stderr)
            errs += len(chunk)
            continue
        fd = (body.get("data") or {}).get("fileDelete") or {}
        uerr = fd.get("userErrors") or []
        if uerr:
            for e in uerr:
                print(f"  userError: {e}", file=sys.stderr)
            errs += len(uerr)
        dids = fd.get("deletedFileIds") or []
        deleted += len(dids)
        print(f"  Verwijderd batch {start // batch + 1}: {len(dids)} bestand(en).", flush=True)
        time.sleep(0.25)

    print(f"Klaar: {deleted} bestand(en) door API als verwijderd gerapporteerd.")
    if errs:
        print(f"Let op: er traden fouten op (zie stderr).", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
