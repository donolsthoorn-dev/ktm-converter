#!/usr/bin/env python3
"""Sync CBEXPDN MUSS/EMPF/ET relations naar Shopify product-metafields (ktm-shop).

Complementary (Aanbevolen-block):
  MUSSARTIKEL + EMPFART2ART → shopify--discovery--product_recommendation.complementary_products

Bijbehorende onderdelen (data; theme later):
  ET2ARTIKEL → global.et_articles (list.product_reference)

Gebruik:
  python3 scripts/shopify_sync_empfart_complementary.py --dry-run --sku A62409960044
  python3 scripts/shopify_sync_empfart_complementary.py --sku A62409960044
  python3 scripts/shopify_sync_empfart_complementary.py --dry-run --limit 50
  python3 scripts/shopify_sync_empfart_complementary.py --force-refresh-cache --force-write
  python3 scripts/shopify_sync_empfart_complementary.py --force-refresh-cache
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import config  # noqa: E402
from modules.cross_brand_ymm import resolve_cross_brand_xml_paths  # noqa: E402
from modules.empfart_complementary import (  # noqa: E402
    COMPLEMENTARY_KEY,
    COMPLEMENTARY_NAMESPACE,
    COMPLEMENTARY_TYPE,
    DEFAULT_MAX_PRODUCTS,
    ET_DEFINITION_NAME,
    ET_KEY,
    ET_NAMESPACE,
    ET_TYPE,
    build_relation_maps,
    build_sync_targets,
    metafield_list_value,
)
from modules.shopify_client import get_shopify_sku_to_product_id  # noqa: E402

_GQL_SET = """
mutation EmpfComplementarySet($metafields: [MetafieldsSetInput!]!) {
  metafieldsSet(metafields: $metafields) {
    metafields { namespace key value }
    userErrors { field message code }
  }
}
"""

_GQL_READ = """
query EmpfComplementaryRead($id: ID!) {
  product(id: $id) {
    id
    complementary: metafield(
      namespace: "shopify--discovery--product_recommendation"
      key: "complementary_products"
    ) { value }
    etArticles: metafield(namespace: "global", key: "et_articles") { value }
  }
}
"""

_GQL_DEF = """
query EtArticlesDef {
  metafieldDefinitions(
    first: 1
    ownerType: PRODUCT
    namespace: "global"
    key: "et_articles"
  ) {
    nodes { id name access { storefront } }
  }
}
"""

_GQL_DEF_CREATE = """
mutation EtArticlesDefCreate($definition: MetafieldDefinitionInput!) {
  metafieldDefinitionCreate(definition: $definition) {
    createdDefinition { id }
    userErrors { field message code }
  }
}
"""


def _session() -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    return s


def gql(sess: requests.Session, query: str, variables: dict | None = None) -> dict:
    url = (
        f"https://{config.SHOPIFY_SHOP_DOMAIN}/admin/api/"
        f"{config.SHOPIFY_ADMIN_API_VERSION}/graphql.json"
    )
    last: dict = {}
    for attempt in range(20):
        try:
            r = sess.post(
                url,
                headers={
                    "X-Shopify-Access-Token": config.SHOPIFY_ACCESS_TOKEN,
                    "Content-Type": "application/json",
                },
                json={"query": query, "variables": variables or {}},
                timeout=(15, 180),
                proxies={"http": None, "https": None},
            )
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as exc:
            wait = min(2.0 * (attempt + 1), 30.0)
            print(
                f"  Netwerk/timeout, opnieuw over {wait:.0f}s ({exc.__class__.__name__})",
                flush=True,
            )
            time.sleep(wait)
            continue
        if r.status_code == 429:
            time.sleep(2 + attempt)
            continue
        r.raise_for_status()
        last = r.json()
        errs = last.get("errors") or []
        if any((e.get("extensions") or {}).get("code") == "THROTTLED" for e in errs):
            time.sleep(min(2.0 * (attempt + 1), 30.0))
            continue
        return last
    return last


def ensure_et_definition(sess: requests.Session) -> None:
    body = gql(sess, _GQL_DEF)
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    nodes = (((body.get("data") or {}).get("metafieldDefinitions")) or {}).get("nodes") or []
    if nodes:
        print(f"Metafield-definitie global.et_articles bestaat: {nodes[0].get('id')}", flush=True)
        return
    created = gql(
        sess,
        _GQL_DEF_CREATE,
        {
            "definition": {
                "name": ET_DEFINITION_NAME,
                "namespace": ET_NAMESPACE,
                "key": ET_KEY,
                "description": (
                    "Onderdelen/componenten uit CBEXPDN ET2ARTIKEL "
                    "(bijbehorende reservedelen)."
                ),
                "type": ET_TYPE,
                "ownerType": "PRODUCT",
                "access": {"storefront": "PUBLIC_READ"},
            }
        },
    )
    if created.get("errors"):
        raise RuntimeError(created["errors"])
    result = ((created.get("data") or {}).get("metafieldDefinitionCreate")) or {}
    errors = result.get("userErrors") or []
    if errors:
        raise RuntimeError(errors)
    print(
        f"Metafield-definitie aangemaakt: "
        f"{(result.get('createdDefinition') or {}).get('id')}",
        flush=True,
    )


def _normalize_list_value(raw: str | None) -> str:
    if not raw:
        return "[]"
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            return json.dumps(parsed, separators=(",", ":"))
    except json.JSONDecodeError:
        pass
    return raw.strip()


def read_current(sess: requests.Session, owner_gid: str) -> tuple[str, str]:
    body = gql(sess, _GQL_READ, {"id": owner_gid})
    if body.get("errors"):
        raise RuntimeError(body["errors"])
    product = ((body.get("data") or {}).get("product")) or {}
    comp = ((product.get("complementary") or {}).get("value")) or ""
    et = ((product.get("etArticles") or {}).get("value")) or ""
    return _normalize_list_value(comp), _normalize_list_value(et)


def _drop_invalid_gid_from_value(value: str, bad_gid: str) -> str | None:
    try:
        gids = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(gids, list):
        return None
    cleaned = [g for g in gids if g != bad_gid]
    if len(cleaned) == len(gids):
        return None
    if not cleaned:
        return "[]"
    return metafield_list_value([str(g) for g in cleaned])


def metafields_set(sess: requests.Session, metafields: list[dict]) -> tuple[int, int]:
    """
    Write metafields one-by-one. On INVALID_VALUE for a missing product GID,
    drop that GID and retry (SKU-cache can be stale).
    Stale ownerId (deleted product) is skipped.
    Returns (ok_count, skipped_empty_after_filter).
    """
    ok = 0
    skipped_empty = 0
    skipped_owner = 0
    total = len(metafields)
    for idx, mf in enumerate(metafields, start=1):
        current = dict(mf)
        for _attempt in range(12):
            body = gql(sess, _GQL_SET, {"metafields": [current]})
            if body.get("errors"):
                raise RuntimeError(body["errors"])
            payload = ((body.get("data") or {}).get("metafieldsSet")) or {}
            errs = payload.get("userErrors") or []
            if not errs:
                ok += 1
                break
            msg = (errs[0].get("message") or "") if errs else ""
            code = (errs[0].get("code") or "") if errs else ""
            if "Owner does not exist" in msg or (
                code == "INVALID_VALUE" and "ownerId" in str(errs[0].get("field") or [])
            ):
                print(
                    f"  Skip ontbrekende owner {current.get('ownerId')}",
                    flush=True,
                )
                skipped_owner += 1
                break
            # e.g. Value references non-existent resource gid://shopify/Product/123.
            bad = ""
            if "non-existent resource" in msg and "gid://" in msg:
                start = msg.find("gid://")
                end = msg.find(".", start)
                bad = msg[start:end] if end > start else msg[start:].split()[0]
            if not bad:
                raise RuntimeError(errs)
            print(f"  Skip ontbrekende productref {bad}", flush=True)
            new_val = _drop_invalid_gid_from_value(current.get("value") or "", bad)
            if new_val is None:
                raise RuntimeError(errs)
            if new_val == "[]":
                print(
                    f"  Geen geldige refs meer voor "
                    f"{current.get('namespace')}.{current.get('key')} op {current.get('ownerId')}",
                    flush=True,
                )
                skipped_empty += 1
                break
            current["value"] = new_val
        else:
            raise RuntimeError(f"Te veel ongeldige productrefs voor {mf.get('ownerId')}")
        if idx % 100 == 0 or idx == total:
            print(
                f"  Progress writes: {idx}/{total} "
                f"(ok={ok}, skip_owner={skipped_owner}, skip_empty={skipped_empty})",
                flush=True,
            )
        time.sleep(0.12)
    return ok, skipped_empty + skipped_owner


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Sync EMPFART2ART/MUSSARTIKEL/ET2ARTIKEL naar Shopify metafields."
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Alleen tonen wat gezet zou worden; geen writes.",
    )
    p.add_argument(
        "--sku",
        action="append",
        default=[],
        help="Alleen deze owner-SKU(s); herhaalbaar. Case-insensitive.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Max aantal owner-producten om te verwerken (0 = alle).",
    )
    p.add_argument(
        "--max-products",
        type=int,
        default=DEFAULT_MAX_PRODUCTS,
        help=f"Max productrefs per metafield (default {DEFAULT_MAX_PRODUCTS}).",
    )
    p.add_argument(
        "--force-refresh-cache",
        action="store_true",
        help="Shopify SKU→product-id cache verversen.",
    )
    p.add_argument(
        "--force-write",
        action="store_true",
        help="Schrijf altijd (skip vergelijking met huidige metafields; sneller bij full sync).",
    )
    p.add_argument(
        "--skip-et",
        action="store_true",
        help="global.et_articles niet schrijven.",
    )
    p.add_argument(
        "--skip-complementary",
        action="store_true",
        help="complementary_products niet schrijven.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if not config.SHOPIFY_SHOP_DOMAIN or not config.SHOPIFY_ACCESS_TOKEN:
        print("SHOPIFY_SHOP_DOMAIN / SHOPIFY_ACCESS_TOKEN ontbreken in .env", file=sys.stderr)
        return 1

    xml_paths = resolve_cross_brand_xml_paths()
    if not xml_paths:
        print("Geen CBEXPDN XML gevonden (input/ / hsq / wp).", file=sys.stderr)
        return 1
    print("XML:", ", ".join(Path(p).name for p in xml_paths), flush=True)

    print("Relaties uit XML streamen…", flush=True)
    relations = build_relation_maps(xml_paths)
    n_muss = sum(1 for r in relations.values() if r.muss)
    n_empf = sum(1 for r in relations.values() if r.empf)
    n_et = sum(1 for r in relations.values() if r.et)
    print(
        f"Owners met relaties: {len(relations)} "
        f"(MUSS={n_muss}, EMPF={n_empf}, ET={n_et})",
        flush=True,
    )

    only = {s.strip() for s in args.sku if s.strip()} or None
    sku_map = get_shopify_sku_to_product_id(force_refresh=args.force_refresh_cache)
    print(f"SKU→product cache: {len(sku_map)} entries", flush=True)

    targets = build_sync_targets(
        relations,
        sku_map,
        max_products=args.max_products,
        only_skus=only,
    )
    owners = sorted(targets.keys())
    if args.limit and args.limit > 0:
        owners = owners[: args.limit]

    print(f"Owners in Shopify met sync-target: {len(owners)}", flush=True)
    if only and not owners:
        print("Geen van de opgegeven SKU’s gevonden in XML+Shopify.", file=sys.stderr)
        return 1

    sess = _session()
    if not args.dry_run and not args.skip_et:
        ensure_et_definition(sess)

    to_set: list[dict] = []
    skipped_same = 0
    skipped_empty = 0
    would_comp = 0
    would_et = 0

    for owner in owners:
        t = targets[owner]
        owner_gid = t["owner_gid"]
        comp_gids = t["complementary_gids"]
        et_gids = t["et_gids"]

        if args.dry_run or only:
            print(f"\n{owner} → product {t['owner_product_id']}", flush=True)
            if t["has_complementary_source"]:
                print(
                    f"  complementary SKUs ({len(t['complementary_skus_raw'])}): "
                    f"{t['complementary_skus_raw'][:12]}"
                    f"{'…' if len(t['complementary_skus_raw']) > 12 else ''}",
                    flush=True,
                )
                print(f"  complementary GIDs ({len(comp_gids)}): {comp_gids}", flush=True)
            if t["has_et_source"]:
                print(
                    f"  ET SKUs ({len(t['et_skus_raw'])}): "
                    f"{t['et_skus_raw'][:12]}"
                    f"{'…' if len(t['et_skus_raw']) > 12 else ''}",
                    flush=True,
                )
                print(f"  ET GIDs ({len(et_gids)}): {et_gids}", flush=True)

        cur_comp, cur_et = ("[]", "[]")
        if not args.dry_run and not args.force_write:
            try:
                cur_comp, cur_et = read_current(sess, owner_gid)
            except Exception as exc:
                print(f"  Waarschuwing: huidige metafields lezen mislukt ({exc})", flush=True)

        if not args.skip_complementary and t["has_complementary_source"]:
            if not comp_gids:
                skipped_empty += 1
            else:
                new_val = metafield_list_value(comp_gids)
                if not args.dry_run and not args.force_write and new_val == cur_comp:
                    skipped_same += 1
                else:
                    would_comp += 1
                    to_set.append(
                        {
                            "ownerId": owner_gid,
                            "namespace": COMPLEMENTARY_NAMESPACE,
                            "key": COMPLEMENTARY_KEY,
                            "type": COMPLEMENTARY_TYPE,
                            "value": new_val,
                        }
                    )

        if not args.skip_et and t["has_et_source"]:
            if not et_gids:
                skipped_empty += 1
            else:
                new_val = metafield_list_value(et_gids)
                if not args.dry_run and not args.force_write and new_val == cur_et:
                    skipped_same += 1
                else:
                    would_et += 1
                    to_set.append(
                        {
                            "ownerId": owner_gid,
                            "namespace": ET_NAMESPACE,
                            "key": ET_KEY,
                            "type": ET_TYPE,
                            "value": new_val,
                        }
                    )

    print(
        f"\nTe schrijven: complementary={would_comp}, et={would_et}, "
        f"metafield-ops={len(to_set)}; "
        f"ongewijzigd={skipped_same}, leeg-na-mapping={skipped_empty}",
        flush=True,
    )

    if args.dry_run:
        print("Dry-run: geen writes.", flush=True)
        return 0

    if not to_set:
        print("Niets te schrijven.", flush=True)
        return 0

    print(f"metafieldsSet ({len(to_set)} ops)…", flush=True)
    ok, skipped_after = metafields_set(sess, to_set)
    print(f"Klaar: geschreven={ok}, leeg-na-stale-filter={skipped_after}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
