#!/usr/bin/env python3
"""
Batch: normaliseer alle Kutvek Graphics & Stickers (SKU 25.*) op ktm-shop.nl.

Per product (geen merge — één product per design/model blijft):
  - Titel: merk + model + segment + design (Kutvek-vorm)
  - Opties: Bouwjaar (losse jaren) + Uitvoering
  - YMM metafields (fits_on / ymm_summary / …)
  - Media: galerijvolgorde + Complete/Light-koppeling
  - Garagecatalogus: ontbrekende jaren (optioneel bij --apply)

Voorbeelden:
  python3 scripts/ktm_graphics_normalize_all.py
  python3 scripts/ktm_graphics_normalize_all.py --apply --limit 25
  python3 scripts/ktm_graphics_normalize_all.py --apply --offset 25 --limit 50
  python3 scripts/ktm_graphics_normalize_all.py --apply --handle 45-150-036-kutvek
  python3 scripts/ktm_graphics_normalize_all.py --only-todo   # skip al gemigreerd
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import os
import re
import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(PROJECT_ROOT)
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

_spec = importlib.util.spec_from_file_location(
    "ktm_graphics_pilot",
    PROJECT_ROOT / "scripts" / "ktm_graphics_pilot.py",
)
_pilot = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
_spec.loader.exec_module(_pilot)

OUT_DIR = PROJECT_ROOT / "output" / "graphics"
SHOPIFY_Q = "product_type:'Graphics & Stickers' status:active sku:25.*"

YEAR_BAND = re.compile(r"\d{4}\s*[-–]\s*\d{4}")


def already_migrated(node: dict) -> bool:
    """True als Bouwjaar bestaat met alleen losse jaren (geen Year-optie)."""
    opts = {o.get("name"): o for o in (node.get("options") or [])}
    if "Year" in opts:
        return False
    bj = opts.get("Bouwjaar")
    if not bj:
        return False
    vals = bj.get("values") or []
    if not vals:
        return False
    return all(re.fullmatch(r"\d{4}", str(v)) for v in vals)


def fetch_all_products(sess, url, headers, query: str) -> list[dict]:
    gql_query = """
    query($q: String!, $cursor: String) {
      products(first: 25, query: $q, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          handle
          title
          status
          vendor
          productType
          options { name values }
          variants(first: 100) {
            nodes {
              id
              sku
              title
              price
              selectedOptions { name value }
            }
          }
          ymm: metafield(namespace: "global", key: "ymm_summary") { value }
          fits: metafield(namespace: "global", key: "fits_on") { value }
        }
      }
    }
    """
    nodes: list[dict] = []
    cursor = None
    while True:
        data = _pilot.gql(
            sess, url, headers, gql_query, {"q": query, "cursor": cursor}
        )
        page = data.get("products") or {}
        batch = page.get("nodes") or []
        nodes.extend(batch)
        pi = page.get("pageInfo") or {}
        if not pi.get("hasNextPage"):
            break
        cursor = pi.get("endCursor")
        print(f"  … {len(nodes)} producten geladen", flush=True)
        time.sleep(0.2)
    return nodes


def plan_row(plan: dict, error: str = "") -> dict:
    return {
        "ok": "1" if plan.get("ok") and not error else "0",
        "handle": plan.get("handle") or "",
        "old_title": plan.get("old_title") or "",
        "new_title": plan.get("new_title") or "",
        "make": plan.get("make") or "",
        "model": plan.get("model") or "",
        "years": " ".join(plan.get("years") or []),
        "variant_count": plan.get("variant_count") or 0,
        "source_sku_count": plan.get("source_sku_count") or 0,
        "ymm_summary": plan.get("ymm_summary") or "",
        "missing_skus": " | ".join(plan.get("missing_skus") or []),
        "error": error,
    }


def apply_one(sess, url, headers, plan: dict, skip_catalog: bool) -> dict:
    result = {
        "handle": plan["handle"],
        "ok": False,
        "steps": [],
        "error": "",
    }
    try:
        if not skip_catalog:
            cat = _pilot.ensure_catalog_years(
                plan["make"], plan["model"], plan["years"]
            )
            result["steps"].append(f"catalog+{cat.get('added') or []}")
        _pilot.apply_plan(sess, url, headers, plan, quiet=True)
        result["ok"] = True
        result["steps"].append("productSet+ymm+media")
    except Exception as exc:  # noqa: BLE001 — batch moet door/stop met log
        result["error"] = str(exc)[:800]
    return result


def main() -> int:
    _pilot.load_dotenv()
    ap = argparse.ArgumentParser(description="Batch Kutvek graphics normaliseren")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--limit", type=int, default=0, help="Max producten om te schrijven")
    ap.add_argument("--offset", type=int, default=0, help="Sla eerste N todo-producten over")
    ap.add_argument("--handle", action="append", default=[], help="Alleen deze handle(s)")
    ap.add_argument(
        "--only-todo",
        action="store_true",
        default=True,
        help="Skip al gemigreerde (default aan)",
    )
    ap.add_argument(
        "--include-done",
        action="store_true",
        help="Neem ook al-gemigreerde mee (her-apply)",
    )
    ap.add_argument("--skip-catalog", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.8, help="Pauze tussen writes (s)")
    ap.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Ga door na failure i.p.v. stoppen",
    )
    ap.add_argument(
        "--query",
        default=SHOPIFY_Q,
        help="Shopify product search query",
    )
    args = ap.parse_args()
    only_todo = not args.include_done

    sess, url, headers = _pilot.session_url()
    print(f"Query: {args.query}", flush=True)
    print("Producten laden…", flush=True)
    nodes = fetch_all_products(sess, url, headers, args.query)
    print(f"Totaal: {len(nodes)}", flush=True)

    if args.handle:
        want = set(args.handle)
        nodes = [n for n in nodes if (n.get("handle") or "") in want]
        print(f"Filter handles: {len(nodes)}", flush=True)

    done = [n for n in nodes if already_migrated(n)]
    todo_nodes = [n for n in nodes if not already_migrated(n)] if only_todo else list(nodes)
    print(f"Al gemigreerd: {len(done)} | te doen: {len(todo_nodes)}", flush=True)

    plans: list[dict] = []
    fail_rows: list[dict] = []
    for n in todo_nodes:
        try:
            plan = _pilot.build_plan(n)
            plans.append(plan)
            if not plan.get("ok"):
                fail_rows.append(plan_row(plan, "plan verification failed"))
        except SystemExit as exc:
            fail_rows.append(
                plan_row(
                    {
                        "handle": n.get("handle"),
                        "old_title": n.get("title"),
                        "ok": False,
                    },
                    str(exc),
                )
            )
        except Exception as exc:  # noqa: BLE001
            fail_rows.append(
                plan_row(
                    {
                        "handle": n.get("handle"),
                        "old_title": n.get("title"),
                        "ok": False,
                    },
                    str(exc)[:500],
                )
            )

    ok_plans = [p for p in plans if p.get("ok")]
    bad_plans = [p for p in plans if not p.get("ok")]
    print(f"Plannen OK: {len(ok_plans)}  FAIL: {len(bad_plans) + len(fail_rows)}", flush=True)
    if fail_rows[:5] or bad_plans[:5]:
        print("Eerste failures:", flush=True)
        for r in (fail_rows + [plan_row(p, "plan ok=false") for p in bad_plans])[:8]:
            print(f"  {r.get('handle')}: {r.get('error') or r.get('old_title')}", flush=True)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%Y%m%d_%H%M%S")
    plan_path = OUT_DIR / f"normalize_all_plan_{stamp}.json"
    slim = [
        {k: v for k, v in p.items() if k not in {"variants", "tuples", "fits_on_json"}}
        for p in plans
    ]
    plan_path.write_text(json.dumps(slim, indent=2, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "normalize_all_plan_latest.json").write_bytes(plan_path.read_bytes())

    overview = OUT_DIR / f"normalize_all_overview_{stamp}.csv"
    fields = [
        "ok",
        "handle",
        "old_title",
        "new_title",
        "make",
        "model",
        "years",
        "variant_count",
        "source_sku_count",
        "ymm_summary",
        "missing_skus",
        "error",
    ]
    with overview.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter=";")
        w.writeheader()
        for p in ok_plans:
            w.writerow(plan_row(p))
        for p in bad_plans:
            w.writerow(plan_row(p, "plan verification failed"))
        for r in fail_rows:
            w.writerow(r)
    (OUT_DIR / "normalize_all_overview_latest.csv").write_bytes(overview.read_bytes())
    print(f"Plan: {plan_path}", flush=True)
    print(f"Overview: {overview}", flush=True)

    if not args.apply:
        print(
            f"Dry-run. Zou {len(ok_plans)} producten schrijven "
            f"({len(fail_rows) + len(bad_plans)} failures overgeslagen).",
            flush=True,
        )
        return 0 if not (fail_rows or bad_plans) else 0  # dry-run altijd 0 als plan-file er is

    todo = ok_plans[args.offset :]
    if args.limit:
        todo = todo[: args.limit]
    print(
        f"Apply {len(todo)} producten (offset={args.offset}, limit={args.limit or '∞'})…",
        flush=True,
    )

    results: list[dict] = []
    for i, plan in enumerate(todo, 1):
        print(
            f"[{i}/{len(todo)}] {plan['handle']} "
            f"{plan['old_title']!r} → {plan['new_title']!r} "
            f"vars={plan['variant_count']}…",
            flush=True,
        )
        # Minder ruis van nested prints: apply_plan print zelf — ok voor log
        res = apply_one(sess, url, headers, plan, skip_catalog=args.skip_catalog)
        results.append(res)
        status = "OK" if res["ok"] else "FAIL"
        print(f"  {status} {res.get('steps')} {res.get('error')}", flush=True)
        if not res["ok"] and not args.continue_on_error:
            print("STOP bij failure — rest niet aangepast. Hervat met --offset.", flush=True)
            break
        time.sleep(args.sleep)

    res_path = OUT_DIR / f"normalize_all_results_{stamp}.json"
    res_path.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding="utf-8")
    (OUT_DIR / "normalize_all_results_latest.json").write_bytes(res_path.read_bytes())
    print(f"Results: {res_path}", flush=True)
    failed = [r for r in results if not r.get("ok")]
    ok_n = len(results) - len(failed)
    print(f"Klaar: {ok_n} OK, {len(failed)} FAIL", flush=True)
    if not args.skip_catalog and ok_n:
        print(
            "Catalogus lokaal bijgewerkt — deploy garage indien jaren toegevoegd:\n"
            "  cd ktm/garage && shopify app deploy --allow-updates "
            '--message "graphics YMM years"',
            flush=True,
        )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
