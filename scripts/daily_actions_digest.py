#!/usr/bin/env python3
"""Dagelijkse Actions-digest: alleen gelukte updates, mail via Resend.

Venster standaard: gisteren 10:00 → vandaag 10:00 (Europe/Amsterdam).
Bronnen: Supabase workflow_dispatch_log (prijs/ETA-stats) + GitHub Actions-runs/logs.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import os
import re
import sys
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

try:
    import requests
except ImportError:
    print("Installeer requests: pip install requests", file=sys.stderr)
    raise SystemExit(2)

NL = ZoneInfo("Europe/Amsterdam")
UTC = timezone.utc

REPO_DEFAULT = "donolsthoorn-dev/ktm-converter"
DIGEST_WORKFLOW = "daily_actions_digest.yml"

# Workflows die we meenemen (logs parsen). Prijs/ETA komt uit Supabase.
TRACKED_WORKFLOWS = {
    "motox_ebihr_sync.yml",
    "motox_ebihr_seo.yml",
    "motox_putoline.yml",
    "motox_deactivate_products_without_image.yml",
    "motox_shopify_gtin_fill.yml",
    "motox_ymm_derived_fill.yml",
    "ktm_seo.yml",
    "ktm_new_products_weekly.yml",
    "ktm_ymm_delivery.yml",
    "ktm_garage_fitment_deploy.yml",
    "ktm_job_worker.yml",
    "ktm_customs_missing_fill.yml",
    "ktm_shopify_gtin_fill.yml",
    "ktm_shopify_auto_deactivate_invalid_products.yml",
    "ktm_shopify_publish_sellable_active_products.yml",
    "shops_category_fill_empty.yml",
    "shops_synkro_clone_missing.yml",
}

GROUP_KTM = "ktm"
GROUP_MOTOX = "motox"
GROUP_PUTOLINE = "putoline"

GROUP_TITLES = {
    GROUP_KTM: "KTM aanpassingen aan producten",
    GROUP_MOTOX: "Motox aanpassingen aan producten",
    GROUP_PUTOLINE: "Putoline",
}

GROUP_ORDER = (GROUP_KTM, GROUP_MOTOX, GROUP_PUTOLINE)

NL_MONTHS = {
    1: "jan",
    2: "feb",
    3: "mrt",
    4: "apr",
    5: "mei",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "okt",
    11: "nov",
    12: "dec",
}
NL_WEEKDAYS = {
    0: "ma",
    1: "di",
    2: "wo",
    3: "do",
    4: "vr",
    5: "za",
    6: "zo",
}


@dataclass
class DigestItem:
    """Één opsommingsregel onder een groeps-kop."""

    group: str
    text: str
    sort_key: int = 100


def _nl_num(n: int | float) -> str:
    return f"{int(n):,}".replace(",", ".")


def _fmt_nl(dt: datetime) -> str:
    local = dt.astimezone(NL)
    return (
        f"{NL_WEEKDAYS[local.weekday()]} {local.day} "
        f"{NL_MONTHS[local.month]} {local.strftime('%H:%M')}"
    )


def _fmt_nl_short(dt: datetime) -> str:
    local = dt.astimezone(NL)
    return f"{local.day} {NL_MONTHS[local.month]}"


def digest_window(
    *,
    until: datetime | None = None,
    hours: float | None = None,
) -> tuple[datetime, datetime]:
    """Return (since, until) as timezone-aware datetimes in UTC-comparable form."""
    now_nl = (until or datetime.now(tz=NL)).astimezone(NL)
    if hours is not None:
        end = now_nl
        start = end - timedelta(hours=hours)
        return start, end
    # Align to 10:00 NL boundary: if we run at/after 10:00, window ends at today's 10:00.
    end = now_nl.replace(hour=10, minute=0, second=0, microsecond=0)
    if now_nl < end:
        end -= timedelta(days=1)
    start = end - timedelta(days=1)
    return start, end


def _gh_headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": "ktm-daily-actions-digest",
    }


def fetch_github_runs(
    *,
    repo: str,
    token: str,
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    since_s = since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    until_s = until.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    runs: list[dict[str, Any]] = []
    page = 1
    while page <= 20:
        url = f"https://api.github.com/repos/{repo}/actions/runs"
        resp = requests.get(
            url,
            headers=_gh_headers(token),
            params={
                "created": f"{since_s}..{until_s}",
                "per_page": 100,
                "page": page,
            },
            timeout=60,
        )
        if not resp.ok:
            raise RuntimeError(f"GitHub runs HTTP {resp.status_code}: {resp.text[:500]}")
        batch = (resp.json() or {}).get("workflow_runs") or []
        if not batch:
            break
        for r in batch:
            created = r.get("created_at") or ""
            if created < since_s or created > until_s:
                continue
            path = (r.get("path") or "").split("/")[-1]
            if path == DIGEST_WORKFLOW:
                continue
            runs.append(r)
        if len(batch) < 100:
            break
        page += 1
    return runs


def fetch_dispatch_logs(
    *,
    supabase_url: str,
    service_key: str,
    since: datetime,
    until: datetime,
) -> list[dict[str, Any]]:
    base = supabase_url.rstrip("/")
    since_s = since.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    until_s = until.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    url = f"{base}/rest/v1/workflow_dispatch_log"
    # Duplicate created_at keys: gte + lte
    resp = requests.get(
        url,
        headers={
            "apikey": service_key,
            "Authorization": f"Bearer {service_key}",
            "Accept": "application/json",
        },
        params=[
            ("select", "id,created_at,workflow_file,mode,run_state,run_summary,run_stats"),
            ("created_at", f"gte.{since_s}"),
            ("created_at", f"lte.{until_s}"),
            ("order", "created_at.desc"),
            ("limit", "2000"),
        ],
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Supabase HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json() or []


def download_run_log_text(*, repo: str, token: str, run_id: int) -> str:
    url = f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/logs"
    resp = requests.get(url, headers=_gh_headers(token), timeout=120)
    if not resp.ok:
        raise RuntimeError(f"GitHub logs HTTP {resp.status_code}: {resp.text[:300]}")
    parts: list[str] = []
    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        for name in zf.namelist():
            if not name.endswith(".txt"):
                continue
            if any(
                x in name
                for x in ("Set up job", "Checkout", "Complete job", "Post ")
            ):
                continue
            text = zf.read(name).decode("utf-8", errors="replace")
            for line in text.splitlines():
                s = re.sub(r"^\d{4}-\d{2}-\d{2}T.*?Z\s*", "", line).strip()
                if s:
                    parts.append(s)
    return "\n".join(parts)


def _sum_int(stats: dict[str, Any], *keys: str) -> int:
    total = 0
    for k in keys:
        v = stats.get(k)
        if isinstance(v, (int, float)):
            total += int(v)
    return total


def items_price_eta(rows: list[dict[str, Any]]) -> list[DigestItem]:
    agg: Counter[str] = Counter()
    for r in rows:
        if (r.get("workflow_file") or "") != "ktm_price_eta_status_sync.yml":
            continue
        if r.get("run_state") != "success":
            continue
        stats = r.get("run_stats") or {}
        if not isinstance(stats, dict):
            continue
        agg["price"] += _sum_int(stats, "price_success")
        agg["eta"] += _sum_int(stats, "eta_success")
        agg["gtin"] += _sum_int(stats, "gtin_success")
        agg["policy"] += _sum_int(stats, "policy_success")
        agg["customs"] += _sum_int(stats, "customs_success")
        agg["product_status"] += _sum_int(stats, "product_status_success")
    items: list[DigestItem] = []
    mapping = [
        ("price", "prijs", 10),
        ("eta", "ETA", 11),
        ("gtin", "barcodes", 12),
        ("policy", "voorraadbeleid", 13),
        ("customs", "HS/herkomst", 14),
        ("product_status", "productstatus", 15),
    ]
    for key, label, sort in mapping:
        if agg.get(key):
            items.append(
                DigestItem(GROUP_KTM, f"{label} {_nl_num(agg[key])}", sort)
            )
    return items


def _parse_metrics(wf: str, text: str) -> dict[str, int]:
    m: dict[str, int] = {}
    if wf == "shops_synkro_clone_missing.yml":
        g = re.search(r"getagd=(\d+)", text)
        if g:
            m["getagd"] = int(g.group(1))
        return m
    if wf in ("ktm_seo.yml", "motox_ebihr_seo.yml"):
        g = re.search(r"Producten:\s*\d+\s+geschreven:\s*(\d+)", text)
        if g:
            m["geschreven"] = int(g.group(1))
        return m
    if wf == "motox_ebihr_sync.yml":
        for key, pat in (
            ("created", r'"created":\s*(\d+)'),
            ("price", r'"price_variant_updates":\s*(\d+)'),
            ("stock_policy", r'"stock_policy_variant_updates":\s*(\d+)'),
            ("images", r'"images_backfilled":\s*(\d+)'),
            ("fitment", r'"fitment_written":\s*(\d+)'),
            ("customs", r'"customs_written":\s*(\d+)'),
            ("articles", r'"articles_written":\s*(\d+)'),
            ("drafted", r'"drafted":\s*(\d+)'),
        ):
            g = re.search(pat, text)
            if g:
                m[key] = int(g.group(1))
        return m
    if wf == "ktm_shopify_gtin_fill.yml":
        g = re.search(r"Klaar\.\s*ok=(\d+)", text)
        if g:
            m["ok"] = int(g.group(1))
        return m
    if wf == "motox_shopify_gtin_fill.yml":
        g = re.search(r"Klaar\.\s*ok=(\d+)", text)
        if g:
            m["ok"] = int(g.group(1))
        return m
    if wf == "ktm_shopify_auto_deactivate_invalid_products.yml":
        g = re.search(r"Klaar:\s*(\d+)/\d+", text)
        if g:
            m["ok"] = int(g.group(1))
        return m
    if wf == "ktm_shopify_publish_sellable_active_products.yml":
        g = re.search(r"Klaar:\s*gepubliceerd\s*(\d+)", text)
        if g:
            m["published"] = int(g.group(1))
        return m
    if wf == "motox_deactivate_products_without_image.yml":
        g = re.search(r"Klaar:\s*(\d+)/\d+\s*op DRAFT", text)
        if g:
            m["draft"] = int(g.group(1))
        return m
    if wf == "motox_ymm_derived_fill.yml":
        g = re.search(r"(?:Klaar|APPLY|DRY-RUN)\.\s*ok=(\d+)", text)
        if g:
            m["ok"] = int(g.group(1))
        return m
    if wf == "shops_category_fill_empty.yml":
        # May run ktm + motox in one workflow; sum all Done ok=
        for g in re.finditer(r"Done:\s*\d+\s+ok=(\d+)", text):
            m["ok"] = m.get("ok", 0) + int(g.group(1))
        return m
    if wf == "ktm_new_products_weekly.yml":
        g = re.search(r"Klaar:\s*geschreven=(\d+)", text)
        if g:
            m["geschreven"] = int(g.group(1))
        return m
    if wf == "ktm_ymm_delivery.yml":
        g = re.search(r"YMM push klaar.*?\bok=(\d+)", text)
        if g:
            m["ok"] = int(g.group(1))
        return m
    if wf == "motox_putoline.yml":
        g = re.search(
            r"(?:updated|bijgewerkt|nieuw|created|prijs|EAN).*?(\d+)",
            text,
            re.I,
        )
        # Prefer explicit summary if present
        for pat, key in (
            (r"prijzen?[=\s:]+(\d+)", "price"),
            (r"nieuw(?:e)?[=\s:]+(\d+)", "new"),
            (r"EAN[=\s:]+(\d+)", "ean"),
            (r"updated[=\s:]+(\d+)", "updated"),
        ):
            gg = re.search(pat, text, re.I)
            if gg:
                m[key] = int(gg.group(1))
        if not m and g:
            pass  # avoid noisy false positives
        return m
    if wf == "ktm_job_worker.yml":
        g = re.search(
            r"Spiegel klaar:\s*(\d+)\s*producten,\s*(\d+)\s*varianten", text
        )
        if g:
            m["products"] = int(g.group(1))
            m["variants"] = int(g.group(2))
        return m
    if wf == "ktm_customs_missing_fill.yml":
        g = re.search(r"Klaar\.\s*(?:ok=(\d+)|fouten=(\d+))", text)
        # Also look for written counts
        for pat, key in (
            (r"geschreven[=:\s]+(\d+)", "written"),
            (r"updated[=:\s]+(\d+)", "updated"),
            (r"HS[^0-9]*(\d+)", "hs"),
        ):
            gg = re.search(pat, text, re.I)
            if gg and key == "written":
                m["written"] = int(gg.group(1))
        return m
    return m


def _metrics_to_items(wf: str, totals: dict[str, int]) -> list[DigestItem]:
    if not any(v > 0 for v in totals.values()):
        return []
    out: list[DigestItem] = []
    if wf == "shops_synkro_clone_missing.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"Synkro POS-kopieën {_nl_num(totals['getagd'])} getagd",
                50,
            )
        )
    elif wf == "ktm_seo.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"SEO {_nl_num(totals['geschreven'])} producten",
                30,
            )
        )
    elif wf == "motox_ebihr_seo.yml":
        out.append(
            DigestItem(
                GROUP_MOTOX,
                f"SEO {_nl_num(totals['geschreven'])} producten",
                30,
            )
        )
    elif wf == "motox_ebihr_sync.yml":
        mapping = [
            ("created", "nieuw", 20),
            ("price", "prijzen", 21),
            ("stock_policy", "voorraadbeleid", 22),
            ("fitment", "fitment", 23),
            ("customs", "HS/herkomst", 24),
            ("articles", "artikelen", 25),
            ("images", "foto’s", 26),
            ("drafted", "op draft", 27),
        ]
        for k, label, sort in mapping:
            if totals.get(k):
                out.append(
                    DigestItem(GROUP_MOTOX, f"{label} {_nl_num(totals[k])}", sort)
                )
    elif wf == "ktm_shopify_gtin_fill.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"barcodes vullen {_nl_num(totals['ok'])}",
                70,
            )
        )
    elif wf == "motox_shopify_gtin_fill.yml":
        out.append(
            DigestItem(
                GROUP_MOTOX,
                f"barcodes vullen {_nl_num(totals['ok'])}",
                70,
            )
        )
    elif wf == "motox_ymm_derived_fill.yml":
        out.append(
            DigestItem(
                GROUP_MOTOX,
                f"YMM-velden {_nl_num(totals['ok'])}",
                92,
            )
        )
    elif wf == "ktm_shopify_auto_deactivate_invalid_products.yml":
        # Merge visually with price/ETA productstatus: same label family
        out.append(
            DigestItem(
                GROUP_KTM,
                f"productstatus {_nl_num(totals['ok'])} aangepast",
                60,
            )
        )
    elif wf == "ktm_shopify_publish_sellable_active_products.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"leverbare producten publiceren {_nl_num(totals['published'])}",
                61,
            )
        )
    elif wf == "motox_deactivate_products_without_image.yml":
        out.append(
            DigestItem(
                GROUP_MOTOX,
                f"zonder foto op draft {_nl_num(totals['draft'])}",
                91,
            )
        )
    elif wf == "shops_category_fill_empty.yml":
        # Beide shops; toon onder KTM (één regel)
        out.append(
            DigestItem(
                GROUP_KTM,
                f"category vullen {_nl_num(totals['ok'])}",
                80,
            )
        )
    elif wf == "ktm_new_products_weekly.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"nieuwe producten {_nl_num(totals['geschreven'])}",
                40,
            )
        )
    elif wf == "ktm_ymm_delivery.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"YMM-levering {_nl_num(totals['ok'])}",
                93,
            )
        )
    elif wf == "motox_putoline.yml":
        mapping = [
            ("price", "prijzen", 10),
            ("ean", "EAN", 11),
            ("new", "nieuw", 12),
            ("updated", "bijgewerkt", 13),
        ]
        for k, label, sort in mapping:
            if totals.get(k):
                out.append(
                    DigestItem(GROUP_PUTOLINE, f"{label} {_nl_num(totals[k])}", sort)
                )
    elif wf == "ktm_job_worker.yml":
        out.append(
            DigestItem(
                GROUP_KTM,
                f"catalogus-spiegel ~{_nl_num(totals['products'])} producten",
                95,
            )
        )
    elif wf == "ktm_customs_missing_fill.yml":
        if totals.get("written"):
            out.append(
                DigestItem(
                    GROUP_KTM,
                    f"HS/herkomst aanvullen {_nl_num(totals['written'])}",
                    72,
                )
            )
    return out


def aggregate_from_logs(
    *,
    repo: str,
    token: str,
    runs: list[dict[str, Any]],
) -> list[DigestItem]:
    by_wf: dict[str, list[int]] = defaultdict(list)
    for r in runs:
        if (r.get("conclusion") or "") != "success":
            continue
        path = (r.get("path") or "").split("/")[-1]
        if path not in TRACKED_WORKFLOWS:
            continue
        by_wf[path].append(int(r["id"]))

    collected: list[DigestItem] = []

    def work(item: tuple[str, list[int]]) -> list[DigestItem]:
        wf, ids = item
        totals: Counter[str] = Counter()
        for rid in ids:
            try:
                text = download_run_log_text(repo=repo, token=token, run_id=rid)
            except Exception as e:
                print(f"warn: logs {wf} run {rid}: {e}", file=sys.stderr)
                continue
            for k, v in _parse_metrics(wf, text).items():
                totals[k] += v
        return _metrics_to_items(wf, dict(totals))

    items = list(by_wf.items())
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for batch in pool.map(work, items):
            collected.extend(batch)
    return collected


def _escape_html(s: str) -> str:
    return (
        s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def build_digest(
    *,
    since: datetime,
    until: datetime,
    items: list[DigestItem],
) -> tuple[str, str, str]:
    """Return (subject, text_body, html_body)."""
    subject = f"Overzicht updates webshops — {_fmt_nl_short(until)}"
    period = f"Periode: {_fmt_nl(since)} → {_fmt_nl(until)}"

    by_group: dict[str, list[DigestItem]] = defaultdict(list)
    for it in items:
        by_group[it.group].append(it)

    text_parts = [period, ""]
    html_parts = [
        f"<p>{_escape_html(period)}</p>",
    ]

    any_content = False
    for group in GROUP_ORDER:
        group_items = sorted(by_group.get(group, []), key=lambda x: x.sort_key)
        if not group_items:
            continue
        any_content = True
        title = GROUP_TITLES[group]
        text_parts.append(title)
        html_parts.append(f"<p><strong>{_escape_html(title)}</strong></p>")
        html_parts.append("<ul>")
        for it in group_items:
            text_parts.append(f"• {it.text}")
            html_parts.append(f"<li>{_escape_html(it.text)}</li>")
        html_parts.append("</ul>")
        text_parts.append("")

    if not any_content:
        empty = "Geen bijgewerkte resultaten in dit venster."
        text_parts.append(empty)
        html_parts.append(f"<p>{_escape_html(empty)}</p>")

    while text_parts and text_parts[-1] == "":
        text_parts.pop()

    text_body = "\n".join(text_parts)
    html_body = "\n".join(html_parts)
    return subject, text_body, html_body


def send_resend(
    *,
    api_key: str,
    from_addr: str,
    to_addrs: list[str],
    subject: str,
    text: str,
    html: str,
) -> dict[str, Any]:
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "from": from_addr,
            "to": to_addrs,
            "subject": subject,
            "text": text,
            "html": html,
        },
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(f"Resend HTTP {resp.status_code}: {resp.text[:500]}")
    return resp.json() or {}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--send",
        action="store_true",
        help="Mail via Resend versturen (anders alleen printen)",
    )
    ap.add_argument(
        "--hours",
        type=float,
        default=None,
        help="Optioneel: venster van N uur terug i.p.v. 10:00→10:00",
    )
    ap.add_argument(
        "--repo",
        default=os.environ.get("GITHUB_REPOSITORY") or REPO_DEFAULT,
    )
    args = ap.parse_args()

    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if not token:
        print("GITHUB_TOKEN ontbreekt", file=sys.stderr)
        return 2

    supabase_url = (os.environ.get("SUPABASE_URL") or "").strip()
    supabase_key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or "").strip()

    since, until = digest_window(hours=args.hours)
    print(f"Venster: {since.isoformat()} → {until.isoformat()}", flush=True)

    runs = fetch_github_runs(
        repo=args.repo, token=token, since=since, until=until
    )
    print(f"GitHub runs in venster: {len(runs)}", flush=True)

    items: list[DigestItem] = []
    if supabase_url and supabase_key:
        try:
            rows = fetch_dispatch_logs(
                supabase_url=supabase_url,
                service_key=supabase_key,
                since=since,
                until=until,
            )
            print(f"workflow_dispatch_log: {len(rows)}", flush=True)
            items.extend(items_price_eta(rows))
        except Exception as e:
            print(f"warn: supabase dispatch log: {e}", file=sys.stderr)
    else:
        print("warn: geen Supabase-credentials; prijs/ETA-stats overgeslagen", file=sys.stderr)

    items.extend(aggregate_from_logs(repo=args.repo, token=token, runs=runs))

    subject, text_body, html_body = build_digest(
        since=since, until=until, items=items
    )
    print("---", flush=True)
    print(subject, flush=True)
    print(text_body, flush=True)
    print("---", flush=True)

    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as f:
            f.write(f"## {subject}\n\n```\n{text_body}\n```\n")

    if not args.send:
        return 0

    api_key = (os.environ.get("RESEND_API_KEY") or "").strip()
    from_addr = (os.environ.get("DIGEST_FROM") or "").strip()
    to_raw = (os.environ.get("DIGEST_TO") or "").strip()
    missing = [
        k
        for k, v in (
            ("RESEND_API_KEY", api_key),
            ("DIGEST_FROM", from_addr),
            ("DIGEST_TO", to_raw),
        )
        if not v
    ]
    if missing:
        print(f"Kan niet mailen; secret(s) ontbreken: {', '.join(missing)}", file=sys.stderr)
        return 2
    to_addrs = [x.strip() for x in to_raw.split(",") if x.strip()]
    result = send_resend(
        api_key=api_key,
        from_addr=from_addr,
        to_addrs=to_addrs,
        subject=subject,
        text=text_body,
        html=html_body,
    )
    print(f"Resend: {json.dumps(result)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
