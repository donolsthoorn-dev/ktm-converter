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
from dataclasses import dataclass, field
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

LABELS: dict[str, str] = {
    "ktm_price_eta_status_sync.yml": "KTM prijs/ETA",
    "motox_ebihr_sync.yml": "Motox e-Bihr sync",
    "motox_ebihr_seo.yml": "Motox e-Bihr SEO",
    "motox_putoline.yml": "Motox Putoline (Kroon)",
    "motox_deactivate_products_without_image.yml": "Motox zonder foto op draft",
    "motox_shopify_gtin_fill.yml": "Motox barcodes vullen",
    "motox_ymm_derived_fill.yml": "Motox YMM afgeleide velden",
    "ktm_seo.yml": "KTM SEO",
    "ktm_new_products_weekly.yml": "KTM nieuwe producten (wekelijks)",
    "ktm_ymm_delivery.yml": "KTM YMM-levering",
    "ktm_garage_fitment_deploy.yml": "KTM garage fitment",
    "ktm_job_worker.yml": "KTM Shopify-spiegel",
    "ktm_customs_missing_fill.yml": "KTM HS/herkomst aanvullen",
    "ktm_shopify_gtin_fill.yml": "KTM barcodes vullen",
    "ktm_shopify_auto_deactivate_invalid_products.yml": "KTM productstatus",
    "ktm_shopify_publish_sellable_active_products.yml": "KTM leverbare producten publiceren",
    "shops_category_fill_empty.yml": "Category vullen (KTM + Motox)",
    "shops_synkro_clone_missing.yml": "Synkro POS-kopieën",
}

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
class Section:
    title: str
    lines: list[str] = field(default_factory=list)
    sort_key: int = 100

    @property
    def has_content(self) -> bool:
        return bool(self.lines)


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


def section_price_eta(rows: list[dict[str, Any]]) -> Section | None:
    agg: Counter[str] = Counter()
    n = 0
    for r in rows:
        if (r.get("workflow_file") or "") != "ktm_price_eta_status_sync.yml":
            continue
        if r.get("run_state") != "success":
            continue
        stats = r.get("run_stats") or {}
        if not isinstance(stats, dict):
            continue
        n += 1
        agg["price"] += _sum_int(stats, "price_success")
        agg["eta"] += _sum_int(stats, "eta_success")
        agg["gtin"] += _sum_int(stats, "gtin_success")
        agg["policy"] += _sum_int(stats, "policy_success")
        agg["customs"] += _sum_int(stats, "customs_success")
        agg["product_status"] += _sum_int(stats, "product_status_success")
    if n == 0 or sum(agg.values()) == 0:
        return None
    bits = []
    if agg["price"]:
        bits.append(f"prijs {_nl_num(agg['price'])}")
    if agg["eta"]:
        bits.append(f"ETA {_nl_num(agg['eta'])}")
    if agg["gtin"]:
        bits.append(f"barcodes {_nl_num(agg['gtin'])}")
    if agg["policy"]:
        bits.append(f"voorraadbeleid {_nl_num(agg['policy'])}")
    if agg["customs"]:
        bits.append(f"HS/herkomst {_nl_num(agg['customs'])}")
    if agg["product_status"]:
        bits.append(f"productstatus {_nl_num(agg['product_status'])}")
    if not bits:
        return None
    # Split into two lines if long
    mid = (len(bits) + 1) // 2
    lines = ["  " + " · ".join(bits[:mid])]
    if bits[mid:]:
        lines.append("  " + " · ".join(bits[mid:]))
    return Section(LABELS["ktm_price_eta_status_sync.yml"], lines, sort_key=10)


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


def _metrics_to_lines(wf: str, totals: dict[str, int]) -> list[str] | None:
    if not any(v > 0 for v in totals.values()):
        return None
    if wf == "shops_synkro_clone_missing.yml":
        return [f"  {_nl_num(totals['getagd'])} producten getagd"]
    if wf in ("ktm_seo.yml", "motox_ebihr_seo.yml"):
        return [f"  {_nl_num(totals['geschreven'])} producten bijgewerkt"]
    if wf == "motox_ebihr_sync.yml":
        bits = []
        mapping = [
            ("created", "nieuw"),
            ("price", "prijzen"),
            ("stock_policy", "voorraadbeleid"),
            ("fitment", "fitment"),
            ("customs", "HS/herkomst"),
            ("articles", "artikelen"),
            ("images", "foto’s"),
            ("drafted", "op draft"),
        ]
        for k, label in mapping:
            if totals.get(k):
                bits.append(f"{label} {_nl_num(totals[k])}")
        if not bits:
            return None
        mid = (len(bits) + 1) // 2
        lines = ["  " + " · ".join(bits[:mid])]
        if bits[mid:]:
            lines.append("  " + " · ".join(bits[mid:]))
        return lines
    if wf in (
        "ktm_shopify_gtin_fill.yml",
        "motox_shopify_gtin_fill.yml",
        "motox_ymm_derived_fill.yml",
    ):
        return [f"  {_nl_num(totals['ok'])} bijgewerkt"]
    if wf == "ktm_shopify_auto_deactivate_invalid_products.yml":
        return [f"  {_nl_num(totals['ok'])} producten aangepast"]
    if wf == "ktm_shopify_publish_sellable_active_products.yml":
        return [f"  {_nl_num(totals['published'])} producten gepubliceerd"]
    if wf == "motox_deactivate_products_without_image.yml":
        return [f"  {_nl_num(totals['draft'])} producten op draft"]
    if wf == "shops_category_fill_empty.yml":
        return [f"  {_nl_num(totals['ok'])} producten van category voorzien"]
    if wf == "ktm_new_products_weekly.yml":
        return [f"  {_nl_num(totals['geschreven'])} geschreven"]
    if wf == "ktm_ymm_delivery.yml":
        return [f"  {_nl_num(totals['ok'])} producten gepusht"]
    if wf == "motox_putoline.yml":
        bits = []
        for k, label in (
            ("price", "prijzen"),
            ("ean", "EAN"),
            ("new", "nieuw"),
            ("updated", "bijgewerkt"),
        ):
            if totals.get(k):
                bits.append(f"{label} {_nl_num(totals[k])}")
        return ["  " + " · ".join(bits)] if bits else None
    if wf == "ktm_job_worker.yml":
        return [
            f"  catalogus vernieuwd (~{_nl_num(totals['products'])} producten)"
        ]
    if wf == "ktm_customs_missing_fill.yml":
        if totals.get("written"):
            return [f"  {_nl_num(totals['written'])} bijgewerkt"]
        return None
    return None


def aggregate_from_logs(
    *,
    repo: str,
    token: str,
    runs: list[dict[str, Any]],
) -> list[Section]:
    by_wf: dict[str, list[int]] = defaultdict(list)
    for r in runs:
        if (r.get("conclusion") or "") != "success":
            continue
        path = (r.get("path") or "").split("/")[-1]
        if path not in LABELS or path == "ktm_price_eta_status_sync.yml":
            continue
        by_wf[path].append(int(r["id"]))

    sections: list[Section] = []

    def work(item: tuple[str, list[int]]) -> Section | None:
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
        lines = _metrics_to_lines(wf, dict(totals))
        if not lines:
            return None
        sort = {
            "motox_ebihr_sync.yml": 20,
            "ktm_seo.yml": 30,
            "motox_ebihr_seo.yml": 31,
            "ktm_new_products_weekly.yml": 40,
            "shops_synkro_clone_missing.yml": 50,
            "ktm_shopify_auto_deactivate_invalid_products.yml": 60,
            "ktm_shopify_publish_sellable_active_products.yml": 61,
            "ktm_shopify_gtin_fill.yml": 70,
            "motox_shopify_gtin_fill.yml": 71,
            "ktm_customs_missing_fill.yml": 72,
            "shops_category_fill_empty.yml": 80,
            "motox_putoline.yml": 90,
            "motox_deactivate_products_without_image.yml": 91,
            "motox_ymm_derived_fill.yml": 92,
            "ktm_ymm_delivery.yml": 93,
            "ktm_job_worker.yml": 95,
        }.get(wf, 100)
        return Section(LABELS[wf], lines, sort_key=sort)

    items = list(by_wf.items())
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        for sec in pool.map(work, items):
            if sec:
                sections.append(sec)
    return sections


def build_digest(
    *,
    since: datetime,
    until: datetime,
    sections: list[Section],
) -> tuple[str, str]:
    subject = f"KTM digest — {_fmt_nl_short(until)}"
    header = [
        "KTM / Motox — dagelijkse digest",
        f"Periode: {_fmt_nl(since)} → {_fmt_nl(until)}",
        "",
    ]
    body_sections = [s for s in sorted(sections, key=lambda x: x.sort_key) if s.has_content]
    if not body_sections:
        body = "\n".join(header + ["Geen bijgewerkte resultaten in dit venster."])
        return subject, body
    parts = list(header)
    for s in body_sections:
        parts.append(f"• {s.title}")
        parts.extend(s.lines)
        parts.append("")
    while parts and parts[-1] == "":
        parts.pop()
    return subject, "\n".join(parts)


def send_resend(
    *,
    api_key: str,
    from_addr: str,
    to_addrs: list[str],
    subject: str,
    text: str,
) -> dict[str, Any]:
    resp = requests.post(
        "https://api.resend.com/emails",
        headers={"Authorization": f"Bearer {api_key}"},
        json={
            "from": from_addr,
            "to": to_addrs,
            "subject": subject,
            "text": text,
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

    sections: list[Section] = []
    if supabase_url and supabase_key:
        try:
            rows = fetch_dispatch_logs(
                supabase_url=supabase_url,
                service_key=supabase_key,
                since=since,
                until=until,
            )
            print(f"workflow_dispatch_log: {len(rows)}", flush=True)
            pe = section_price_eta(rows)
            if pe:
                sections.append(pe)
        except Exception as e:
            print(f"warn: supabase dispatch log: {e}", file=sys.stderr)
    else:
        print("warn: geen Supabase-credentials; prijs/ETA-stats overgeslagen", file=sys.stderr)

    sections.extend(
        aggregate_from_logs(repo=args.repo, token=token, runs=runs)
    )

    subject, body = build_digest(since=since, until=until, sections=sections)
    print("---", flush=True)
    print(subject, flush=True)
    print(body, flush=True)
    print("---", flush=True)

    gh_summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if gh_summary:
        with open(gh_summary, "a", encoding="utf-8") as f:
            f.write(f"## {subject}\n\n```\n{body}\n```\n")

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
        text=body,
    )
    print(f"Resend: {json.dumps(result)}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
