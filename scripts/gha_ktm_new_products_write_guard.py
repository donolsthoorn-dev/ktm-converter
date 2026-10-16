#!/usr/bin/env python3
"""
Guard voor ktm_new_products_daily buiten shopify-write-lock-ktm.

Waarom: GHA cancelt oudere *pending* jobs in dezelfde concurrency-group als er
een nieuwere bij komt. Op de gedeelde Shopify-lock sneuvelt nieuwe-producten
daardoor door uurlijkse prijs/ETA. Deze guard:

  1) claim  — zet workflow_dispatch_log op running (Supabase skip nieuwe writes)
  2) wait   — wacht tot andere KTM Shopify-schrijfjobs klaar zijn
  3) release — markeer success/failed

Gebruik in GHA (met GITHUB_TOKEN + SUPABASE_*).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

WORKFLOW_FILE = "ktm_new_products_daily.yml"

# Workflows die shopify-write-lock-ktm gebruiken (of matrix-varianten daarvan).
KTM_SHOPIFY_WRITE_WORKFLOWS = (
    "ktm_price_eta_status_sync.yml",
    "ktm_customs_missing_fill.yml",
    "ktm_shopify_auto_deactivate_invalid_products.yml",
    "ktm_shopify_publish_sellable_active_products.yml",
    "ktm_shopify_gtin_fill.yml",
    "shops_synkro_clone_missing.yml",
    "ktm_seo.yml",
    "shops_category_fill_empty.yml",
    "shops_category_reclassify.yml",
    "ktm_ymm_delivery.yml",
)


def _env(name: str) -> str:
    return (os.environ.get(name) or "").strip()


def _gh_get(path: str, token: str) -> dict[str, Any]:
    url = f"https://api.github.com{path}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "ktm-new-products-write-guard",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _busy_runs(repo: str, token: str) -> list[str]:
    """Return korte labels van in_progress/queued runs op KTM write-workflows."""
    out: list[str] = []
    for wf in KTM_SHOPIFY_WRITE_WORKFLOWS:
        path = (
            f"/repos/{repo}/actions/workflows/{urllib.parse.quote(wf)}"
            f"/runs?per_page=20"
        )
        try:
            data = _gh_get(path, token)
        except urllib.error.HTTPError as e:
            if e.code == 404:
                continue
            raise
        for run in data.get("workflow_runs") or []:
            status = (run.get("status") or "").strip()
            if status not in ("in_progress", "queued", "waiting", "pending"):
                continue
            rid = run.get("id")
            out.append(f"{wf}#{rid}:{status}")
    return out


def cmd_wait(args: argparse.Namespace) -> int:
    token = _env("GITHUB_TOKEN") or _env("GH_TOKEN")
    repo = _env("GITHUB_REPOSITORY") or _env("REPO")
    if not token or not repo:
        print("wait: GITHUB_TOKEN/GITHUB_REPOSITORY ontbreekt", file=sys.stderr)
        return 2

    deadline = time.time() + max(60, int(args.timeout_minutes) * 60)
    poll = max(15, int(args.poll_seconds))
    print(
        f"Wacht tot KTM Shopify-schrijfjobs vrij zijn "
        f"(timeout={args.timeout_minutes}m, poll={poll}s)…",
        flush=True,
    )
    while True:
        busy = _busy_runs(repo, token)
        if not busy:
            print("KTM Shopify-schrijflock vrij — door.", flush=True)
            return 0
        left = int(deadline - time.time())
        if left <= 0:
            print(
                "Timeout: nog bezet door: " + ", ".join(busy[:12]),
                file=sys.stderr,
                flush=True,
            )
            return 1
        print(f"[wait] bezet ({len(busy)}): {', '.join(busy[:8])}  rest={left}s", flush=True)
        time.sleep(poll)


def _supabase_headers(key: str) -> dict[str, str]:
    return {
        "apikey": key,
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def _supabase_request(
    method: str,
    url: str,
    key: str,
    body: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers=_supabase_headers(key),
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8")
    if not raw.strip():
        return []
    parsed = json.loads(raw)
    if isinstance(parsed, list):
        return parsed
    if isinstance(parsed, dict):
        return [parsed]
    return []


def cmd_claim(args: argparse.Namespace) -> int:
    base = _env("SUPABASE_URL").rstrip("/")
    key = _env("SUPABASE_SERVICE_ROLE_KEY")
    if not base or not key:
        print("claim: SUPABASE_* ontbreekt — soft lock overgeslagen", flush=True)
        if args.github_output:
            with open(args.github_output, "a", encoding="utf-8") as fh:
                fh.write("log_id=\n")
        return 0

    url = f"{base}/rest/v1/workflow_dispatch_log"
    rows = _supabase_request(
        "POST",
        url,
        key,
        {
            "workflow_file": WORKFLOW_FILE,
            "mode": "apply",
            "status": "queued",
            "run_state": "running",
            "run_summary": "ktm_new_products_daily soft lock",
            "run_started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )
    log_id = rows[0].get("id") if rows else None
    print(f"Soft lock geclaimd: workflow_dispatch_log.id={log_id}", flush=True)
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as fh:
            fh.write(f"log_id={log_id or ''}\n")
    return 0


def cmd_release(args: argparse.Namespace) -> int:
    base = _env("SUPABASE_URL").rstrip("/")
    key = _env("SUPABASE_SERVICE_ROLE_KEY")
    log_id = (args.log_id or "").strip()
    if not base or not key or not log_id:
        print("release: geen log_id/Supabase — skip", flush=True)
        return 0

    state = "success" if args.ok else "failed"
    url = f"{base}/rest/v1/workflow_dispatch_log?id=eq.{urllib.parse.quote(log_id)}"
    _supabase_request(
        "PATCH",
        url,
        key,
        {
            "run_state": state,
            "status": "queued" if args.ok else "failed",
            "run_summary": f"ktm_new_products_daily {state}",
            "run_finished_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
    )
    print(f"Soft lock vrijgegeven: id={log_id} state={state}", flush=True)
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("wait", help="Wacht tot andere KTM Shopify-schrijfjobs klaar zijn")
    w.add_argument("--timeout-minutes", type=int, default=180)
    w.add_argument("--poll-seconds", type=int, default=30)
    w.set_defaults(func=cmd_wait)

    c = sub.add_parser("claim", help="Claim soft lock in workflow_dispatch_log")
    c.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT") or "")
    c.set_defaults(func=cmd_claim)

    r = sub.add_parser("release", help="Release soft lock")
    r.add_argument("--log-id", required=True)
    r.add_argument("--ok", action="store_true")
    r.set_defaults(func=cmd_release)

    args = p.parse_args()
    return int(args.func(args) or 0)


if __name__ == "__main__":
    raise SystemExit(main())
