#!/usr/bin/env python3
"""Harvest echte merkenlogo's vanaf officiële domains — GEEN wordmarks.

Pipeline:
  1. Voor elk actief Motox-merk met domain in brand-domains.json
  2. Homepage scrapen → logo-kandidaten in header/nav (img/svg)
  3. Scoren + downloaden (reject favicon/flag/cart/og/sprite)
  4. fit_left + QA via motox_build_merk_logos
  5. Alleen geslaagde renders in batch20/

Geen Clearbit, geen text-fallbacks.
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import subprocess
import time
import unicodedata
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "output" / "merk-logos"
BATCH = OUT / "batch20"
SRC = OUT / "src-harvest"
DOMAINS = OUT / "brand-domains.json.DISABLED"
VENDORS = Path("/tmp/motox_active_vendors_exact.json")
REPORT = BATCH / "harvest-report.json"

UA = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
)
SKIP = frozenset({"none", "unknown", ""})
REJECT_SUB = (
    "favicon",
    "sprite",
    "placeholder",
    "1x1",
    "pixel",
    "icon-cart",
    "icons8",
    "flags/",
    "/flags/",
    "flag-",
    "payment",
    "badge",
    "avatar",
    "og-image",
    "opengraph",
    "twitter-card",
    "data:image",
)


def load_build():
    spec = importlib.util.spec_from_file_location(
        "motox_build_merk_logos", ROOT / "scripts" / "motox_build_merk_logos.py"
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


def vendor_handle(name: str) -> str:
    s = unicodedata.normalize("NFD", name.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


def score_url(url: str, alt: str = "", page_host: str = "") -> int:
    u = (url + " " + alt).lower()
    if any(x in u for x in REJECT_SUB):
        return -1000
    score = 0
    host = urlparse(url).netloc.lower()
    ph = (page_host or "").lower()
    if ph:
        # Prefer same registrable domain; heavily penalize off-site random assets
        base = ".".join(ph.split(".")[-2:])
        if base and base in host:
            score += 60
        elif host and base and base not in host:
            score -= 80
    if "logo" in u:
        score += 50
    if u.endswith(".svg") or ".svg" in u:
        score += 40
    if any(x in u for x in ("brand", "header", "site-logo", "navbar")):
        score += 20
    if any(x in u for x in ("wp-content", "cdn/shop", "images/")):
        score += 5
    m = re.search(r"[?&]width=(\d+)", u)
    if m and int(m.group(1)) >= 200:
        score += 10
    if any(x in u for x in ("thumb", "small", "32x", "64x", "48x", "poker", "buzztime")):
        score -= 40
    return score


def extract_candidates(html: str, base_url: str) -> list[tuple[int, str]]:
    soup = BeautifulSoup(html, "lxml")
    found: list[tuple[int, str]] = []
    page_host = urlparse(base_url).netloc
    selectors = [
        "header img",
        "nav img",
        ".logo img",
        "a.logo img",
        ".navbar-brand img",
        ".site-logo img",
        "img.logo",
        "img[src*='logo' i]",
        "img[alt*='logo' i]",
        "img[class*='logo' i]",
        "source[srcset*='logo' i]",
    ]
    nodes = []
    for sel in selectors:
        try:
            nodes.extend(soup.select(sel))
        except Exception:
            continue
    # also inline SVG use via <img> only; skip raw svg complex
    seen = set()
    for el in nodes:
        src = (
            el.get("src")
            or el.get("data-src")
            or el.get("data-lazy-src")
            or el.get("data-original")
            or ""
        )
        if not src and el.get("srcset"):
            src = el["srcset"].split(",")[0].strip().split(" ")[0]
        if not src or src.startswith("data:"):
            continue
        full = urljoin(base_url, src)
        if full in seen:
            continue
        seen.add(full)
        sc = score_url(full, el.get("alt") or "", page_host=page_host)
        if sc > 0:
            found.append((sc, full))
    found.sort(reverse=True)
    return found


def download(sess: requests.Session, url: str, dest: Path) -> Path:
    r = sess.get(url, timeout=(8, 25), allow_redirects=True)
    r.raise_for_status()
    ctype = (r.headers.get("Content-Type") or "").lower()
    if "html" in ctype:
        raise RuntimeError("got HTML")
    if len(r.content) < 500:
        raise RuntimeError(f"too small {len(r.content)}B")
    head = r.content[:200].lstrip()
    ext = Path(urlparse(url).path).suffix.lower()
    if head.startswith(b"<svg") or b"<svg" in head[:80] or "svg" in ctype:
        ext = ".svg"
    elif ext not in {".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"}:
        if "png" in ctype:
            ext = ".png"
        elif "jpeg" in ctype or "jpg" in ctype:
            ext = ".jpg"
        else:
            ext = ".png"
    out = dest.with_suffix(ext)
    out.write_bytes(r.content)
    if out.suffix != ".svg":
        subprocess.check_call(
            ["magick", "identify", str(out)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        w, h = map(
            int,
            subprocess.check_output(
                ["magick", "identify", "-format", "%w %h", str(out)], text=True
            ).split(),
        )
        if max(w, h) < 80 or (min(w, h) < 20 and max(w, h) < 120):
            raise RuntimeError(f"source too small {w}x{h}")
    return out


def harvest_one(
    sess: requests.Session,
    build,
    handle: str,
    domain: str,
    *,
    force: bool,
) -> dict:
    dest = BATCH / f"merk-{handle}.png"
    if dest.exists() and not force:
        try:
            build.qa(dest, force_black=handle in build.FORCE_BLACK_HANDLES)
            return {"handle": handle, "status": "exists_ok"}
        except Exception:
            pass

    base = domain if domain.startswith("http") else f"https://{domain}"
    try:
        r = sess.get(base, timeout=(8, 20), allow_redirects=True)
        r.raise_for_status()
    except Exception as e:
        return {"handle": handle, "status": "fetch_fail", "error": str(e)[:160]}

    cands = extract_candidates(r.text, r.url)
    if not cands:
        return {"handle": handle, "status": "no_candidates", "url": r.url}

    SRC.mkdir(parents=True, exist_ok=True)
    errors = []
    for sc, url in cands[:5]:
        # Reject corporate-group logos leaking onto sibling brands
        host = urlparse(url).netloc.lower()
        if "allballsracing" in host and handle not in {"all-balls", "hot-cams", "cylinder-works", "pivot-works"}:
            continue
        if handle not in {"all-balls"} and "allballsracing" in url.lower() and handle not in host.replace(".", "-"):
            # still allow if it's the brand's own asset path mentioning group — skip shared web-assets
            if "web-assets/all" in url.lower() or "allballsracinggroup" in url.lower():
                continue
        try:
            src = download(sess, url, SRC / handle)
            force_black = handle in build.FORCE_BLACK_HANDLES
            build.fit_left(src, dest, force_black=force_black)
            build.qa(dest, force_black=force_black)
            return {
                "handle": handle,
                "status": "ok",
                "url": url,
                "score": sc,
                "file": dest.name,
            }
        except Exception as e:
            errors.append(f"{url[:80]} → {e}"[:180])
            dest.unlink(missing_ok=True)
            continue
    return {
        "handle": handle,
        "status": "render_fail",
        "candidates": len(cands),
        "errors": errors[:3],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--offset", type=int, default=0)
    ap.add_argument("--only", default="")
    ap.add_argument("--sleep", type=float, default=0.7)
    ap.add_argument("--missing-only", action="store_true", help="Skip handles that already have batch20 PNG")
    args = ap.parse_args()

    build = load_build()
    BATCH.mkdir(parents=True, exist_ok=True)
    domains = json.loads(DOMAINS.read_text())
    vendors = json.loads(VENDORS.read_text())
    vendors = sorted(vendors, key=lambda x: (-x["active_products"], x["vendor"]))

    only = {x.strip() for x in args.only.split(",") if x.strip()} if args.only else set()
    have = {p.stem.replace("merk-", "") for p in BATCH.glob("merk-*.png")}

    sess = requests.Session()
    sess.trust_env = False
    sess.headers.update({"User-Agent": UA})

    jobs = []
    seen = set()
    for row in vendors:
        h = vendor_handle(row["vendor"])
        if h in SKIP or h in seen:
            continue
        seen.add(h)
        if only and h not in only:
            continue
        if args.missing_only and h in have:
            continue
        dom = domains.get(h)
        if not dom:
            continue
        jobs.append((h, dom, row["active_products"]))
    if args.offset:
        jobs = jobs[args.offset :]
    if args.limit:
        jobs = jobs[: args.limit]

    report = []
    ok = 0
    for i, (handle, dom, nprod) in enumerate(jobs, 1):
        print(f"[{i}/{len(jobs)}] {handle} ({nprod}) ← {dom}", flush=True)
        row = harvest_one(sess, build, handle, dom, force=args.force)
        print(f"  → {row['status']}" + (f" {row.get('url','')[:70]}" if row.get("url") else ""), flush=True)
        if row.get("errors"):
            print(f"    err: {row['errors'][0]}", flush=True)
        report.append(row)
        if row["status"] in {"ok", "exists_ok"}:
            ok += 1
        # Only sleep after network work
        if row["status"] not in {"exists_ok"}:
            time.sleep(args.sleep)

    prev = []
    if REPORT.exists():
        try:
            prev = json.loads(REPORT.read_text())
        except Exception:
            prev = []
    by_h = {r["handle"]: r for r in prev if isinstance(r, dict) and "handle" in r}
    for r in report:
        by_h[r["handle"]] = r
    REPORT.write_text(json.dumps(list(by_h.values()), indent=2, ensure_ascii=False))
    print(f"\nOK {ok}/{len(jobs)}  report={REPORT}", flush=True)
    print(f"batch20 logos: {len(list(BATCH.glob('merk-*.png')))}", flush=True)
    return 0 if ok or not jobs else 1


if __name__ == "__main__":
    raise SystemExit(main())
