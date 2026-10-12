#!/usr/bin/env python3
"""Excel SKU overlap vs Motox/KTM caches + check examples."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "motox" / "Voorraad winkel - WERKDOC EJIS.csv"
OUT = ROOT / "output" / "winkelvoorraad_routing.json"

LOC = {
    "bak": "Bak (product.metafields.global.parts_box)",
    "vak": "Vak (product.metafields.global.parts_compartment)",
    "verdieping": "Verdieping (product.metafields.global.parts_floor)",
    "rij": "Rij (product.metafields.global.parts_row)",
    "plank": "Plank (product.metafields.global.parts_shelve)",
}


def sku_key(s: str) -> str:
    return re.sub(r"[\s\-\.]+", "", str(s)).upper()


def loc_label(r) -> str:
    parts = []
    v = str(r.get(LOC["verdieping"], "")).strip()
    k = str(r.get(LOC["vak"], "")).strip()
    b = str(r.get(LOC["bak"], "")).strip()
    p = str(r.get(LOC["plank"], "")).strip()
    rij = str(r.get(LOC["rij"], "")).strip()
    if v:
        parts.append(v)
    if k:
        parts.append("kast A" if k.upper() == "A" else f"kast {k}")
    if rij:
        parts.append(f"rij {rij}")
    if b:
        parts.append(f"bak {b}")
    if p:
        parts.append(f"plank {p}")
    return " · ".join(parts) if parts else "(geen stelling-locatie)"


def main() -> None:
    df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    if cols[0].startswith("Unnamed") or cols[0].strip() == "":
        df = df.rename(columns={cols[0]: "Handle"})
    df.columns = [c.strip() for c in df.columns]
    df["tag"] = df["Tags"].astype(str).str.strip().replace("", "(leeg)")
    df["sku"] = df["Variant SKU"].astype(str).str.strip()
    df["sku_key"] = df["sku"].map(lambda s: sku_key(s) if s else "")
    df["title"] = df["titel"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["Variant Inventory Qty"], errors="coerce").fillna(0).astype(int)

    excel_keys = {k for k in df["sku_key"] if k and k != "0"}
    motox = {sku_key(s) for s in json.loads((ROOT / "cache/motox/shopify_skus.json").read_text()) if s}
    # KTM cache if present
    ktm_path = ROOT / "cache" / "shopify_skus.json"
    ktm = set()
    if ktm_path.exists():
        raw = json.loads(ktm_path.read_text())
        if isinstance(raw, list):
            ktm = {sku_key(s) for s in raw if s}
        elif isinstance(raw, dict):
            ktm = {sku_key(s) for s in raw.keys() if s}

    in_motox = excel_keys & motox
    only_ktm = (excel_keys & ktm) - motox if ktm else set()
    neither = excel_keys - motox - ktm
    both = excel_keys & motox & ktm if ktm else in_motox

    print("excel unique sku", len(excel_keys))
    print("motox cache", len(motox), "hit", len(in_motox))
    print("ktm cache", len(ktm), "only_ktm", len(only_ktm), "both", len(both), "neither", len(neither))

    with_sku = df[df["sku_key"] != ""]
    examples = []
    for key, g in with_sku.groupby("sku_key"):
        tags = sorted(set(g["tag"]))
        if "Partstream, stock" not in tags or "ejis" not in tags:
            continue
        eq = int(g.loc[g["tag"] == "ejis", "qty"].sum())
        pq = int(g.loc[g["tag"] == "Partstream, stock", "qty"].sum())
        if eq == pq:
            continue
        ps = g[g["tag"] == "Partstream, stock"].iloc[0]
        loc = loc_label(ps)
        if "bak " not in loc:
            continue
        title = g.loc[g["tag"] == "ejis", "title"].iloc[0]
        examples.append(
            {
                "sku": g["sku"].iloc[0],
                "title": title,
                "locatie": loc,
                "partstream": pq,
                "ejis": eq,
                "delta": pq - eq,
            }
        )
    examples.sort(key=lambda x: -abs(x["delta"]))

    wv = []
    for key, g in with_sku.groupby("sku_key"):
        tags = set(g["tag"])
        if "Winkelvoorraad, stock" not in tags or "ejis" not in tags:
            continue
        eq = int(g.loc[g["tag"] == "ejis", "qty"].sum())
        wq = int(g.loc[g["tag"] == "Winkelvoorraad, stock", "qty"].sum())
        if eq == wq:
            continue
        title = g.loc[g["tag"] == "ejis", "title"].iloc[0] or g["title"].iloc[0]
        wv.append(
            {
                "sku": g["sku"].iloc[0],
                "title": title,
                "winkelvoorraad": wq,
                "ejis": eq,
            }
        )
    wv.sort(key=lambda x: -abs(x["winkelvoorraad"] - x["ejis"]))

    ejis_zero = []
    for key, g in with_sku.groupby("sku_key"):
        if "ejis" not in set(g["tag"]) or "Partstream, stock" not in set(g["tag"]):
            continue
        eq = int(g.loc[g["tag"] == "ejis", "qty"].sum())
        pq = int(g.loc[g["tag"] == "Partstream, stock", "qty"].sum())
        if eq > 0 and pq == 0:
            ps = g[g["tag"] == "Partstream, stock"].iloc[0]
            ejis_zero.append(
                {
                    "sku": g["sku"].iloc[0],
                    "title": g.loc[g["tag"] == "ejis", "title"].iloc[0],
                    "locatie": loc_label(ps),
                    "ejis": eq,
                    "partstream": 0,
                }
            )

    equal = []
    for key, g in with_sku.groupby("sku_key"):
        if "ejis" not in set(g["tag"]) or "Partstream, stock" not in set(g["tag"]):
            continue
        eq = int(g.loc[g["tag"] == "ejis", "qty"].sum())
        pq = int(g.loc[g["tag"] == "Partstream, stock", "qty"].sum())
        if eq != pq or eq <= 0:
            continue
        ps = g[g["tag"] == "Partstream, stock"].iloc[0]
        loc = loc_label(ps)
        if "bak " not in loc:
            continue
        equal.append(
            {
                "sku": g["sku"].iloc[0],
                "title": g.loc[g["tag"] == "ejis", "title"].iloc[0],
                "locatie": loc,
                "aantal": eq,
            }
        )
        if len(equal) >= 5:
            break

    OUT.write_text(
        json.dumps(
            {
                "routing": {
                    "excel_unique_sku": len(excel_keys),
                    "in_motox": len(in_motox),
                    "only_ktm": len(only_ktm),
                    "in_both": len(both),
                    "in_neither": len(neither),
                    "ktm_cache_loaded": bool(ktm),
                },
                "stelling_verschil": examples[:10],
                "winkelvoorraad_vs_ejis": wv[:8],
                "ejis_pos_partstream_nul": ejis_zero[:6],
                "stelling_eens": equal,
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    print("wrote", OUT)


if __name__ == "__main__":
    main()
