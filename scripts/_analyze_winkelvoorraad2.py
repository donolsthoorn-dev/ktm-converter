#!/usr/bin/env python3
"""Deeper overlap / structure analysis of the winkelvoorraad CSV."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

SRC = Path("/Users/donolsthoorn/Documents/ktm_project/motox/Voorraad winkel - WERKDOC EJIS.csv")
OUT = Path("/Users/donolsthoorn/Documents/ktm_project/output/winkelvoorraad_analysis2.json")


def sku_key(s: str) -> str:
    return re.sub(r"[\s\-\.]+", "", str(s)).upper()


def parse_euro(val):
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("€", "").replace(" ", "").replace("\xa0", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
    cols = list(df.columns)
    if cols[0].startswith("Unnamed") or cols[0].strip() == "":
        df = df.rename(columns={cols[0]: "Handle"})
    df.columns = [c.strip() for c in df.columns]
    df["row"] = range(1, len(df) + 1)  # 1-based data row
    df["tag"] = df["Tags"].astype(str).str.strip()
    df["tag_label"] = df["tag"].replace("", "(leeg)")
    df["sku"] = df["Variant SKU"].astype(str).str.strip()
    df["sku_key"] = df["sku"].map(lambda s: sku_key(s) if s else "")
    df["title"] = df["titel"].astype(str).str.strip()
    df["title_key"] = df["title"].str.casefold().str.replace(r"\s+", " ", regex=True)
    df["qty"] = pd.to_numeric(df["Variant Inventory Qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = df["Variant Price"].map(parse_euro)
    df["cost"] = df["Inkoop exc"].map(parse_euro)
    df["handle"] = df["Handle"].astype(str).str.strip()

    loc = {
        "bak": "Bak (product.metafields.global.parts_box)",
        "vak": "Vak (product.metafields.global.parts_compartment)",
        "verdieping": "Verdieping (product.metafields.global.parts_floor)",
        "rij": "Rij (product.metafields.global.parts_row)",
        "plank": "Plank (product.metafields.global.parts_shelve)",
    }

    report: dict = {}

    # --- block structure ---
    blocks = []
    prev = None
    start = None
    for i, t in enumerate(df["tag_label"]):
        if t != prev:
            if prev is not None:
                blocks.append({"tag": prev, "start": start + 2, "end": i + 1, "rows": i - start})
            prev = t
            start = i
    blocks.append({"tag": prev, "start": start + 2, "end": len(df) + 1, "rows": len(df) - start})
    report["n_blocks"] = len(blocks)
    report["blocks"] = blocks[:40]
    report["n_blocks_total"] = len(blocks)
    report["block_tag_changes"] = len(blocks)

    # --- pairwise SKU overlap ---
    tags = sorted(df["tag_label"].unique())
    sku_sets = {}
    for t in tags:
        sku_sets[t] = set(df.loc[(df["tag_label"] == t) & (df["sku_key"] != ""), "sku_key"])
    matrix = []
    for a in tags:
        row = {"tag": a, "unique_sku": len(sku_sets[a])}
        for b in tags:
            if a == b:
                row[b] = len(sku_sets[a])
            else:
                row[b] = len(sku_sets[a] & sku_sets[b])
        matrix.append(row)
    report["sku_overlap_matrix"] = matrix

    # --- pairwise title overlap ---
    title_sets = {}
    for t in tags:
        title_sets[t] = set(df.loc[(df["tag_label"] == t) & (df["title_key"] != ""), "title_key"])
    tmatrix = []
    for a in tags:
        row = {"tag": a, "unique_titles": len(title_sets[a])}
        for b in tags:
            row[b] = len(title_sets[a] & title_sets[b]) if a != b else len(title_sets[a])
        tmatrix.append(row)
    report["title_overlap_matrix"] = tmatrix

    # --- empty tag composition ---
    empty = df[df["tag_label"] == "(leeg)"].copy()
    bike_re = re.compile(
        r"\b(sx|exc|duke|adventure|enduro|smc|smr|rc |ktm \d|scott |patron|strike|spark|scale|genius|scarp|chicago|ultra |head |mtb|e-?ride|bike|fiets)\b",
        re.I,
    )
    vin_re = re.compile(r"^VBK", re.I)
    empty["looks_bike"] = (
        empty["sku"].str.match(vin_re)
        | empty["title"].str.contains(bike_re, na=False)
        | (empty["price"].fillna(0) >= 1000)
    )
    # more precise bike: VIN-like SKU or high price with bike words
    empty["vin_like"] = empty["sku"].str.match(r"^(VBK|WBS|VIN)", case=False) | empty["sku"].str.contains(
        r"^\d{6}-\d{3}", regex=True
    )
    empty["high_price"] = empty["price"].fillna(0) >= 800
    report["empty_tag"] = {
        "rows": int(len(empty)),
        "missing_title": int((empty["title"] == "").sum()),
        "missing_sku": int((empty["sku"] == "").sum()),
        "missing_price": int(empty["price"].isna().sum()),
        "qty_sum": int(empty["qty"].sum()),
        "high_price_ge_800": int(empty["high_price"].sum()),
        "vin_or_scott_sku": int(empty["vin_like"].sum()),
        "price_buckets": {
            "no_price": int(empty["price"].isna().sum()),
            "<50": int((empty["price"].fillna(-1).between(0, 49.99)).sum()),
            "50-199": int((empty["price"].fillna(-1).between(50, 199.99)).sum()),
            "200-799": int((empty["price"].fillna(-1).between(200, 799.99)).sum()),
            "800-2999": int((empty["price"].fillna(-1).between(800, 2999.99)).sum()),
            "3000+": int((empty["price"].fillna(0) >= 3000).sum()),
        },
        "sample_high": empty.loc[empty["high_price"], ["title", "sku", "qty", "price"]].head(8).to_dict("records"),
        "sample_low": empty.loc[~empty["high_price"] & (empty["title"] != ""), ["title", "sku", "qty", "price"]].head(8).to_dict("records"),
        "sample_no_title": empty.loc[empty["title"] == "", ["sku", "qty", "cost"]].head(10).to_dict("records"),
    }

    # empty vs winkelvoorraad: same title AND same sku vs title-only
    wv = df[df["tag_label"] == "Winkelvoorraad, stock"]
    empty_skus = sku_sets["(leeg)"]
    wv_skus = sku_sets["Winkelvoorraad, stock"]
    empty_titles = title_sets["(leeg)"]
    wv_titles = title_sets["Winkelvoorraad, stock"]
    both_sku = empty_skus & wv_skus
    both_title = empty_titles & wv_titles
    # qty compare for title matches
    qty_same = qty_diff = 0
    qty_ex = []
    for tk in list(both_title)[:]:
        e = empty[empty["title_key"] == tk]
        w = wv[wv["title_key"] == tk]
        eq, wq = int(e["qty"].sum()), int(w["qty"].sum())
        if eq == wq:
            qty_same += 1
        else:
            qty_diff += 1
            if len(qty_ex) < 10:
                qty_ex.append(
                    {
                        "title": e["title"].iloc[0][:70],
                        "empty_qty": eq,
                        "wv_qty": wq,
                        "empty_sku": e["sku"].iloc[0],
                        "wv_sku": w["sku"].iloc[0],
                    }
                )
    report["empty_vs_winkelvoorraad"] = {
        "sku_overlap": len(both_sku),
        "title_overlap": len(both_title),
        "title_qty_same": qty_same,
        "title_qty_diff": qty_diff,
        "title_qty_diff_examples": qty_ex,
        "empty_rows_matching_wv_title": int(empty["title_key"].isin(both_title).sum()),
        "wv_rows_matching_empty_title": int(wv["title_key"].isin(both_title).sum()),
    }

    # --- bikes tag vs empty high-price ---
    bikes = df[df["tag_label"] == "bikes"]
    bike_titles = set(bikes["title_key"])
    empty_bike_title_hit = empty[empty["title_key"].isin(bike_titles)]
    report["bikes_vs_empty"] = {
        "bikes_rows": int(len(bikes)),
        "bikes_missing_sku": int((bikes["sku"] == "").sum()),
        "title_overlap_with_empty": int(len(bike_titles & empty_titles)),
        "empty_rows_same_title": int(len(empty_bike_title_hit)),
        "examples": [],
    }
    # fuzzy: empty titles that start similarly
    # compare normalized without size
    def bike_model(t: str) -> str:
        t = re.sub(r"\b(size\s*)?[xslm]{1,2}\b", "", t, flags=re.I)
        return re.sub(r"\s+", " ", t).strip()

    bikes_models = {bike_model(t) for t in bikes["title_key"] if t}
    empty_models = empty.copy()
    empty_models["model"] = empty_models["title_key"].map(bike_model)
    hits = empty_models[empty_models["model"].isin(bikes_models) & (empty_models["model"] != "")]
    report["bikes_vs_empty"]["model_overlap_empty_rows"] = int(len(hits))
    report["bikes_vs_empty"]["examples"] = (
        pd.concat(
            [
                bikes[["title", "sku", "qty", "price"]].head(3).assign(src="bikes"),
                hits[["title", "sku", "qty", "price"]].head(5).assign(src="(leeg)"),
            ]
        ).to_dict("records")
        if len(hits)
        else bikes[["title", "sku", "qty", "price"]].head(5).assign(src="bikes").to_dict("records")
    )

    # --- location distributions ---
    loc_stats = {}
    for t in ["Partstream, stock", "stock"]:
        g = df[df["tag_label"] == t]
        loc_stats[t] = {}
        for k, col in loc.items():
            vals = g[col].astype(str).str.strip()
            filled = vals[vals != ""]
            loc_stats[t][k] = {
                "filled": int(len(filled)),
                "unique": int(filled.nunique()),
                "top": filled.value_counts().head(12).to_dict(),
            }
        # combo samples
        combos = g.apply(
            lambda r: " / ".join(
                f"{k}={str(r[col]).strip()}" for k, col in loc.items() if str(r[col]).strip()
            ),
            axis=1,
        )
        loc_stats[t]["combo_top"] = combos[combos != ""].value_counts().head(15).to_dict()
        loc_stats[t]["combo_unique"] = int(combos[combos != ""].nunique())
    report["location"] = loc_stats

    # --- Partstream titles that are just SKU ---
    ps = df[df["tag_label"] == "Partstream, stock"]
    title_is_sku = int((ps["title"].map(sku_key) == ps["sku_key"]).sum())
    report["partstream"] = {
        "rows": int(len(ps)),
        "title_equals_sku": title_is_sku,
        "has_image": int((ps["Image Src"].astype(str).str.strip() != "").sum()),
        "option1": ps["Option1 Value"].value_counts().head(8).to_dict() if "Option1 Value" in ps.columns else {},
        "handles_sample": ps.loc[ps["handle"] != "", "handle"].head(10).tolist(),
        "internal_dup_skus": int(ps.duplicated("sku_key").sum()),
    }

    # --- junk SKUs ---
    junk = df[df["sku_key"].isin({"", "0", "X", "NONE", "NULL", "NA"}) | (df["sku"].str.len() <= 2)]
    report["junk_sku_rows"] = int(((df["sku"] != "") & ((df["sku_key"] == "0") | (df["sku"].str.len() <= 2))).sum())
    report["sku_0_rows"] = int((df["sku_key"] == "0").sum())
    report["sku_0_titles"] = df.loc[df["sku_key"] == "0", ["tag_label", "title", "sku"]].head(8).to_dict("records")

    # --- ejis vs each other source qty ---
    ejis = df[df["tag_label"] == "ejis"]
    per_source = {}
    for t in tags:
        if t == "ejis":
            continue
        inter = sku_sets["ejis"] & sku_sets[t]
        same = diff = ejis_only_qty = other_only_qty = 0
        same_n = diff_n = 0
        for k in inter:
            eq = int(ejis.loc[ejis["sku_key"] == k, "qty"].sum())
            oq = int(df.loc[(df["tag_label"] == t) & (df["sku_key"] == k), "qty"].sum())
            if eq == oq:
                same += eq
                same_n += 1
            else:
                diff_n += 1
            if eq > 0 and oq == 0:
                ejis_only_qty += 1
            if oq > 0 and eq == 0:
                other_only_qty += 1
        per_source[t] = {
            "sku_overlap": len(inter),
            "qty_equal_keys": same_n,
            "qty_diff_keys": diff_n,
            "ejis_pos_other_zero": ejis_only_qty,
            "other_pos_ejis_zero": other_only_qty,
        }
    report["ejis_vs_each_source"] = per_source

    # --- missing titles overall ---
    report["missing_title_by_tag"] = (
        df[df["title"] == ""]["tag_label"].value_counts().to_dict()
    )
    report["missing_price_by_tag"] = df[df["price"].isna()]["tag_label"].value_counts().to_dict()
    report["cost_zero_by_tag"] = df[df["cost"].fillna(-1) == 0]["tag_label"].value_counts().to_dict()

    # --- handles ---
    report["handles"] = df.loc[df["handle"] != "", ["handle", "tag_label", "title", "sku"]].head(30).to_dict("records")
    report["n_handles"] = int((df["handle"] != "").sum())

    # --- value at risk if we sum overlap ---
    # if we naively import all rows, qty double-count
    all_qty = int(df["qty"].sum())
    # unique sku keeping max qty
    with_sku = df[df["sku_key"] != ""]
    max_qty = int(with_sku.groupby("sku_key")["qty"].max().sum())
    sum_qty = int(with_sku.groupby("sku_key")["qty"].sum().sum())
    no_sku_qty = int(df.loc[df["sku_key"] == "", "qty"].sum())
    report["qty_if"] = {
        "raw_sum_all_rows": all_qty,
        "sum_per_sku_then_total": sum_qty + no_sku_qty,
        "max_per_sku_then_total": max_qty + no_sku_qty,
        "no_sku_qty": no_sku_qty,
        "no_sku_rows": int((df["sku_key"] == "").sum()),
    }

    # preferred keep rule: drop ejis when SKU exists elsewhere; also drop empty when title/sku in winkelvoorraad?
    # compute remaining qty for a proposed rule:
    # 1. Keep Partstream, Winkelvoorraad, stock, bikes, Winkelvoorraad
    # 2. Keep ejis only if SKU not in any of the above
    # 3. Keep empty only if SKU not in any tagged source AND title not in winkelvoorraad/bikes
    tagged_skus = set()
    for t in ["Partstream, stock", "Winkelvoorraad, stock", "stock", "Winkelvoorraad", "bikes"]:
        tagged_skus |= sku_sets[t]
    keep_ejis = ~((df["tag_label"] == "ejis") & (df["sku_key"].isin(tagged_skus)))
    wv_and_bikes_titles = title_sets["Winkelvoorraad, stock"] | title_sets["bikes"] | title_sets.get("Winkelvoorraad", set())
    other_tagged_skus = tagged_skus | sku_sets["ejis"]
    drop_empty = (df["tag_label"] == "(leeg)") & (
        ((df["sku_key"] != "") & df["sku_key"].isin(other_tagged_skus))
        | ((df["title_key"] != "") & df["title_key"].isin(wv_and_bikes_titles))
    )
    keep = keep_ejis & ~drop_empty
    report["proposed_keep"] = {
        "rows_keep": int(keep.sum()),
        "rows_drop": int((~keep).sum()),
        "qty_keep": int(df.loc[keep, "qty"].sum()),
        "qty_drop": int(df.loc[~keep, "qty"].sum()),
        "drop_by_tag": df.loc[~keep, "tag_label"].value_counts().to_dict(),
        "keep_by_tag": df.loc[keep, "tag_label"].value_counts().to_dict(),
        "drop_empty_rows": int(drop_empty.sum()),
        "drop_ejis_rows": int((~keep_ejis).sum()),
    }

    # empty leftover composition
    leftover_empty = df[keep & (df["tag_label"] == "(leeg)")]
    report["leftover_empty"] = {
        "rows": int(len(leftover_empty)),
        "qty": int(leftover_empty["qty"].sum()),
        "missing_title": int((leftover_empty["title"] == "").sum()),
        "missing_sku": int((leftover_empty["sku"] == "").sum()),
        "high_price": int((leftover_empty["price"].fillna(0) >= 800).sum()),
        "samples": leftover_empty.loc[leftover_empty["title"] != "", ["title", "sku", "qty", "price"]].head(12).to_dict("records"),
    }

    # stock vs partstream overlap details
    inter_ps_stock = sku_sets["Partstream, stock"] & sku_sets["stock"]
    ps_stock_ex = []
    for k in list(inter_ps_stock)[:8]:
        rows = df[df["sku_key"] == k][["tag_label", "title", "sku", "qty"] + [loc["vak"], loc["verdieping"], loc["bak"]]]
        ps_stock_ex.append(rows.head(6).to_dict("records"))
    report["partstream_vs_stock"] = {
        "sku_overlap": len(inter_ps_stock),
        "examples": ps_stock_ex,
    }

    # Partstream vs ejis: title quality
    inter_ps_ejis = sku_sets["Partstream, stock"] & sku_sets["ejis"]
    ps_title_sku = 0
    ejis_has_real_title = 0
    qty_eq = 0
    for k in inter_ps_ejis:
        p = ps[ps["sku_key"] == k].iloc[0]
        e = ejis[ejis["sku_key"] == k].iloc[0]
        if sku_key(p["title"]) == p["sku_key"] or p["title"] == p["sku"]:
            ps_title_sku += 1
        if e["title"] and sku_key(e["title"]) != e["sku_key"]:
            ejis_has_real_title += 1
        if int(p["qty"]) == int(e["qty"]):
            qty_eq += 1
    report["partstream_vs_ejis"] = {
        "sku_overlap": len(inter_ps_ejis),
        "partstream_title_is_sku": ps_title_sku,
        "ejis_has_real_title": ejis_has_real_title,
        "qty_equal": qty_eq,
        "qty_not_equal": len(inter_ps_ejis) - qty_eq,
    }

    # stock rows with empty title
    st = df[df["tag_label"] == "stock"]
    report["stock_profile"] = {
        "rows": int(len(st)),
        "missing_title": int((st["title"] == "").sum()),
        "missing_price": int(st["price"].isna().sum()),
        "with_price": int(st["price"].notna().sum()),
        "location_vak_top": st[loc["vak"]].value_counts().head(15).to_dict(),
        "location_verdieping_top": st[loc["verdieping"]].value_counts().head(15).to_dict(),
        "sample_with_title": st.loc[st["title"] != "", ["title", "sku", "qty"]].head(8).to_dict("records"),
        "sample_no_title": st.loc[st["title"] == "", ["sku", "qty", loc["vak"], loc["verdieping"]]].head(8).to_dict("records"),
    }

    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    print("blocks", len(blocks))
    print("ejis vs sources", json.dumps(per_source, indent=2))
    print("empty vs wv", report["empty_vs_winkelvoorraad"])
    print("proposed", report["proposed_keep"])
    print("ps vs ejis", report["partstream_vs_ejis"])
    print("qty_if", report["qty_if"])


if __name__ == "__main__":
    main()
