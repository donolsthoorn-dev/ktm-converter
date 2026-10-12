#!/usr/bin/env python3
"""One-off analysis of motox/Voorraad winkel - WERKDOC EJIS.csv (tab 1)."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

SRC = Path("/Users/donolsthoorn/Documents/ktm_project/motox/Voorraad winkel - WERKDOC EJIS.csv")
OUT = Path("/Users/donolsthoorn/Documents/ktm_project/output/winkelvoorraad_analysis.json")


def parse_euro(val) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    s = s.replace("€", "").replace(" ", "").replace("\xa0", "")
    # Dutch: 1.234,56 or 1234,56
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def norm_sku(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    s = str(val).strip()
    if s.lower() in {"nan", "none"}:
        return ""
    return s


def norm_tag(val) -> str:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return ""
    return str(val).strip()


def sku_key(s: str) -> str:
    return re.sub(r"[\s\-\.]+", "", s).upper()


def main() -> None:
    df = pd.read_csv(SRC, dtype=str, keep_default_na=False)
    # first unnamed column is handle
    cols = list(df.columns)
    if cols[0].startswith("Unnamed") or cols[0].strip() == "":
        df = df.rename(columns={cols[0]: "Handle"})
    df.columns = [c.strip() for c in df.columns]

    n = len(df)
    report: dict = {
        "file": str(SRC),
        "n_rows": n,
        "n_cols": len(df.columns),
        "columns": list(df.columns),
        "nonempty_per_col": {},
    }

    for c in df.columns:
        nonempty = (df[c].astype(str).str.strip() != "").sum()
        report["nonempty_per_col"][c] = int(nonempty)

    df["tag"] = df["Tags"].map(norm_tag)
    df["sku"] = df["Variant SKU"].map(norm_sku)
    df["sku_key"] = df["sku"].map(sku_key)
    df["title"] = df["titel"].astype(str).str.strip()
    df["barcode"] = df["Variant Barcode"].astype(str).str.strip()
    df["qty"] = pd.to_numeric(df["Variant Inventory Qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = df["Variant Price"].map(parse_euro)
    df["cost"] = df["Inkoop exc"].map(parse_euro)
    df["status"] = df.get("Status", pd.Series([""] * n)).astype(str).str.strip()
    df["vendor"] = df["Vendor"].astype(str).str.strip()
    df["ptype"] = df["Type"].astype(str).str.strip()
    df["published"] = df["Published"].astype(str).str.strip()

    loc_cols = [
        "Bak (product.metafields.global.parts_box)",
        "Vak (product.metafields.global.parts_compartment)",
        "Verdieping (product.metafields.global.parts_floor)",
        "Plaatsing_list (product.metafields.global.parts_list)",
        "Rij (product.metafields.global.parts_row)",
        "Plank (product.metafields.global.parts_shelve)",
    ]
    loc_cols = [c for c in loc_cols if c in df.columns]
    df["has_location"] = df[loc_cols].apply(
        lambda r: any(str(v).strip() != "" for v in r), axis=1
    )

    # --- tags / sources ---
    tag_counts = Counter(df["tag"])
    report["tags"] = {
        "unique": sorted(tag_counts.keys()),
        "counts": dict(tag_counts.most_common()),
        "empty": int((df["tag"] == "").sum()),
    }

    # tag combos if comma-separated
    combo = Counter()
    for t in df["tag"]:
        parts = tuple(sorted(p.strip() for p in t.split(",") if p.strip()))
        combo[str(parts) if parts else "()"] += 1
    report["tag_combos"] = dict(combo.most_common())

    # --- per source stats ---
    sources = {}
    for tag, g in df.groupby("tag", dropna=False):
        label = tag if tag else "(leeg)"
        nonempty_loc = int(g["has_location"].sum())
        sku_empty = int((g["sku"] == "").sum())
        sku_dups_internal = int(g[g["sku"] != ""].duplicated("sku_key").sum())
        qty_sum = int(g["qty"].sum())
        qty_pos = int((g["qty"] > 0).sum())
        qty_zero = int((g["qty"] == 0).sum())
        qty_neg = int((g["qty"] < 0).sum())
        price_ok = int(g["price"].notna().sum())
        cost_ok = int(g["cost"].notna().sum())
        barcode_ok = int((g["barcode"] != "").sum())
        vendors = Counter(g["vendor"]).most_common(8)
        types = Counter(g["ptype"]).most_common(8)
        status = Counter(g["status"]).most_common()
        published = Counter(g["published"]).most_common()
        loc_fill = {c: int((g[c].astype(str).str.strip() != "").sum()) for c in loc_cols}
        sample_titles = g["title"].head(5).tolist()
        sample_skus = g["sku"].head(5).tolist()
        sources[label] = {
            "rows": int(len(g)),
            "unique_sku": int(g.loc[g["sku"] != "", "sku_key"].nunique()),
            "empty_sku": sku_empty,
            "internal_sku_dup_rows": sku_dups_internal,
            "qty_sum": qty_sum,
            "qty_positive_rows": qty_pos,
            "qty_zero_rows": qty_zero,
            "qty_neg_rows": qty_neg,
            "with_price": price_ok,
            "with_cost": cost_ok,
            "with_barcode": barcode_ok,
            "with_location": nonempty_loc,
            "location_fill": loc_fill,
            "vendors": vendors,
            "types": types,
            "status": status,
            "published": published,
            "sample_titles": sample_titles,
            "sample_skus": sample_skus,
        }
    report["sources"] = sources

    # --- overall qty ---
    report["qty"] = {
        "sum": int(df["qty"].sum()),
        "positive_rows": int((df["qty"] > 0).sum()),
        "zero_rows": int((df["qty"] == 0).sum()),
        "neg_rows": int((df["qty"] < 0).sum()),
        "max": int(df["qty"].max()),
        "p50": float(df["qty"].median()),
    }

    # --- SKU duplicates overall ---
    with_sku = df[df["sku"] != ""].copy()
    sku_groups = with_sku.groupby("sku_key")
    dup_skus = sku_groups.filter(lambda g: len(g) > 1)
    n_dup_keys = dup_skus["sku_key"].nunique()
    report["sku_dup_overall"] = {
        "rows_with_sku": int(len(with_sku)),
        "unique_sku_keys": int(with_sku["sku_key"].nunique()),
        "duplicate_sku_keys": int(n_dup_keys),
        "rows_in_duplicate_groups": int(len(dup_skus)),
    }

    # duplicate SKUs by tag combination of the group
    dup_combo = Counter()
    dup_examples = []
    same_tag_dups = 0
    cross_tag_dups = 0
    ejis_vs_other = 0
    ejis_vs_other_examples = []
    qty_conflict = 0
    qty_same = 0
    title_same = 0
    title_diff = 0
    price_diff = 0
    loc_on_non_ejis = 0
    loc_on_ejis = 0

    for key, g in dup_skus.groupby("sku_key"):
        tags = sorted({t if t else "(leeg)" for t in g["tag"]})
        combo_label = " + ".join(tags)
        dup_combo[combo_label] += 1
        if len(tags) == 1:
            same_tag_dups += 1
        else:
            cross_tag_dups += 1
        has_ejis = "ejis" in tags
        has_other = any(t != "ejis" for t in tags)
        if has_ejis and has_other:
            ejis_vs_other += 1
            if len(ejis_vs_other_examples) < 25:
                rows = []
                for _, r in g.iterrows():
                    rows.append(
                        {
                            "tag": r["tag"] or "(leeg)",
                            "title": r["title"][:80],
                            "sku": r["sku"],
                            "qty": int(r["qty"]),
                            "price": r["price"],
                            "cost": r["cost"],
                            "barcode": r["barcode"][:40],
                            "has_location": bool(r["has_location"]),
                            "bak": str(r.get(loc_cols[0], ""))[:20] if loc_cols else "",
                        }
                    )
                ejis_vs_other_examples.append({"sku_key": key, "sku": g["sku"].iloc[0], "rows": rows})

        qtys = sorted(set(int(x) for x in g["qty"]))
        if len(qtys) > 1:
            qty_conflict += 1
        else:
            qty_same += 1
        titles = {t.casefold() for t in g["title"]}
        if len(titles) == 1:
            title_same += 1
        else:
            title_diff += 1
        prices = {p for p in g["price"] if p is not None}
        if len(prices) > 1:
            price_diff += 1

        if has_ejis and has_other:
            if g.loc[g["tag"] == "ejis", "has_location"].any():
                loc_on_ejis += 1
            if g.loc[g["tag"] != "ejis", "has_location"].any():
                loc_on_non_ejis += 1

        if len(dup_examples) < 8 and len(tags) == 1:
            dup_examples.append(
                {
                    "sku": g["sku"].iloc[0],
                    "tags": tags,
                    "titles": g["title"].tolist(),
                    "qtys": [int(x) for x in g["qty"]],
                }
            )

    report["sku_dup_combos"] = dict(dup_combo.most_common())
    report["sku_dup_same_tag_keys"] = same_tag_dups
    report["sku_dup_cross_tag_keys"] = cross_tag_dups
    report["ejis_vs_other_sku_keys"] = ejis_vs_other
    report["ejis_vs_other_examples"] = ejis_vs_other_examples
    report["dup_qty_conflict_keys"] = qty_conflict
    report["dup_qty_same_keys"] = qty_same
    report["dup_title_same_keys"] = title_same
    report["dup_title_diff_keys"] = title_diff
    report["dup_price_diff_keys"] = price_diff
    report["ejis_dup_location_on_ejis"] = loc_on_ejis
    report["ejis_dup_location_on_other"] = loc_on_non_ejis
    report["same_tag_dup_examples"] = dup_examples

    # For ejis vs other: qty comparison patterns
    qty_patterns = Counter()
    ejis_qty_sum = 0
    other_qty_sum = 0
    ejis_zero_other_pos = 0
    ejis_pos_other_zero = 0
    both_pos_same = 0
    both_pos_diff = 0
    both_zero = 0
    keep_other_qty_sum = 0  # if we keep non-ejis qty
    keep_max_qty_sum = 0
    keep_sum_qty = 0

    ejis_only_skus = set(df.loc[df["tag"] == "ejis", "sku_key"]) - {""}
    other_skus = set(df.loc[df["tag"] != "ejis", "sku_key"]) - {""}
    report["sku_overlap"] = {
        "ejis_unique_sku": len(ejis_only_skus),
        "non_ejis_unique_sku": len(other_skus),
        "intersection": len(ejis_only_skus & other_skus),
        "ejis_only": len(ejis_only_skus - other_skus),
        "non_ejis_only": len(other_skus - ejis_only_skus),
    }

    for key, g in with_sku.groupby("sku_key"):
        ejis = g[g["tag"] == "ejis"]
        other = g[g["tag"] != "ejis"]
        if ejis.empty or other.empty:
            continue
        eq = int(ejis["qty"].sum())
        oq = int(other["qty"].sum())
        ejis_qty_sum += eq
        other_qty_sum += oq
        keep_other_qty_sum += oq
        keep_max_qty_sum += max(eq, oq)
        keep_sum_qty += eq + oq
        if eq == 0 and oq > 0:
            ejis_zero_other_pos += 1
            qty_patterns["ejis=0, andere>0"] += 1
        elif eq > 0 and oq == 0:
            ejis_pos_other_zero += 1
            qty_patterns["ejis>0, andere=0"] += 1
        elif eq == 0 and oq == 0:
            both_zero += 1
            qty_patterns["beide 0"] += 1
        elif eq == oq:
            both_pos_same += 1
            qty_patterns["beide >0 en gelijk"] += 1
        else:
            both_pos_diff += 1
            qty_patterns[f"beide >0 en verschillend"] += 1

    report["ejis_overlap_qty"] = {
        "patterns": dict(qty_patterns),
        "ejis_qty_sum_in_overlap": ejis_qty_sum,
        "other_qty_sum_in_overlap": other_qty_sum,
        "if_keep_other": keep_other_qty_sum,
        "if_keep_max": keep_max_qty_sum,
        "if_sum_both": keep_sum_qty,
        "ejis_zero_other_pos": ejis_zero_other_pos,
        "ejis_pos_other_zero": ejis_pos_other_zero,
        "both_pos_same": both_pos_same,
        "both_pos_diff": both_pos_diff,
        "both_zero": both_zero,
    }

    # title duplicates (normalized) across tags
    df["title_key"] = df["title"].str.casefold().str.replace(r"\s+", " ", regex=True)
    title_dups = df[df["title_key"] != ""].groupby("title_key").filter(lambda g: len(g) > 1)
    title_combo = Counter()
    title_cross = 0
    for tkey, g in title_dups.groupby("title_key"):
        tags = sorted({x if x else "(leeg)" for x in g["tag"]})
        title_combo[" + ".join(tags)] += 1
        if len(tags) > 1:
            title_cross += 1
    report["title_dups"] = {
        "duplicate_title_keys": int(title_dups["title_key"].nunique()),
        "rows": int(len(title_dups)),
        "cross_tag_keys": title_cross,
        "combos": dict(title_combo.most_common(20)),
    }

    # barcode overlap
    with_bc = df[df["barcode"] != ""].copy()
    bc_dups = with_bc.groupby("barcode").filter(lambda g: len(g) > 1)
    report["barcode"] = {
        "with_barcode": int(len(with_bc)),
        "unique": int(with_bc["barcode"].nunique()),
        "dup_keys": int(bc_dups["barcode"].nunique()) if len(bc_dups) else 0,
        "dup_rows": int(len(bc_dups)),
    }

    # location by tag
    loc_by_tag = {}
    for tag, g in df.groupby("tag"):
        loc_by_tag[tag or "(leeg)"] = int(g["has_location"].sum())
    report["location_by_tag"] = loc_by_tag

    # sample location values for partstream
    ps = df[df["tag"].str.contains("partstream", case=False, na=False)]
    loc_samples = []
    if len(ps):
        for _, r in ps.head(8).iterrows():
            loc_samples.append(
                {
                    "title": r["title"][:70],
                    "sku": r["sku"],
                    "qty": int(r["qty"]),
                    **{c.split("(")[0].strip(): str(r[c])[:30] for c in loc_cols},
                }
            )
    report["partstream_location_samples"] = loc_samples

    # unique location occupancy
    if loc_cols:
        bak = loc_cols[0]
        report["unique_bak"] = int(df[df[bak].str.strip() != ""][bak].nunique()) if bak in df.columns else 0
        filled_any = int(df["has_location"].sum())
        report["rows_with_any_location"] = filled_any

    # empty sku rows
    empty_sku = df[df["sku"] == ""]
    report["empty_sku"] = {
        "rows": int(len(empty_sku)),
        "by_tag": dict(Counter(empty_sku["tag"].replace("", "(leeg)"))),
        "sample_titles": empty_sku["title"].head(10).tolist(),
    }

    # bikes analysis
    bikes = df[df["tag"].str.contains("bikes", case=False, na=False)]
    report["bikes"] = {
        "rows": int(len(bikes)),
        "qty_sum": int(bikes["qty"].sum()) if len(bikes) else 0,
        "vendors": Counter(bikes["vendor"]).most_common(10) if len(bikes) else [],
        "sample": bikes[["title", "sku", "qty", "price"]].head(8).to_dict("records") if len(bikes) else [],
    }

    # stock vs winkelvoorraad stock
    for name in ["stock", "winkelvoorraad", "winkelvoorraad, stock", "Partstream stock", "ejis"]:
        pass

    # SKU format heuristics per tag
    sku_patterns = {}
    for tag, g in df.groupby("tag"):
        samples = g["sku"].head(15).tolist()
        lens = Counter(len(s) for s in g["sku"] if s)
        digits_only = int(g["sku"].str.fullmatch(r"\d+").fillna(False).sum())
        alnum = int(g["sku"].str.fullmatch(r"[A-Za-z0-9\-]+").fillna(False).sum())
        sku_patterns[tag or "(leeg)"] = {
            "digit_only": digits_only,
            "alnum": alnum,
            "len_top": lens.most_common(5),
            "samples": samples,
        }
    report["sku_patterns"] = sku_patterns

    # handle column
    report["handle_nonempty"] = int((df["Handle"].astype(str).str.strip() != "").sum()) if "Handle" in df.columns else 0
    report["body_nonempty"] = int((df["Body (HTML)"].astype(str).str.strip() != "").sum())
    report["image_nonempty"] = int((df["Image Src"].astype(str).str.strip() != "").sum()) if "Image Src" in df.columns else 0

    # status / published overall
    report["status_counts"] = dict(Counter(df["status"]))
    report["published_counts"] = dict(Counter(df["published"]))
    report["vendor_top"] = Counter(df["vendor"]).most_common(15)
    report["type_top"] = Counter(df["ptype"]).most_common(15)

    # What happens if we drop ALL ejis rows that have a matching non-ejis SKU
    drop_mask = (df["tag"] == "ejis") & (df["sku_key"].isin(ejis_only_skus & other_skus))
    remaining = df[~drop_mask]
    report["after_drop_ejis_dups"] = {
        "rows_dropped": int(drop_mask.sum()),
        "rows_remaining": int(len(remaining)),
        "qty_remaining": int(remaining["qty"].sum()),
        "qty_dropped": int(df.loc[drop_mask, "qty"].sum()),
        "remaining_by_tag": dict(Counter(remaining["tag"].replace("", "(leeg)"))),
        "ejis_kept_unique": int(remaining.loc[remaining["tag"] == "ejis", "sku_key"].nunique()),
    }

    # ejis rows that are NOT duplicated - keep these?
    ejis_only_rows = df[(df["tag"] == "ejis") & (~df["sku_key"].isin(other_skus))]
    report["ejis_only_not_in_other"] = {
        "rows": int(len(ejis_only_rows)),
        "qty_sum": int(ejis_only_rows["qty"].sum()),
        "qty_positive": int((ejis_only_rows["qty"] > 0).sum()),
        "qty_zero": int((ejis_only_rows["qty"] == 0).sum()),
        "with_location": int(ejis_only_rows["has_location"].sum()),
        "sample": ejis_only_rows[["title", "sku", "qty", "price"]].head(12).to_dict("records"),
    }

    # qty distribution by tag
    qty_bins = {}
    for tag, g in df.groupby("tag"):
        qty_bins[tag or "(leeg)"] = {
            "0": int((g["qty"] == 0).sum()),
            "1": int((g["qty"] == 1).sum()),
            "2-5": int(((g["qty"] >= 2) & (g["qty"] <= 5)).sum()),
            "6-20": int(((g["qty"] >= 6) & (g["qty"] <= 20)).sum()),
            "21+": int((g["qty"] >= 21).sum()),
        }
    report["qty_bins"] = qty_bins

    # both_pos_diff examples
    both_diff_ex = []
    for key, g in with_sku.groupby("sku_key"):
        ejis = g[g["tag"] == "ejis"]
        other = g[g["tag"] != "ejis"]
        if ejis.empty or other.empty:
            continue
        eq = int(ejis["qty"].sum())
        oq = int(other["qty"].sum())
        if eq > 0 and oq > 0 and eq != oq and len(both_diff_ex) < 15:
            both_diff_ex.append(
                {
                    "sku": g["sku"].iloc[0],
                    "ejis_qty": eq,
                    "other_qty": oq,
                    "other_tags": sorted({t or "(leeg)" for t in other["tag"]}),
                    "ejis_title": ejis["title"].iloc[0][:70],
                    "other_title": other["title"].iloc[0][:70],
                }
            )
    report["both_pos_diff_examples"] = both_diff_ex

    # ejis>0 other=0 examples (would lose stock if we drop ejis)
    lose_stock = []
    for key, g in with_sku.groupby("sku_key"):
        ejis = g[g["tag"] == "ejis"]
        other = g[g["tag"] != "ejis"]
        if ejis.empty or other.empty:
            continue
        eq = int(ejis["qty"].sum())
        oq = int(other["qty"].sum())
        if eq > 0 and oq == 0 and len(lose_stock) < 12:
            lose_stock.append(
                {
                    "sku": g["sku"].iloc[0],
                    "ejis_qty": eq,
                    "other_tags": sorted({t or "(leeg)" for t in other["tag"]}),
                    "ejis_title": ejis["title"].iloc[0][:70],
                    "other_title": other["title"].iloc[0][:70],
                }
            )
    report["ejis_pos_other_zero_examples"] = lose_stock

    # Check if "Winkelvoorraad, stock" is two tags or one
    report["raw_tag_repr"] = {repr(k): v for k, v in tag_counts.most_common()}

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False, default=str))
    print(f"Wrote {OUT} rows={n}")
    print("TAGS", dict(tag_counts.most_common()))
    print("SKU overlap", report["sku_overlap"])
    print("ejis vs other keys", ejis_vs_other)
    print("after drop", report["after_drop_ejis_dups"])


if __name__ == "__main__":
    main()
