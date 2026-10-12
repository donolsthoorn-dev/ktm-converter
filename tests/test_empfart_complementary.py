"""Unit tests for EMPF/MUSS/ET relation mapping (no Shopify API)."""

from __future__ import annotations

from modules.empfart_complementary import (
    OwnerRelations,
    build_sync_targets,
    metafield_list_value,
    product_gid,
    resolve_skus_to_product_gids,
    symmetrize_complementary_cliques,
)


def test_complementary_skus_muss_before_empf():
    rel = OwnerRelations(muss=["B", "A"], empf=["A", "C", "D"])
    assert rel.complementary_skus() == ["B", "A", "C", "D"]


def test_resolve_skus_dedupe_cap_and_exclude():
    sku_map = {"A": "1", "B": "2", "C": "2", "D": "3", "E": "4"}
    gids, unresolved = resolve_skus_to_product_gids(
        ["A", "B", "C", "MISSING", "D", "E"],
        sku_map,
        max_products=2,
        exclude_product_ids={"1"},
    )
    assert gids == [product_gid("2"), product_gid("3")]
    assert unresolved == 1


def test_build_sync_targets_pilot_shape():
    relations = {
        "OWNER": OwnerRelations(muss=["M1"], empf=["E1", "E2"], et=["T1"]),
        "OTHER": OwnerRelations(empf=["E9"]),
    }
    sku_map = {
        "OWNER": "100",
        "M1": "201",
        "E1": "202",
        "E2": "202",  # same product as E1
        "T1": "301",
        "OTHER": "400",
        "E9": "401",
    }
    targets = build_sync_targets(relations, sku_map, max_products=10, only_skus={"OWNER"})
    assert set(targets) == {"OWNER"}
    t = targets["OWNER"]
    assert t["owner_product_id"] == "100"
    assert t["complementary_gids"] == [product_gid("201"), product_gid("202")]
    assert t["et_gids"] == [product_gid("301")]
    assert metafield_list_value(t["complementary_gids"]) == (
        '["gid://shopify/Product/201","gid://shopify/Product/202"]'
    )


def test_symmetrize_complementary_clique_brake_set():
    """A→{B,C}, B→{A}, C→{A} → each lists the other two."""
    relations = {
        "A": OwnerRelations(empf=["B", "C"]),
        "B": OwnerRelations(empf=["A"]),
        "C": OwnerRelations(empf=["A"]),
    }
    out = symmetrize_complementary_cliques(relations)
    assert out["A"].complementary_skus() == ["B", "C"]
    assert out["B"].complementary_skus() == ["A", "C"]
    assert out["C"].complementary_skus() == ["A", "B"]


def test_symmetrize_does_not_merge_via_shared_neighbor():
    """A→{B,C} and D→{C,E} must not put E on A."""
    relations = {
        "A": OwnerRelations(empf=["B", "C"]),
        "B": OwnerRelations(empf=["A"]),
        "C": OwnerRelations(empf=["A"]),
        "D": OwnerRelations(empf=["C", "E"]),
        "E": OwnerRelations(empf=["D"]),
    }
    out = symmetrize_complementary_cliques(relations)
    assert out["A"].complementary_skus() == ["B", "C"]
    assert set(out["C"].complementary_skus()) == {"A", "B", "D", "E"}
    assert "E" not in out["A"].complementary_skus()
    assert "A" not in out["E"].complementary_skus()
    assert set(out["D"].complementary_skus()) == {"C", "E"}
