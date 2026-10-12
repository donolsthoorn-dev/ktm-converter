from modules.ebihr.discontinued import (
    catalog_part_status,
    select_fully_discontinued,
    select_products_without_bihr_offer,
    variants_to_block,
)


def test_only_fully_discontinued_active_products_are_selected() -> None:
    discontinued = {"1019810", "8003403"}
    sku_to_pid = {
        "1019810": "1",
        "8003403": "2",
        "8003403001": "2",
        "999": "3",
    }
    index = {
        "1019810": {"id": "1", "title": "Zuigerveerset", "status": "ACTIVE"},
        "8003403": {"id": "2", "title": "Helm", "status": "ACTIVE"},
        "999": {"id": "3", "title": "Al draft", "status": "DRAFT"},
        "live": {"id": "4", "title": "Nog leverbaar", "status": "ACTIVE"},
    }
    sku_to_pid["LIVE1"] = "4"
    chosen = select_fully_discontinued(discontinued, sku_to_pid, index)
    assert [item["id"] for item in chosen] == ["1"]


def _helmet_shop():
    sku_to_pid = {
        "8009205001": "9",
        "8009205003": "9",
        "8009205004": "9",
        "KTM1": "8",
    }
    index = {
        "8009205": {"id": "9", "title": "BELL Custom 500", "status": "ACTIVE"},
        "ktm-filter": {"id": "8", "title": "KTM filter", "status": "ACTIVE"},
    }
    sku_to_vid = {sku: f"v{sku}" for sku in sku_to_pid}
    return sku_to_pid, index, sku_to_vid


def test_dropped_sizes_draft_product_when_nothing_remains_offered() -> None:
    sku_to_pid, index, _vids = _helmet_shop()
    chosen = select_products_without_bihr_offer(
        {"8009205003"},
        {"8009205003"},
        sku_to_pid,
        index,
    )
    assert [item["id"] for item in chosen] == ["9"]


def test_one_offered_size_keeps_the_product_active() -> None:
    sku_to_pid, index, _vids = _helmet_shop()
    sku_to_pid["8009205002"] = "9"
    chosen = select_products_without_bihr_offer(
        {"8009205002", "8009205003"},
        {"8009205003"},
        sku_to_pid,
        index,
    )
    assert chosen == []


def test_temporary_stockout_does_not_draft() -> None:
    sku_to_pid, index, _vids = _helmet_shop()
    chosen = select_products_without_bihr_offer(
        {"8009205001", "8009205003", "8009205004"},
        set(),
        sku_to_pid,
        index,
    )
    assert chosen == []


def test_unrelated_shop_product_is_left_alone() -> None:
    sku_to_pid, index, _vids = _helmet_shop()
    chosen = select_products_without_bihr_offer(set(), set(), sku_to_pid, index)
    assert chosen == []


def test_dropped_and_discontinued_variants_are_blocked() -> None:
    sku_to_pid, _index, sku_to_vid = _helmet_shop()
    sku_to_pid["8009205002"] = "9"
    sku_to_vid["8009205002"] = "v8009205002"
    blocked, already = variants_to_block(
        {"8009205002", "8009205003"},
        {"8009205003"},
        {"8009205002"},
        sku_to_pid,
        sku_to_vid,
        {"8009205004": "CONTINUE", "8009205003": "DENY"},
        respect_current=True,
    )
    assert already == 1
    assert blocked == {"9": [("v8009205001", "DENY"), ("v8009205004", "DENY")]}


def test_stock_row_keeps_ownership_of_that_variant() -> None:
    sku_to_pid, _index, sku_to_vid = _helmet_shop()
    blocked, already = variants_to_block(
        {"8009205003"},
        {"8009205003"},
        {"8009205003"},
        sku_to_pid,
        sku_to_vid,
        {},
        respect_current=True,
    )
    vids = {vid for _pid, pairs in blocked.items() for vid, _pol in pairs}
    assert "v8009205003" not in vids
    assert vids == {"v8009205001", "v8009205004"}
    assert already == 0


def test_catalog_file_reads_part_numbers_and_discontinued_flag(tmp_path) -> None:
    (tmp_path / "products.json").write_bytes(
        b"""{"Products":[
          {"PartNumber":"8009205001","IsDiscontinued":false},
          {"partNumber":"8009205003","isDiscontinued":true},
          {"PartNumber":"8009205004"}
        ]}"""
    )
    (tmp_path / "Prices.json").write_bytes(b'{"PartNumber":"999"}')
    catalog, discontinued = catalog_part_status(tmp_path)
    assert catalog == {"8009205001", "8009205003", "8009205004"}
    assert discontinued == {"8009205003"}
