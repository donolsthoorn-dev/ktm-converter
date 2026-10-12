from modules.ebihr.discontinued import select_fully_discontinued


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
