"""Galerijplan: XML-bestandsnamen vervangen oude Shopify-foto's."""

from modules.shopify_export_images_lib import (
    expected_norm_for_live,
    gallery_needs_reorder,
    gallery_replace_is_safe,
    plan_product_gallery,
)

SKU = "pho_et_nmon_54830097100__sall__awsg__v1.jpg"
SYMBOL = "pho_et_nmon_symbolbilddichtsatz__sall__awsg__v1.jpg"


def _url(name: str) -> str:
    return f"https://cdn.shopify.com/s/files/1/1/1/files/{name}"


def test_keeps_current_xml_photo_and_drops_older_names():
    plan = plan_product_gallery(
        [_url(SKU)],
        [
            {
                "id": "1",
                "src": _url(
                    "PHO-ET-NMON-SymbolbildDichtsatz-SALL-AWSG-V1_2_29b7bfe7-0289-483f-923a-f1a471f08144.jpg"
                ),
                "norm": "pho-et-nmon-symbolbilddichtsatz-sall-awsg-v1_2.jpg",
            },
            {
                "id": "2",
                "src": _url(f"{SYMBOL.replace('.jpg', '')}_6d4caa17-9b73-4a6f-9133-f19b39e3983c.jpg"),
                "norm": SYMBOL,
            },
            {
                "id": "3",
                "src": _url(f"{SKU.replace('.jpg', '')}_f3be5289-504e-4af2-873d-6b7e67c42637.jpg"),
                "norm": SKU,
            },
        ],
    )
    assert plan["missing_urls"] == []
    assert plan["delete_ids"] == ["1", "2"]
    assert plan["keep_id_by_norm"] == {SKU: "3"}
    assert gallery_replace_is_safe(plan, {})


def test_empty_csv_does_not_delete():
    plan = plan_product_gallery([], [{"id": "9", "src": _url(SKU), "norm": SKU}])
    assert plan["delete_ids"] == []
    assert plan["missing_urls"] == []
    assert not gallery_replace_is_safe(plan, {})


def test_does_not_delete_until_replacement_is_on_the_product():
    plan = plan_product_gallery(
        [_url(SKU)],
        [{"id": "2", "src": _url(SYMBOL), "norm": SYMBOL}],
    )
    assert plan["missing_urls"] == [_url(SKU)]
    assert plan["delete_ids"] == ["2"]
    assert not gallery_replace_is_safe(plan, {})
    assert gallery_replace_is_safe(plan, {SKU: "99"})


def test_shopify_extra_version_suffix_is_the_same_photo():
    expected = {SKU}
    doubled = SKU.replace(".jpg", "__v1.jpg")
    assert expected_norm_for_live(doubled, expected) == SKU
    assert expected_norm_for_live(SKU.replace("__v1.jpg", "__v2.jpg"), expected) is None

    plan = plan_product_gallery(
        [_url(SKU)],
        [{"id": "7", "src": _url(doubled), "norm": doubled}],
    )
    assert plan["missing_urls"] == []
    assert plan["delete_ids"] == []
    assert plan["keep_id_by_norm"] == {SKU: "7"}


def test_reorders_when_xml_photo_is_not_first():
    plan = plan_product_gallery(
        [_url(SKU), _url(SYMBOL)],
        [
            {"id": "2", "src": _url(SYMBOL), "norm": SYMBOL},
            {"id": "3", "src": _url(SKU), "norm": SKU},
        ],
    )
    assert plan["delete_ids"] == []
    assert gallery_needs_reorder(plan, {})


def test_no_reorder_when_deleting_leaves_the_xml_photo_alone():
    plan = plan_product_gallery(
        [_url(SKU)],
        [
            {"id": "2", "src": _url(SYMBOL), "norm": SYMBOL},
            {"id": "3", "src": _url(SKU), "norm": SKU},
        ],
    )
    assert not gallery_needs_reorder(plan, {})
