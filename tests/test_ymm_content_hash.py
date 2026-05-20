from modules.ymm_content_hash import needs_shopify_push, ymm_json_content_hash


def test_hash_stable() -> None:
    a = {"KTM": {"125 SX": ["2020"]}}
    b = {"KTM": {"125 SX": ["2020"]}}
    assert ymm_json_content_hash(a) == ymm_json_content_hash(b)


def test_needs_push() -> None:
    h = ymm_json_content_hash({"KTM": {"X": ["2020"]}})
    assert needs_shopify_push(h, None)
    assert not needs_shopify_push(h, h)
    assert needs_shopify_push(h, "other")
