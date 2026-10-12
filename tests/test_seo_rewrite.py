from modules.seo_completeness import description_needs_rewrite, title_needs_rewrite


def test_truncated_title_is_rewritten() -> None:
    assert title_needs_rewrite("Fly screen kit", "Fly screen kit 790 Duke 2027", "KTM")


def test_full_branded_title_stays() -> None:
    assert not title_needs_rewrite(
        "KTM Fly screen kit 790 Duke 2027",
        "Fly screen kit 790 Duke 2027",
        "KTM",
    )


def test_empty_title_is_rewritten() -> None:
    assert title_needs_rewrite("", "Stand plate", "KTM")


def test_english_import_description_is_rewritten() -> None:
    raw = (
        "Extremely light Made of high-quality carbon Highest quality workmanship "
        "Mounting material included in scope of supply"
    )
    assert description_needs_rewrite(raw, "Fly screen kit 790 Duke 2027")


def test_long_import_description_is_rewritten() -> None:
    assert description_needs_rewrite("woord " * 40, "Titel")


def test_dutch_sentence_stays() -> None:
    text = (
        "Veilige stand voor uw motor, speciaal voor zachte ondergronden zoals zand. "
        "Gemaakt van aluminium en staal."
    )
    assert not description_needs_rewrite(text, "Stand plate")


def test_title_echo_is_rewritten() -> None:
    assert description_needs_rewrite(
        "Factory start device 50-65 (SX) 02-20.",
        "Factory start device 50-65 (SX) 02-20",
    )
