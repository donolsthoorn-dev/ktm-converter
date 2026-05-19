"""Upload-metadata: extensie/MIME afstemmen op bestandsinhoud."""

from __future__ import annotations

from pathlib import Path

import modules.image_manager as image_manager


def test_detect_jpeg_with_png_extension(tmp_path: Path) -> None:
    p = tmp_path / "pho_wp_nmon_t1240s__sall__awsg__v1.png"
    p.write_bytes(b"\xff\xd8\xff\xe0" + b"\x00" * 20)
    name, mime = image_manager.detect_image_upload_metadata(p)
    assert name == "pho_wp_nmon_t1240s__sall__awsg__v1.jpg"
    assert mime == "image/jpeg"


def test_detect_real_png_unchanged(tmp_path: Path) -> None:
    p = tmp_path / "real.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\n" + b"\x00" * 20)
    name, mime = image_manager.detect_image_upload_metadata(p)
    assert name == "real.png"
    assert mime == "image/png"
