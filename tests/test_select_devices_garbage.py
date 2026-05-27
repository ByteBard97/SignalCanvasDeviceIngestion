"""Tests for garbage-device pre-filter in select_devices."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.select_devices import _is_garbage_model, select  # noqa: E402


# ---------------------------------------------------------------------------
# _is_garbage_model — garbage cases
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "model",
    [
        "USB C",
        "usb-c",
        "USB C ",
        "RJ45",
        "HDMI",
        "SDI",
        "",
        "  ",
        "Analog",
        "Dante",
    ],
)
def test_is_garbage_model_true(model: str) -> None:
    assert _is_garbage_model(model) is True


# ---------------------------------------------------------------------------
# _is_garbage_model — real product names
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "model",
    [
        "AVIO USB-C",
        "Rio1608-D2",
        "476P",
        "ULXD4",
        "DI RADIAL Pro AV",
        "Dante AVIO USB",
    ],
)
def test_is_garbage_model_false(model: str) -> None:
    assert _is_garbage_model(model) is False


# ---------------------------------------------------------------------------
# select() drops garbage, keeps real
# ---------------------------------------------------------------------------
def test_select_drops_garbage_keeps_real() -> None:
    devices = [
        {
            "device_id": "audinate-usb-c",
            "manufacturer": "Audinate",
            "model": "USB C",
            "mfg_slug": "audinate",
            "priority_score": 0,
        },
        {
            "device_id": "audinate-avio-usb-c",
            "manufacturer": "Audinate",
            "model": "AVIO USB-C",
            "mfg_slug": "audinate",
            "priority_score": 0,
        },
    ]
    result = select(devices, excluded=set(), processed=set(), n=10, max_per_mfg=None, seed=42)
    ids = {d["device_id"] for d in result}
    assert "audinate-usb-c" not in ids
    assert "audinate-avio-usb-c" in ids
