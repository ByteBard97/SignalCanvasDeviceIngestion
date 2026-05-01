"""Tests for the two-tier device classifier."""

from __future__ import annotations

import pytest

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from stages.classify_device import _classify_by_rule, classify, Classification  # noqa: E402


# ---------------------------------------------------------------------------
# Rule-based positive tests
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "manufacturer,model,expected_class",
    [
        ("Yamaha", "Rio1608-D2", "dante_stagebox"),
        ("Audinate", "AVIO-AO2", "dante_adapter_output"),
        ("Audinate", "AVIO-AI2", "dante_adapter_input"),
        ("Shure", "ULXD4", "wireless_rx"),
        ("Sennheiser", "EW-DX-EM", "wireless_rx"),
        ("Allen & Heath", "SQ-5", "mixing_console"),
        ("QSC", "Core 110f", "dsp_processor"),
    ],
)
def test_classify_by_rule_positive(manufacturer: str, model: str, expected_class: str) -> None:
    """Rule table must correctly map known devices."""
    result = _classify_by_rule(manufacturer, model)
    assert result is not None
    assert result.class_ == expected_class
    assert result.source == "rule"
    assert result.confidence == 1.0


def test_classify_by_rule_unknown() -> None:
    """Unknown devices must return None so the LLM tier is invoked."""
    result = _classify_by_rule("UnknownCorp", "XYZ-999")
    assert result is None


# ---------------------------------------------------------------------------
# Public API: rule hit (no LLM call)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_classify_rule_hit() -> None:
    """Public classify() must return rule result without calling LLM."""
    result = await classify("Yamaha", "Rio1608-D2")
    assert result.class_ == "dante_stagebox"
    assert result.source == "rule"


@pytest.mark.asyncio
async def test_classify_unknown_no_llm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unknown devices should fall through to generic without an actual LLM call."""
    calls: list[tuple[str, str]] = []

    async def _fake_llm(manufacturer: str, model: str, markdown_excerpt=None, moonshot=None, **kwargs) -> Classification:
        calls.append((manufacturer, model))
        return Classification(class_="generic", confidence=0.6, source="llm")

    monkeypatch.setattr(
        "stages.classify_device._classify_by_llm",
        _fake_llm,
    )

    result = await classify("MysteryCorp", "M-100")
    assert result.class_ == "generic"
    assert result.source == "llm"
    assert len(calls) == 1
    assert calls[0] == ("MysteryCorp", "M-100")


# ---------------------------------------------------------------------------
# Case-insensitivity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "manufacturer,model,expected_class",
    [
        ("yamaha", "rio1608-d2", "dante_stagebox"),
        ("AUDINATE", "avio-ao2", "dante_adapter_output"),
        ("Shure", "ulxd4", "wireless_rx"),
    ],
)
def test_classify_case_insensitive(manufacturer: str, model: str, expected_class: str) -> None:
    """Rules must be case-insensitive."""
    result = _classify_by_rule(manufacturer, model)
    assert result is not None
    assert result.class_ == expected_class
