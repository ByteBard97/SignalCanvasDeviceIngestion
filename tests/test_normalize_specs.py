"""Tests for the Stage 5 post-processing normalization layer."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from stages.normalize_specs import (
    _canonicalize_name,
    _has_port_category,
    _infer_connector,
    _merge_ports,
    _normalize_bridge,
    _parse_channels,
    _placeholder_port,
    _standardize_direction,
    normalize_extraction,
)

# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Dante Primary", "Dante"),
        ("dante secondary", "Dante"),
        ("RJ45 Dante", "Dante"),
        ("Local Mic Inputs", "Analog Input"),
        ("Mic/Line Inputs", "Analog Input"),
        ("Analog Input", "Analog Input"),
        ("Main Outputs", "Main Output"),
        ("XLR Outputs", "Main Output"),
        ("Line Outputs", "Main Output"),
        ("Headphone out", "Headphone"),
        ("Monitor out", "Headphone"),
        ("Talkback mic input", "Talkback"),
        ("Dedicated Talkback mic input", "Talkback"),
        ("Ethernet Port", "Network"),
        ("Control Network", "Network"),
        ("USB Audio", "USB"),
        ("USB-B", "USB"),
        ("SLink EtherCON", "SLink"),
        ("Expansion Port", "SLink"),
        ("RF Antenna Diversity Input", "RF Antenna"),
        ("SMA Antenna", "RF Antenna"),
        ("Power Supply Jack", "Power"),
        ("Unknown Port", "Unknown Port"),
    ],
)
def test_canonicalize_name(name: str, expected: str) -> None:
    assert _canonicalize_name(name) == expected


@pytest.mark.parametrize(
    "direction,name,expected",
    [
        (None, "Analog Input", "input"),
        (None, "Main Output", "output"),
        (None, "Flex Channels", "input_output"),
        (None, "Digital I/O", "input_output"),
        ("input", "X", "input"),
        ("in", "X", "input"),
        ("output", "X", "output"),
        ("out", "X", "output"),
        ("input/output", "X", "input_output"),
        ("i/o", "X", "input_output"),
        ("bidirectional", "X", "bidirectional"),
    ],
)
def test_standardize_direction(direction: str | None, name: str, expected: str) -> None:
    assert _standardize_direction(direction, name) == expected


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Balanced XLR Audio Output", "XLR"),
        ('1/4" TRS Input', "TRS"),
        ("6.35mm jack", "TRS"),
        ("Dante Primary", "RJ45"),
        ("USB Audio", "USB-B"),
        ("SMA Antenna", "SMA"),
        ("BNC RF Input", "BNC"),
        ("LEMO Power", "LEMO"),
        ("HDMI Output", "HDMI"),
        ("RCA Cable", "RCA"),
        ("Euroblock Connector", "Euroblock"),
        ("Phoenix Terminal", "Euroblock"),
        ("Plain Name", None),
    ],
)
def test_infer_connector(name: str, expected: str | None) -> None:
    assert _infer_connector(name) == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (16, 16),
        ("16", 16),
        ("16ch", 16),
        ("16ch (Rio to other)", 16),
        ("2 XLR-M", 2),
        (None, None),
        ("", None),
        ("N/A", None),
    ],
)
def test_parse_channels(value: Any, expected: int | None) -> None:
    assert _parse_channels(value) == expected


@pytest.mark.parametrize(
    "bridge,expected",
    [
        ("A -> B", "A->B"),
        ("A  ->  B", "A->B"),
        ("A=>B", "A->B"),
        ("A → B", "A->B"),
        ("A->A", None),
        ("Same->Same", None),
    ],
)
def test_normalize_bridge(bridge: str, expected: str | None) -> None:
    assert _normalize_bridge(bridge) == expected


def test_merge_ports_sums_channels() -> None:
    a = {
        "name": "Analog Input",
        "direction": "input",
        "connector": "XLR",
        "channels": 8,
        "attributes": ["balanced"],
    }
    b = {
        "name": "Analog Input",
        "direction": "input",
        "connector": "XLR",
        "channels": 8,
        "attributes": ["phantom"],
    }
    merged = _merge_ports(a, b)
    assert merged["channels"] == 16
    assert sorted(merged["attributes"]) == ["balanced", "phantom"]


def test_merge_ports_one_null_channel() -> None:
    a = {
        "name": "Analog Input",
        "direction": "input",
        "connector": "XLR",
        "channels": 8,
        "attributes": None,
    }
    b = {
        "name": "Analog Input",
        "direction": "input",
        "connector": "XLR",
        "channels": None,
        "attributes": None,
    }
    merged = _merge_ports(a, b)
    assert merged["channels"] == 8


def test_has_port_category() -> None:
    ports = [
        {"name": "Dante", "connector": "RJ45", "attributes": []},
    ]
    assert _has_port_category(ports, "dante_port")
    assert not _has_port_category(ports, "analog_port")


def test_placeholder_port() -> None:
    ph = _placeholder_port("dante_port")
    assert ph["_placeholder"] is True
    assert ph["_confidence"] == "low"
    assert ph["name"] == "Dante"


# ---------------------------------------------------------------------------
# Integration tests against real extractions
# ---------------------------------------------------------------------------

_STAGE5_DIR = Path(__file__).resolve().parent.parent / "output" / "stage_5_agentic"


def _load_all_moonshot() -> list[tuple[str, dict]]:
    files = sorted(_STAGE5_DIR.glob("*.moonshot.json"))
    results: list[tuple[str, dict]] = []
    for p in files:
        with open(p, encoding="utf-8") as fh:
            results.append((p.name, json.load(fh)))
    return results


@pytest.mark.parametrize("fname,data", _load_all_moonshot())
def test_normalize_extraction_runs_without_error(fname: str, data: dict) -> None:
    """Every real extraction file must survive normalization."""
    # Infer a plausible device_class from the filename for the test
    class_map: dict[str, str] = {
        "allen-heath-sq5": "mixing_console",
        "audinate-avio-ai2": "dante_adapter_input",
        "audinate-avio-ao2": "dante_adapter_output",
        "qsc-core-110f": "dsp_processor",
        "shure-ulxd4": "wireless_rx",
        "yamaha-rio1608-d2": "dante_stagebox",
    }
    stem = fname.replace(".moonshot.json", "")
    device_class = class_map.get(stem, "generic")
    result = normalize_extraction(data, device_class)

    assert "signal_flow" in result
    ports = result["signal_flow"]["ports"]
    bridges = result["signal_flow"]["bridges"]

    # Verify determinism: same input → same output
    result2 = normalize_extraction(data, device_class)
    assert result == result2

    # Bridges must be sorted and deduped
    assert bridges == sorted(set(bridges))

    # No self-loops
    for b in bridges:
        src, dst = b.split("->", 1)
        assert src.strip() != dst.strip(), f"Self-loop found: {b}"

    # Ports sorted by (direction, name)
    keys = [(p.get("direction") or "", p.get("name") or "") for p in ports]
    assert keys == sorted(keys)


# ---------------------------------------------------------------------------
# Behavioural assertions on specific devices
# ---------------------------------------------------------------------------


def test_shure_ulxd4_antenna_placeholder() -> None:
    path = _STAGE5_DIR / "shure-ulxd4.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    result = normalize_extraction(data, "wireless_rx")
    ports = result["signal_flow"]["ports"]
    names = [p["name"] for p in ports]
    assert "RF Antenna" in names
    # RF Antenna should have been canonicalized from "RF Antenna Diversity Input Jack (2)"
    rf_ports = [p for p in ports if p["name"] == "RF Antenna"]
    assert len(rf_ports) == 1
    assert rf_ports[0]["connector"] == "BNC"


def test_yamaha_rio_dante_canonicalized() -> None:
    path = _STAGE5_DIR / "yamaha-rio1608-d2.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    result = normalize_extraction(data, "dante_stagebox")
    ports = result["signal_flow"]["ports"]
    dante_ports = [p for p in ports if p["name"] == "Dante"]
    assert len(dante_ports) == 1
    # Fixture currently has RJ45; enrichment only backfills null connectors
    assert dante_ports[0]["connector"] == "RJ45"
    attrs = dante_ports[0].get("attributes") or []
    assert "etherCON" in attrs
    assert "Cat5e" in attrs


def test_qsc_core_network_placeholder() -> None:
    path = _STAGE5_DIR / "qsc-core-110f.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    result = normalize_extraction(data, "dsp_processor")
    ports = result["signal_flow"]["ports"]
    # QSC already has a Network port, so no placeholder should be added
    placeholders = [p for p in ports if p.get("_placeholder")]
    assert not placeholders


def test_allen_heath_sq5_headphone_canonicalized() -> None:
    path = _STAGE5_DIR / "allen-heath-sq5.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    result = normalize_extraction(data, "mixing_console")
    ports = result["signal_flow"]["ports"]
    names = [p["name"] for p in ports]
    assert "Headphone" in names
    assert "Talkback" in names
    assert "SLink" in names


def test_audinate_avio_ao2_analog_placeholder() -> None:
    path = _STAGE5_DIR / "audinate-avio-ao2.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    result = normalize_extraction(data, "dante_adapter_output")
    ports = result["signal_flow"]["ports"]
    # AVIO-AO2 already has an analog output port
    placeholders = [p for p in ports if p.get("_placeholder")]
    assert not placeholders


def test_required_port_enforcement_adds_placeholder() -> None:
    """If a required category is missing, a placeholder must appear."""
    extracted = {
        "signal_flow": {
            "ports": [
                {
                    "name": "Dante",
                    "direction": "input_output",
                    "connector": "RJ45",
                    "channels": None,
                    "attributes": [],
                }
            ],
            "bridges": [],
        }
    }
    result = normalize_extraction(extracted, "wireless_rx")
    ports = result["signal_flow"]["ports"]
    placeholders = [p for p in ports if p.get("_placeholder")]
    assert len(placeholders) == 2  # analog_port + antenna_port
