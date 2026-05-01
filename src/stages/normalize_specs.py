"""Post-processing normalization for Stage 5 LLM extractions.

Canonicalizes port names, merges equivalent ports, dedupes bridges,
infers missing fields, enforces required categories, and sorts
outputs deterministically.
"""

from __future__ import annotations

import copy
import re
from typing import Any

# ---------------------------------------------------------------------------
# Canonicalization tables
# ---------------------------------------------------------------------------

# Each entry: (canonical_name, [matching_substrings...])
# Matching is case-insensitive and performed on the original port name.
_NAME_RULES: list[tuple[str, list[str]]] = [
    (
        "Dante",
        [
            "dante primary",
            "dante secondary",
            "dante primary/secondary",
            "dante network",
            "rj45 dante",
            "dante output",
            "dante input",
        ],
    ),
    ("Analog Input", ["mic/line inputs", "local mic inputs", "analog inputs", "analog input"]),
    (
        "Main Output",
        [
            "main outputs",
            "xlr outputs",
            "local xlr outputs",
            "line outputs",
            "analog outputs",
            "analog output",
        ],
    ),
    ("Headphone", ["headphone out", "headphone output", "monitor out"]),
    (
        "Talkback",
        [
            "talkback mic input",
            "talkback input",
            "talkback microphone",
            "dedicated talkback mic input",
        ],
    ),
    ("Network", ["ethernet", "network", "lan", "control network", "gigabit ethernet"]),
    ("USB", ["usb audio", "usb-b", "usb streaming"]),
    ("SLink", ["slink", "slink ethercon", "expansion port"]),
    ("RF Antenna", ["rf antenna", "antenna diversity", "rf input", "sma antenna"]),
    ("Power", ["power supply", "dc power", "lemo power"]),
]

_REQUIRED_CATEGORIES: dict[str, list[str]] = {
    "dante_stagebox": ["dante_port"],
    "dante_adapter_input": ["dante_port", "analog_port"],
    "dante_adapter_output": ["dante_port", "analog_port"],
    "wireless_rx": ["analog_port", "antenna_port"],
    "mixing_console": ["analog_port"],
    "dsp_processor": ["analog_port", "network_port"],
}

_BRIDGE_HEURISTICS: dict[str, list[tuple[str, str]]] = {
    "dante_stagebox": [
        ("Analog Input", "Dante"),
        ("Dante", "Main Output"),
    ],
    "dante_adapter_input": [
        ("Analog Input", "Dante"),
        ("Analog Input", "Network"),
    ],
    "dante_adapter_output": [
        ("Dante", "Main Output"),
        ("Network", "Main Output"),
    ],
    "wireless_rx": [
        ("RF Antenna", "Main Output"),
        ("RF Antenna", "Output"),
    ],
    "mixing_console": [],
    "dsp_processor": [],
}

_CONNECTOR_INFERENCES: list[tuple[str, str]] = [
    ("XLR", r"\bXLR\b"),
    ("TRS", r"\b(TRS|1/4[\s\"']*\s*\(?6\.35\s*mm\)?|6\.35\s*mm)\b"),
    ("RJ45", r"\b(RJ45|EtherCON|Dante)\b"),
    ("USB-B", r"\bUSB\b"),
    ("SMA", r"\bSMA\b"),
    ("BNC", r"\bBNC\b"),
    ("LEMO", r"\bLEMO\b"),
    ("HDMI", r"\bHDMI\b"),
    ("RCA", r"\bRCA\b"),
    ("Euroblock", r"\b(Euroblock|Phoenix)\b"),
]

_CONNECTOR_OVERRIDES: dict[str, dict[str, str]] = {
    "QSC:Core 110f": {
        "Analog Audio Inputs": "Euroblock",
        "Analog Audio Outputs": "Euroblock",
        "FlexChannels": "Euroblock",
    },
    "Audinate:AVIO-AO2": {
        "Analog Output": "XLR-M",
    },
    "Audinate:AVIO-AI2": {
        "Analog Input": "XLR-F",
    },
}

_DIRECTION_OVERRIDES: dict[str, dict[str, str]] = {
    "Audinate:AVIO-AO2": {
        "Analog Output": "output",
    },
    "Audinate:AVIO-AI2": {
        "Analog Input": "input",
    },
}

_PLACEHOLDER_SPECS: dict[str, dict] = {
    "dante_port": {
        "name": "Dante",
        "direction": "input_output",
        "connector": "RJ45",
        "channels": None,
        "attributes": ["placeholder"],
    },
    "analog_port": {
        "name": "Analog Input",
        "direction": "input",
        "connector": "XLR",
        "channels": None,
        "attributes": ["placeholder"],
    },
    "digital_port": {
        "name": "Digital I/O",
        "direction": "input_output",
        "connector": None,
        "channels": None,
        "attributes": ["placeholder"],
    },
    "antenna_port": {
        "name": "RF Antenna",
        "direction": "input",
        "connector": "BNC",
        "channels": None,
        "attributes": ["placeholder"],
    },
    "network_port": {
        "name": "Network",
        "direction": "input_output",
        "connector": "RJ45",
        "channels": None,
        "attributes": ["placeholder"],
    },
    "usb_port": {
        "name": "USB",
        "direction": "input_output",
        "connector": "USB-B",
        "channels": None,
        "attributes": ["placeholder"],
    },
    "control_port": {
        "name": "Control",
        "direction": "input_output",
        "connector": None,
        "channels": None,
        "attributes": ["placeholder"],
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _canonicalize_name(name: str) -> str:
    """Map semantically-equivalent port names to a canonical form."""
    lowered = name.lower()
    for canonical, patterns in _NAME_RULES:
        for pat in patterns:
            # Use word-boundary checks so short tokens like "lan" don't match
            # inside "balanced".
            regex = r"(?<![a-z])" + re.escape(pat) + r"(?![a-z])"
            if re.search(regex, lowered):
                return canonical
    return name


def _standardize_direction(direction: str | None, name: str) -> str | None:
    """Standardize direction strings and infer from name if missing."""
    if direction:
        d = direction.strip().lower()
        if d in ("input", "in"):
            return "input"
        if d in ("output", "out"):
            return "output"
        if d in ("input_output", "input/output", "i/o"):
            return "input_output"
        if d == "bidirectional":
            return "bidirectional"
        return d

    lowered = name.lower()
    if "input" in lowered:
        return "input"
    if "output" in lowered:
        return "output"
    if "i/o" in lowered or "flex" in lowered:
        return "input_output"
    return None


def _infer_connector(name: str) -> str | None:
    """Infer connector type from port name when connector is null."""
    for canonical, pattern in _CONNECTOR_INFERENCES:
        if re.search(pattern, name, re.IGNORECASE):
            return canonical
    return None


def _parse_channels(ch: Any) -> int | None:
    """Attempt to coerce a channel value to an integer."""
    if isinstance(ch, int):
        return ch
    if isinstance(ch, str):
        m = re.search(r"(\d+)", ch.replace(",", ""))
        if m:
            return int(m.group(1))
    return None


def _merge_ports(a: dict, b: dict) -> dict:
    """Merge two ports with same canonical name, direction, connector."""
    ca = _parse_channels(a.get("channels"))
    cb = _parse_channels(b.get("channels"))
    merged_ch = (ca + cb) if ca is not None and cb is not None else (ca if ca is not None else cb)

    attrs_a = set(a.get("attributes") or [])
    attrs_b = set(b.get("attributes") or [])
    merged_attrs = sorted(attrs_a | attrs_b) or None

    return {
        "name": a["name"],
        "direction": a["direction"],
        "connector": a["connector"],
        "channels": merged_ch,
        "attributes": merged_attrs,
    }


def _normalize_bridge(bridge: str) -> str | None:
    """Strip whitespace and standardize arrow format. Return None for self-loops."""
    cleaned = bridge.strip()
    # Normalize various arrow forms to "->"
    cleaned = re.sub(r"\s*[-=]+>\s*", "->", cleaned)
    cleaned = re.sub(r"\s*→\s*", "->", cleaned)
    if "->" not in cleaned:
        return cleaned
    parts = cleaned.split("->", 1)
    src = parts[0].strip()
    dst = parts[1].strip()
    if src == dst:
        return None
    return f"{src}->{dst}"


def _has_port_category(ports: list[dict], category: str) -> bool:
    """Check if any port matches the required category."""
    for port in ports:
        name = (port.get("name") or "").lower()
        connector = (port.get("connector") or "").lower()
        attrs = port.get("attributes", []) or []
        attr_str = " ".join(str(a) for a in attrs).lower()
        combined = f"{name} {connector} {attr_str}"
        if category == "dante_port" and "dante" in combined:
            return True
        if category == "analog_port" and any(
            x in combined for x in ("xlr", "trs", "rca", "analog", "mic", "line")
        ):
            return True
        if category == "digital_port" and any(
            x in combined for x in ("aes3", "aes", "digital", "madi", "optical", "coaxial")
        ):
            return True
        if category == "antenna_port" and any(
            x in combined for x in ("antenna", "bnc", "sma", "diversity")
        ):
            return True
        if category == "network_port" and any(
            x in combined for x in ("ethernet", "rj45", "network", "tcp", "ip")
        ):
            return True
        if category == "usb_port" and "usb" in combined:
            return True
        if category == "control_port" and any(
            x in combined for x in ("gpio", "contact", "rs-232", "control")
        ):
            return True
    return False


def _placeholder_port(category: str) -> dict:
    """Create a low-confidence placeholder port for a missing category."""
    port = copy.deepcopy(_PLACEHOLDER_SPECS.get(category, _PLACEHOLDER_SPECS["control_port"]))
    port["_placeholder"] = True
    port["_confidence"] = "low"
    return port


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _normalize_ports(raw_ports: list[dict]) -> list[dict]:
    """Canonicalize names, infer missing fields, and merge duplicates."""
    normalized: list[dict] = []
    for port in raw_ports:
        if not isinstance(port, dict):
            continue
        name = port.get("name")
        if not name:
            continue

        canonical_name = _canonicalize_name(name)
        direction = _standardize_direction(port.get("direction"), canonical_name)
        connector = port.get("connector") or _infer_connector(canonical_name)
        channels = _parse_channels(port.get("channels"))
        attrs = port.get("attributes")
        if attrs is not None and not isinstance(attrs, list):
            attrs = [attrs] if attrs else None

        normalized.append(
            {
                "name": canonical_name,
                "direction": direction,
                "connector": connector,
                "channels": channels,
                "attributes": attrs,
            }
        )

    # First dedupe by name alone — if the LLM emitted the same port twice
    # (e.g. main extraction + peripheral pass), keep the richer instance.
    name_map: dict[str, dict] = {}
    for port in normalized:
        name = port["name"]
        if name not in name_map:
            name_map[name] = port
        else:
            existing = name_map[name]
            # Keep the one with more fields filled in
            existing_score = sum(1 for k in ("direction", "connector", "channels") if existing.get(k))
            new_score = sum(1 for k in ("direction", "connector", "channels") if port.get(k))
            if new_score > existing_score:
                name_map[name] = port
            elif new_score == existing_score:
                name_map[name] = _merge_ports(existing, port)

    normalized = list(name_map.values())

    merged_map: dict[tuple[str, str | None, str | None], dict] = {}
    for port in normalized:
        key = (port["name"], port["direction"], port["connector"])
        if key in merged_map:
            merged_map[key] = _merge_ports(merged_map[key], port)
        else:
            merged_map[key] = port
    return list(merged_map.values())


def _normalize_bridges(raw_bridges: list[str]) -> list[str]:
    """Dedupe bridges, strip whitespace, standardize arrows, drop self-loops."""
    seen: set[str] = set()
    out: list[str] = []
    for bridge in raw_bridges:
        if not isinstance(bridge, str):
            continue
        norm = _normalize_bridge(bridge)
        if norm is None or norm in seen:
            continue
        seen.add(norm)
        out.append(norm)
    return out


def apply_bridge_heuristics(extracted: dict, device_class: str) -> dict:
    """Replace LLM bridges with deterministic class-specific heuristics."""
    signal_flow = extracted.setdefault("signal_flow", {})
    ports = signal_flow.get("ports") or []
    heuristics = _BRIDGE_HEURISTICS.get(device_class, [])
    new_bridges: list[str] = []

    for src_pat, dst_pat in heuristics:
        src_port = None
        dst_port = None
        for port in ports:
            name = (port.get("name") or "").lower()
            if src_pat.lower() in name and src_port is None:
                src_port = port["name"]
            if dst_pat.lower() in name and dst_port is None:
                dst_port = port["name"]
            if src_port is not None and dst_port is not None:
                break
        if src_port is not None and dst_port is not None:
            new_bridges.append(f"{src_port} -> {dst_port}")

    signal_flow["bridges"] = new_bridges
    return extracted


def normalize_extraction(extracted: dict, device_class: str) -> dict:
    """Canonicalize and normalize an LLM extraction result.

    Args:
        extracted: The raw extraction dict from Stage 5.
        device_class: The classified device class (e.g. "dante_stagebox").

    Returns:
        A new dict with normalized ports, bridges, and inferred fields.
    """
    result = copy.deepcopy(extracted)
    signal_flow = result.setdefault("signal_flow", {})

    merged_ports = _normalize_ports(signal_flow.get("ports") or [])
    deduped_bridges = _normalize_bridges(signal_flow.get("bridges") or [])

    for cat in _REQUIRED_CATEGORIES.get(device_class, []):
        if not _has_port_category(merged_ports, cat):
            merged_ports.append(_placeholder_port(cat))

    merged_ports.sort(key=lambda p: (p.get("direction") or "", p.get("name") or ""))
    deduped_bridges.sort()

    # General connector cleanup: normalize common variants
    _CONNECTOR_CLEANUP = {
        "rj-11": "RJ11",
        "rj-45": "RJ45",
        "rj11": "RJ11",
        "rj45": "RJ45",
        "usb-b": "USB-B",
        "usb-a": "USB-A",
        "usb_b": "USB-B",
        "usb_a": "USB-A",
        "ethercon": "etherCON",
        "ethernet": "RJ45",
        "gigabit ethernet": "RJ45",
        "fast ethernet": "RJ45",
    }
    for port in merged_ports:
        conn = port.get("connector")
        if conn:
            conn_lc = conn.lower()
            if conn_lc in _CONNECTOR_CLEANUP:
                port["connector"] = _CONNECTOR_CLEANUP[conn_lc]

    # Apply connector / direction overrides for known devices.
    device_key = f"{extracted.get('device_metadata', {}).get('manufacturer', '')}:{extracted.get('device_metadata', {}).get('model_number', '')}"
    for port in merged_ports:
        port_name = port.get("name", "")
        for override_key, overrides in _CONNECTOR_OVERRIDES.items():
            if override_key.lower() == device_key.lower():
                if port_name in overrides:
                    port["connector"] = overrides[port_name]
        for override_key, overrides in _DIRECTION_OVERRIDES.items():
            if override_key.lower() == device_key.lower():
                if port_name in overrides:
                    port["direction"] = overrides[port_name]

    signal_flow["ports"] = merged_ports
    signal_flow["bridges"] = deduped_bridges

    result = apply_bridge_heuristics(result, device_class)
    return result
