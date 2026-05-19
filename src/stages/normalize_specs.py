"""Post-processing normalization for Stage 5 LLM extractions.

Canonicalizes port names, merges equivalent ports, dedupes bridges,
infers missing fields, enforces required categories, and sorts
outputs deterministically.
"""

from __future__ import annotations

import copy
import re
from typing import Any

from stages.device_enrichment import enrich_extraction

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
    # converter bridges are inferred from port structure — see _infer_converter_bridges
    "converter": [],
}

# Signal type tokens that indicate a professional AV protocol — used for
# converter bridge inference.  Order matters: more specific first.
_SIGNAL_PROTOCOL_TOKENS: list[tuple[str, str]] = [
    ("dante", "Dante"),
    ("ndi", "NDI"),
    ("madi", "MADI"),
    ("aes67", "AES67"),
    ("aes3", "AES3"),
    ("aes", "AES"),
    ("avb", "AVB"),
    ("milan", "Milan"),
    ("ravenna", "Ravenna"),
    ("sdi", "SDI"),
    ("hd-sdi", "SDI"),
    ("hdbaset", "HDBaseT"),
    ("hdmi", "HDMI"),
    ("displayport", "DisplayPort"),
    ("genlock", "Genlock"),
    ("timecode", "Timecode"),
    ("analog", "Analog"),
    ("analogue", "Analog"),
]

# Port name/attr tokens that disqualify a port from converter bridge inference
_NON_SIGNAL_TOKENS = {
    "power", "mains", "iec", "gpio", "gpi", "gpo", "rs232", "rs422",
    "rs485", "usb", "ethernet", "network", "control", "management",
    "wordclock", "word clock", "loop", "thru", "through",
}


def _port_protocol(port: dict) -> str | None:
    """Extract the dominant AV signal protocol from a port's name and attributes."""
    name = (port.get("name") or "").lower()
    attrs = " ".join(str(a) for a in (port.get("attributes") or [])).lower()
    combined = f"{name} {attrs}"
    for token, canonical in _SIGNAL_PROTOCOL_TOKENS:
        if token in combined:
            return canonical
    return None


def _is_non_signal_port(port: dict) -> bool:
    name = (port.get("name") or "").lower()
    attrs = " ".join(str(a) for a in (port.get("attributes") or [])).lower()
    combined = f"{name} {attrs}"
    return any(t in combined for t in _NON_SIGNAL_TOKENS)


def _infer_converter_bridges(ports: list[dict]) -> list[str]:
    """Infer bridge statements from port structure.

    Covers two patterns:
      - Protocol conversion: all inputs are protocol A, all outputs are protocol B (A≠B)
        → bridge first_input -> first_output
      - Distribution amp: one input, N outputs of the same protocol
        → bridge input -> first_output  (one bridge summarises the fan-out)
    """
    signal_ports = [p for p in ports if isinstance(p, dict) and not _is_non_signal_port(p)]

    in_ports = [p for p in signal_ports if (p.get("direction") or "").lower() in ("input", "in")]
    out_ports = [p for p in signal_ports if (p.get("direction") or "").lower() in ("output", "out")]

    if not in_ports or not out_ports:
        return []

    in_protos = {_port_protocol(p) for p in in_ports} - {None}
    out_protos = {_port_protocol(p) for p in out_ports} - {None}

    if not in_protos or not out_protos:
        return []

    in_name = in_ports[0].get("name", "Input")
    out_name = out_ports[0].get("name", "Output")

    # Protocol conversion: clear A→B mapping
    if in_protos != out_protos and len(in_protos) == 1 and len(out_protos) == 1:
        return [f"{in_name} -> {out_name}"]

    # Distribution amp: one input protocol, same protocol on outputs, more outputs than inputs
    if in_protos == out_protos and len(in_ports) == 1 and len(out_ports) > 1:
        return [f"{in_name} -> {out_name}"]

    return []

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


def _canonicalize_name(name: str | None) -> str:
    """Map semantically-equivalent port names to a canonical form."""
    if not name:
        return ""
    # Strip leading numbers like "14 Assignable Local Outputs" -> "Assignable Local Outputs"
    cleaned = re.sub(r"^\d+\s*", "", name)
    lowered = cleaned.lower()
    for canonical, patterns in _NAME_RULES:
        for pat in patterns:
            # Use word-boundary checks so short tokens like "lan" don't match
            # inside "balanced".
            regex = r"(?<![a-z])" + re.escape(pat) + r"(?![a-z])"
            if re.search(regex, lowered):
                return canonical
    return cleaned

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


def _is_power_port(port: dict) -> bool:
    """Return True if this port is a mains power input (not a signal-flow port)."""
    name = (port.get("name") or "").lower()
    power_keywords = [
        "mains power", "power input", "ac power", "dc power",
        "iec inlet", "power supply", "power inlet", "power socket",
    ]
    # Exact match for bare "Power" when connector also suggests mains
    if name == "power":
        return True
    return any(kw in name for kw in power_keywords)


def _is_control_port(port: dict) -> bool:
    """Return True if this port is a control/data port (not an audio signal-flow port).

    USB config/service ports, logic I/O, and control-only network ports are not
    audio signal paths.  USB *audio* ports (e.g. on audio interfaces, mics,
    recorders) ARE signal-flow ports and should be kept.
    """
    name = (port.get("name") or "").lower()
    connector = (port.get("connector") or "").lower()

    # Keep USB ports that are explicitly audio-related
    if "usb audio" in name or "usb streaming" in name:
        return False

    # Drop USB ports whose name clearly indicates control/config purpose
    usb_control_keywords = ["config", "service", "hid", "firmware", "update", "debug", "mgmt"]
    if "usb" in name and any(kw in name for kw in usb_control_keywords):
        return True

    # Drop logic I/O ports (GPI/GPO, GPIO)
    if name in ("gpi", "gpo", "gpio"):
        return True

    # For non-USB-named ports with USB connectors (e.g. "Console" on USB-A),
    # drop only if the name also suggests control
    control_names = {"console", "service", "config", "setup", "debug", "firmware", "update"}
    if any(cn in name for cn in control_names) and "usb" in connector:
        return True

    return False


# Device-specific non-signal ports that should be dropped regardless of name.
# Keyed by canonical device key "Manufacturer:Model".
_NON_SIGNAL_PORT_DENYLIST: dict[str, list[str]] = {
    # SQ-5 Network is control-only (MIDI, TCP/IP). SLink is the audio expansion port.
    "Allen & Heath:SQ-5": ["Network"],
    # ULXD4 Network is Wireless Workbench control only, not Dante audio.
    "Shure Incorporated:ULXD4": ["Network"],
}


def _drop_false_positives(ports: list[dict], device_class: str) -> list[dict]:
    """Remove known false-positive ports that the LLM commonly hallucinates."""
    result: list[dict] = []
    has_rf_antenna = any("rf antenna" in (p.get("name") or "").lower() for p in ports)
    for p in ports:
        name = (p.get("name") or "").lower()
        direction = (p.get("direction") or "").strip()
        connector = (p.get("connector") or "").strip()

        # Wireless receivers: drop SMA_Connector when RF_Antenna already exists
        # (ULXD4 has BNC antenna connectors, not SMA)
        if device_class == "wireless_rx" and has_rf_antenna and "sma" in name:
            continue

        # Drop ports that have neither direction nor connector — these are specs,
        # not physical signal ports (e.g., "Gain/Attenuation", "Resolution").
        if not direction and not connector:
            continue

        # Drop TA4M/LEMO on wireless receivers — it's a transmitter input, not a receiver port
        if device_class == "wireless_rx" and "ta4m" in name:
            continue

        # Dante input adapters (e.g., AVIO-AI2) should not have analog outputs or separate Network ports
        if device_class == "dante_adapter_input":
            if "main output" in name or "network" in name:
                continue

        # Dante output adapters (e.g., AVIO-AO2) should not have analog inputs or separate Network ports
        if device_class == "dante_adapter_output":
            if "analog input" in name or "network" in name:
                continue

        result.append(p)
    return result


def _clean_attributes(attrs: list[str] | None) -> list[str] | None:
    """Drop sentence-length descriptions; keep only short tags."""
    if not attrs:
        return None
    cleaned: list[str] = []
    for a in attrs:
        s = str(a).strip()
        if not s:
            continue
        # Drop full sentences (long AND contain spaces)
        if len(s) > 25 or (len(s) > 15 and " " in s):
            continue
        cleaned.append(s)
    return cleaned if cleaned else None


# Ports that are always a single physical connector regardless of audio channels
_SINGLE_CONNECTOR_PORTS: list[str] = [
    "headphone",
    "talkback",
    "aes digital output",
    "aes output",
    "aes3 output",
]

# Digital protocols where a single connector carries many audio channels.
# When matched, high channel counts should be treated as 1 physical port.
_CHANNEL_BASED_PROTOCOLS: list[str] = [
    "dante", "qlan", "madi", "aes50", "ultranet", "p16", "soundgrid",
    "optocore", "omneo", "gigaace", "dmx", "dmx512", "network", "nd",
]

# Connectors that typically carry multiplexed channels rather than representing
# multiple identical physical connectors in a row.
_MULTIPLEXED_CONNECTORS: set[str] = {
    "rj45", "ethercon", "ethernet", "bnc_75", "bnc", "sc_fiber",
    "lc_fiber", "sfp", "hdmi", "displayport", "usb", "usb-a", "usb-b",
    "usb-c", "coaxial", "optical", "fiber",
}


def _correct_channels(port: dict) -> None:
    """Fix common channel-count miscounts (audio channels vs physical connectors)."""
    name = (port.get("name") or "").lower()
    connector = (port.get("connector") or "").lower().replace("-", "_")
    channels = port.get("channels")

    if not isinstance(channels, int) or channels <= 1:
        return

    # Always-single-connector ports (headphone, talkback, AES)
    for pattern in _SINGLE_CONNECTOR_PORTS:
        if pattern in name:
            port["channels"] = None
            return

    # Channel-based protocols on multiplexed connectors:
    # e.g., "64 Dante channels over 1 RJ45" should be 1 port, not 64.
    # Also catches "Dante_Pri_Out[1..2]" which is always 1 physical port.
    has_protocol = any(p in name for p in _CHANNEL_BASED_PROTOCOLS)
    is_multiplexed = connector in _MULTIPLEXED_CONNECTORS or any(
        c in connector for c in _MULTIPLEXED_CONNECTORS
    )

    if has_protocol and is_multiplexed and channels >= 2:
        port["channels"] = 1
        return

    # Broad safety net: any multiplexed connector with a suspiciously high
    # channel count is almost certainly audio-channel expansion.
    if is_multiplexed and channels >= 32:
        port["channels"] = 1
        return

    # DMX over XLR: 512 channels = 1 physical XLR port
    if "dmx" in name and channels >= 128:
        port["channels"] = 1
        return


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
    # Ports with the same name but different connectors are kept separate
    # (e.g. "Assignable Local Outputs" on XLR vs TRS).
    name_map: dict[str, list[dict]] = {}
    for port in normalized:
        name = port["name"]
        if name not in name_map:
            name_map[name] = [port]
            continue

        candidates = name_map[name]
        merged = False
        for i, existing in enumerate(candidates):
            if existing.get("connector") == port.get("connector"):
                # Same name + same connector → dedupe/merge
                existing_score = sum(1 for k in ("direction", "connector", "channels") if existing.get(k))
                new_score = sum(1 for k in ("direction", "connector", "channels") if port.get(k))
                if new_score > existing_score:
                    candidates[i] = port
                elif new_score == existing_score:
                    candidates[i] = _merge_ports(existing, port)
                merged = True
                break
        if not merged:
            # Different connector → keep as separate port
            candidates.append(port)

    normalized = []
    for candidates in name_map.values():
        normalized.extend(candidates)

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
    """Augment LLM bridges with deterministic class-specific heuristics.

    Only overwrites bridges when the device_class has an explicit heuristic
    entry (including an empty list).  For unknown classes, preserves the
    LLM-extracted bridges unchanged.
    """
    signal_flow = extracted.setdefault("signal_flow", {})
    ports = signal_flow.get("ports") or []

    # If this class has no heuristic entry at all, keep LLM bridges as-is.
    if device_class not in _BRIDGE_HEURISTICS:
        return extracted

    heuristics = _BRIDGE_HEURISTICS[device_class]
    new_bridges: list[str] = []

    # converter class: use port-structure inference instead of name patterns
    if device_class == "converter":
        inferred = _infer_converter_bridges(ports)
        if inferred:
            signal_flow["bridges"] = inferred
        return extracted

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

    # NOTE: Previously we injected placeholder ports for missing required categories
    # based on device classification. This caused phantom ports (e.g. Dante on a
    # headphone amp) when the classifier was wrong or the PDF simply didn't mention
    # the port. We now trust the extraction: if the PDF doesn't mention a port,
    # it doesn't exist. Placeholders are only kept if they came from the LLM
    # extraction itself (not injected here).
    pass

    merged_ports.sort(key=lambda p: (p.get("direction") or "", p.get("name") or ""))
    deduped_bridges.sort()

    # Filter out non-signal ports (power mains, USB control ports, etc.)
    merged_ports = [p for p in merged_ports if not _is_power_port(p)]
    merged_ports = [p for p in merged_ports if not _is_control_port(p)]

    merged_ports = _drop_false_positives(merged_ports, device_class)

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

    # Apply device-specific non-signal port denylist
    denylist = _NON_SIGNAL_PORT_DENYLIST.get(device_key, [])
    if denylist:
        denyset = {n.lower() for n in denylist}
        merged_ports = [p for p in merged_ports if (p.get("name") or "").strip().lower() not in denyset]

    # Cleanup: correct single-connector ports and strip sentence-length attributes
    for port in merged_ports:
        _correct_channels(port)
        port["attributes"] = _clean_attributes(port.get("attributes"))

    # Sanitize false "Dante" labels — the LLM defaults to Dante for every RJ45 port
    for port in merged_ports:
        attrs = port.get("attributes") or []
        name = (port.get("name") or "").lower()
        conn = (port.get("connector") or "").lower()
        if "dante" in [a.lower() for a in attrs]:
            # If the port name clearly indicates another protocol, swap it
            if "aes50" in name:
                attrs = ["AES50" if a.lower() == "dante" else a for a in attrs]
            elif "madi" in name:
                attrs = ["MADI" if a.lower() == "dante" else a for a in attrs]
            elif "ultranet" in name:
                attrs = ["Ultranet" if a.lower() == "dante" else a for a in attrs]
            elif "avb" in name:
                attrs = ["AVB" if a.lower() == "dante" else a for a in attrs]
            # If the document didn't explicitly mention Dante (no "dante" in chunks)
            # and the connector is generic network, fall back to "Network"
            elif conn in ("rj45", "ethercon", "ethernet"):
                attrs = ["Network" if a.lower() == "dante" else a for a in attrs]
            port["attributes"] = attrs

    signal_flow["ports"] = merged_ports
    signal_flow["bridges"] = deduped_bridges

    result = apply_bridge_heuristics(result, device_class)

    # ------------------------------------------------------------------
    # Enrichment: inject known missing ports for well-documented devices
    # ------------------------------------------------------------------
    device_metadata = extracted.get("device_metadata", {})
    manufacturer = device_metadata.get("manufacturer", "")
    model_number = device_metadata.get("model_number", "")
    result = enrich_extraction(result, manufacturer, model_number)

    return result
