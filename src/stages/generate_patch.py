"""Stage 6: Generate PatchLang source from extracted device specs."""

from __future__ import annotations

import re
from typing import Any

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sanitize_identifier(value: str) -> str:
    """Convert a string to a valid PatchLang identifier.

    Replaces all non-alphanumeric characters with underscores, collapses
    consecutive underscores, strips leading/trailing underscores, and
    prefixes with an underscore if the result starts with a digit.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    if cleaned and cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned or "Port"


def _sanitize_attribute(value: str) -> str:
    """Sanitize an attribute string to a valid PatchLang identifier.

    Attributes must be valid unquoted identifiers. We aggressively
    strip non-identifier characters and collapse the result.
    """
    # Keep only letters, digits, and underscores
    cleaned = re.sub(r"[^A-Za-z0-9_]", "_", value)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    if cleaned and cleaned[0].isdigit():
        cleaned = "_" + cleaned
    return cleaned or "attr"


def _parse_channels(ch: Any) -> int | None:
    """Coerce a channel value to an integer."""
    if isinstance(ch, int):
        return ch
    if isinstance(ch, str):
        m = re.search(r"(\d+)", ch.replace(",", ""))
        if m:
            return int(m.group(1))
    return None


def _map_direction(direction: str | None) -> str:
    """Map extracted direction to PatchLang direction keyword."""
    if not direction:
        return "io"
    d = direction.strip().lower()
    if d in ("input", "in"):
        return "in"
    if d in ("output", "out"):
        return "out"
    if d in ("input_output", "input/output", "i/o", "bidirectional"):
        return "io"
    return "io"


# Port name suffixes that unambiguously identify the signal type.
# If a port name contains one of these tokens, the signal type is overridden
# regardless of what the LLM extracted in attributes — the LLM frequently
# copies the signal type from an adjacent port (e.g. DVI tagged as SDI).
_NAME_SIGNAL_OVERRIDE: dict[str, str] = {
    "dvi": "DVI",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "_dp": "DisplayPort",
    "vga": "VGA",
    "sdi": "SDI",
    "hdsdi": "HD-SDI",
    "dante": "Dante",
    "avb": "AVB",
    "aes67": "AES67",
    "madi": "MADI",
    "aes": "AES",
    "aes3": "AES3",
}


def _infer_signal_from_name(port_name: str) -> str | None:
    """Return a corrected signal type if the port name unambiguously implies one."""
    lower = port_name.lower()
    for token, signal in _NAME_SIGNAL_OVERRIDE.items():
        if token in lower:
            return signal
    return None


# Map messy extracted connector strings to clean canonical names
_CONNECTOR_CANON: dict[str, str] = {
    "1/4 trs": "TRS",
    '1/4" trs': "TRS",
    "1/4 ts": "TS",
    '1/4" ts': "TS",
    "3.5mm": "MiniJack",
    "3.5 mm": "MiniJack",
    "mini jack": "MiniJack",
    "minijack": "MiniJack",
    "ta4m": "LEMO",
    "ta4f": "LEMO",
    "usb_b": "USB-B",
    "usb b": "USB-B",
    "usb-a": "USB-A",
    "usb a": "USB-A",
    "xlr 3-31": "XLR",
    "xlr 3-32": "XLR",
    "xlr 3-33": "XLR",
    "xlr 3-34": "XLR",
    "xlr-3-31": "XLR",
    "xlr-3-32": "XLR",
    "xlr-3-33": "XLR",
    "xlr-3-34": "XLR",
}


def _canonicalize_connector(raw: str | None) -> str | None:
    if not raw:
        return None
    key = raw.strip().lower()
    return _CONNECTOR_CANON.get(key, raw)


def _build_port_line(port: dict) -> str:
    """Render a single PatchLang port definition."""
    name = _sanitize_identifier(port.get("name", "Port"))
    direction = _map_direction(port.get("direction"))
    connector = _canonicalize_connector(port.get("connector"))
    channels = _parse_channels(port.get("channels"))
    attrs = list(port.get("attributes") or [])

    # If the port name unambiguously implies a signal type, override LLM attributes.
    # The LLM frequently copies the signal type from an adjacent port (e.g. DVI → SDI).
    inferred = _infer_signal_from_name(port.get("name", ""))
    if inferred:
        attrs = [a for a in attrs if a.upper() not in {s.upper() for s in _NAME_SIGNAL_OVERRIDE.values()}]
        attrs.insert(0, inferred)

    parts = [name]
    if channels is not None and channels > 1:
        parts.append(f"[1..{channels}]")
    parts.append(": ")
    parts.append(direction)
    if connector:
        parts.append(f"({_sanitize_identifier(connector)})")

    if attrs:
        # Belt-and-suspenders: drop anything that still looks like a sentence
        sanitized_attrs = [
            _sanitize_attribute(str(a))
            for a in attrs
            if str(a).strip() and len(str(a).strip()) <= 25 and not (len(str(a).strip()) > 15 and " " in str(a).strip())
        ]
        if sanitized_attrs:
            parts.append(f" [{', '.join(sanitized_attrs)}]")

    return "".join(parts)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_patch(extracted: dict) -> str:
    """Convert a normalized extraction dict to a PatchLang template string.

    Args:
        extracted: Dict following the Stage-5 extraction schema.

    Returns:
        Complete PatchLang source string.
    """
    meta = extracted.get("device_metadata", {})
    signal_flow = extracted.get("signal_flow", {})
    ports = signal_flow.get("ports") or []
    bridges = signal_flow.get("bridges") or []

    template_name = _sanitize_identifier(meta.get("model_number", "Device"))
    manufacturer = meta.get("manufacturer", "")
    model = meta.get("model_number", "")
    category = meta.get("category", "")

    lines: list[str] = [f"template {template_name} {{"]
    lines.append("  meta {")
    lines.append('    kind: "device"')
    if manufacturer:
        lines.append(f'    manufacturer: "{manufacturer}"')
    if model:
        # PatchLang parser does not support escaped quotes inside string literals.
        # Replace the ASCII double-quote (inch symbol) with the Unicode double-prime
        # U+2033 "″" which parses cleanly and reads identically.
        safe_model = model.replace('"', '\u2033')
        lines.append(f'    model: "{safe_model}"')
    if category:
        lines.append(f'    category: "{category}"')
    lines.append("  }")

    if ports:
        lines.append("  ports {")
        seen_names: dict[str, int] = {}
        for port in ports:
            if not isinstance(port, dict):
                continue
            port_line = _build_port_line(port)
            # Extract the sanitized name (first token before ':' or '[')
            raw_name = _sanitize_identifier(port.get("name", "Port"))
            # Detect duplicates and disambiguate by appending connector or counter
            if raw_name in seen_names:
                seen_names[raw_name] += 1
                connector = port.get("connector")
                if connector and _sanitize_identifier(connector) != raw_name:
                    suffix = _sanitize_identifier(connector)
                else:
                    suffix = str(seen_names[raw_name])
                # Rewrite the line with the disambiguated name
                port_line = port_line.replace(raw_name, f"{raw_name}_{suffix}", 1)
            else:
                seen_names[raw_name] = 1
            lines.append(f"    {port_line}")
        lines.append("  }")

    lines.extend(_build_bridge_lines(bridges, ports))
    lines.append("}")
    return "\n".join(lines) + "\n"


def _build_bridge_lines(bridges: list[Any], ports: list[Any]) -> list[str]:
    """Render bridge lines, skipping references to undeclared ports."""
    port_names = {_sanitize_identifier(p.get("name", "")) for p in ports if isinstance(p, dict)}
    out: list[str] = []
    for bridge in bridges:
        if not isinstance(bridge, str) or "->" not in bridge:
            continue
        src, dst = bridge.split("->", 1)
        src = _sanitize_identifier(src.strip())
        dst = _sanitize_identifier(dst.strip())
        if src in port_names and dst in port_names:
            out.append(f"  bridge {src} -> {dst}")
    return out
