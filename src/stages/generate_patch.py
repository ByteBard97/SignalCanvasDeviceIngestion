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


def _build_port_line(port: dict) -> str:
    """Render a single PatchLang port definition."""
    name = _sanitize_identifier(port.get("name", "Port"))
    direction = _map_direction(port.get("direction"))
    connector = port.get("connector")
    channels = _parse_channels(port.get("channels"))
    attrs = port.get("attributes")

    parts = [name]
    if channels is not None and channels > 1:
        parts.append(f"[1..{channels}]")
    parts.append(": ")
    parts.append(direction)
    if connector:
        parts.append(f"({_sanitize_identifier(connector)})")

    if attrs:
        sanitized_attrs = [_sanitize_attribute(str(a)) for a in attrs if str(a).strip()]
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
        lines.append(f'    model: "{model}"')
    if category:
        lines.append(f'    category: "{category}"')
    lines.append("  }")

    if ports:
        lines.append("  ports {")
        for port in ports:
            if not isinstance(port, dict):
                continue
            lines.append(f"    {_build_port_line(port)}")
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
