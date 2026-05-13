"""Completeness review for extracted device specs.

After a patch passes syntax validation, this module checks whether the
extraction is semantically complete — enough ports, appropriate port types
for the device category, and overall richness.  If not, it returns concerns
that can be fed back into a critique/re-extraction loop.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Device-class heuristics
# ---------------------------------------------------------------------------

# Devices that are expected to have 3+ signal-flow ports.
_COMPLEX_DEVICE_CLASSES: set[str] = {
    "mixer",
    "dsp",
    "dsp_processor",
    "dante_stagebox",
    "matrix",
    "switcher",
    "router",
    "codec",
    "processor",
    "recorder",
    "intercom",
    "stagebox",
}

# Minimum port counts by class (signal-flow ports only).
_MIN_PORTS_BY_CLASS: dict[str, int] = {
    "mixer": 4,
    "dsp": 4,
    "dsp_processor": 4,
    "dante_stagebox": 3,
    "dante_adapter_input": 2,
    "dante_adapter_output": 2,
    "matrix": 4,
    "switcher": 3,
    "router": 3,
    "codec": 3,
    "processor": 3,
    "recorder": 3,
    "intercom": 3,
    "stagebox": 3,
    "speaker": 1,
    "microphone": 1,
    "headset": 1,
    "wireless_rx": 2,
    "camera": 2,
    "display": 2,
    "projector": 2,
    "monitor": 2,
}

# Classes that *must* have a specific port category.
# Each entry: list of (keywords, description) where keywords is a list of
# alternative strings — if ANY keyword matches, the category is satisfied.
_REQUIRED_PORT_CATEGORIES: dict[str, list[tuple[list[str], str]]] = {
    "dante_stagebox": [(["dante"], "Dante network port")],
    "dante_adapter_input": [(["dante"], "Dante network port")],
    "dante_adapter_output": [(["dante"], "Dante network port")],
    "wireless_rx": [
        (["antenna", "rf"], "RF/antenna port"),
        (["output"], "audio output"),
    ],
    "speaker": [(["input"], "audio input")],
    "mixer": [(["input"], "audio input")],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_control_only(port: dict) -> bool:
    """Return True if port is control/data-only (not audio signal flow)."""
    name = (port.get("name") or "").lower()
    conn = (port.get("connector") or "").lower()
    if "usb" in name:
        return True
    if name in ("gpi", "gpo", "gpio", "network", "ethernet", "control", "power"):
        return True
    if conn in ("usb", "usb-a", "usb-b", "usb-c"):
        return True
    return False


def _count_signal_ports(ports: list[dict]) -> int:
    """Count ports that appear to be signal-flow (not pure control)."""
    return sum(1 for p in ports if not _is_control_only(p))


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def check_completeness(extracted: dict, device_class: str) -> tuple[bool, list[str]]:
    """Check whether an extraction is semantically complete.

    Args:
        extracted: Normalized extraction dict (has ``signal_flow.ports``).
        device_class: Device classification string (e.g. ``"dante_stagebox"``).

    Returns:
        ``(is_complete, concerns)`` where *concerns* is a list of human-readable
        strings explaining what appears to be missing or suspicious.
    """
    ports: list[dict] = extracted.get("signal_flow", {}).get("ports") or []
    if not isinstance(ports, list):
        ports = []

    concerns: list[str] = []
    signal_ports = _count_signal_ports(ports)

    # 1. Zero ports is always incomplete.
    if len(ports) == 0:
        concerns.append(
            "Zero ports were extracted. The device template has no I/O at all."
        )
        return False, concerns

    # 2. Zero *signal* ports (only control/power).
    if signal_ports == 0:
        concerns.append(
            "Only control/network/power ports were found — no audio signal-flow ports."
        )

    # 3. Too few ports for the device class.
    expected_min = _MIN_PORTS_BY_CLASS.get(device_class)
    if expected_min is not None and signal_ports < expected_min:
        concerns.append(
            f"Only {signal_ports} signal port(s) found for a '{device_class}' device. "
            f"Expected at least {expected_min}."
        )

    # 4. Very small extraction (agent may have stopped early).
    # Measure the JSON-serialized size, not the Python repr.
    import json
    extracted_size = len(json.dumps(extracted))
    if extracted_size < 350:
        concerns.append(
            f"Extraction is very small ({extracted_size} bytes of JSON). "
            "The agent may have stopped prematurely."
        )

    # 5. Required port categories for the device class.
    for keywords, description in _REQUIRED_PORT_CATEGORIES.get(device_class, []):
        found = False
        for p in ports:
            name = str(p.get("name", "")).lower()
            direction = str(p.get("direction", "")).lower()
            connector = str(p.get("connector", "")).lower()
            # Normalize direction aliases so we match both raw and normalized data
            if direction in ("in", "input"):
                direction = "input"
            elif direction in ("out", "output"):
                direction = "output"
            combined = f"{name} {direction} {connector}"
            if any(kw in combined for kw in keywords):
                found = True
                break
        if not found:
            concerns.append(
                f"No {description} found, but '{device_class}' devices typically have one."
            )

    # 6. Very low confidence extractions with minimal ports.
    confidence = (extracted.get("extraction_confidence") or "").lower()
    if confidence == "low" and signal_ports <= 2:
        concerns.append(
            "Extraction confidence is LOW and port count is minimal. "
            "The corpus may lack technical specifications."
        )

    return len(concerns) == 0, concerns
