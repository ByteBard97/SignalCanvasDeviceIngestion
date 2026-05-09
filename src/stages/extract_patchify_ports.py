"""Extract signal-flow specs directly from patchify inputs/outputs arrays.

Patchify stores user-defined port data in `inputs` and `outputs` on every entry.
Since 4614/4615 entries are community submissions with empty `model` fields,
this is often the only reliable source of connector information.
"""

import json
from pathlib import Path
from typing import Any, Optional

_PATCHIFY_PATH = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
_PATCHIFY_BY_ID: dict[str, dict] = {}
_PATCHIFY_BY_DEVICE_ID: dict[str, dict] = {}


def _load_patchify_index() -> None:
    """Lazy-load patchify JSON and build lookup indexes."""
    global _PATCHIFY_BY_ID, _PATCHIFY_BY_DEVICE_ID
    if _PATCHIFY_BY_ID:
        return

    if not _PATCHIFY_PATH.exists():
        return

    with open(_PATCHIFY_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        item_id = item.get("id", "")
        if item_id:
            _PATCHIFY_BY_ID[item_id] = item

        # Build device_id the same way run_batch.py does
        mfg = item.get("manufacturer", "")
        name = item.get("name", "")
        mfg_slug = mfg.lower().replace(" ", "-").replace("_", "-")
        name_slug = name.lower().replace(" ", "-").replace("/", "-").replace("_", "-")
        name_slug = "-".join(filter(None, name_slug.split("-")))
        device_id = f"{mfg_slug}-{name_slug}"[:64]
        if device_id:
            _PATCHIFY_BY_DEVICE_ID[device_id] = item

        # Also index by swapped mfg/name in case the pipeline got them backwards
        # e.g. "lav-k33-rx-sony" could be mfg="Lav K33 RX" name="Sony"
        # OR mfg="Sony" name="Lav K33 RX"
        swapped_id = f"{name_slug}-{mfg_slug}"[:64]
        if swapped_id and swapped_id != device_id:
            _PATCHIFY_BY_DEVICE_ID[swapped_id] = item


def get_patchify_item(device_id: str) -> Optional[dict]:
    """Return the raw patchify dict for a device_id, or None."""
    _load_patchify_index()
    return _PATCHIFY_BY_DEVICE_ID.get(device_id)


# ---------------------------------------------------------------------------
# Mapping tables: patchify values → SignalCanvas extraction schema values
# ---------------------------------------------------------------------------

_CONNECTOR_MAP: dict[str, str] = {
    "xlr": "XLR",
    "xlr-3": "XLR",
    "trs": "TRS_14",
    "trs-mini": "TRS_38",
    "ts": "TS",
    "rca": "RCA",
    "bnc": "BNC_75",
    "rj45": "RJ45",
    "ethercon": "Ethercon",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "dvi": "DVI",
    "vga": "VGA",
    "usb-a": "USB_A",
    "usb-b": "USB_B",
    "usb-c": "USB_C",
    "usb-mini": "USB_mini",
    "midi-din": "MIDI_DIN",
    "db9": "DB9",
    "db15": "DB15",
    "db25": "DB25",
    "db37": "DB37",
    "speakon": "Speakon",
    "sfp": "SFP",
    "lc": "LC",
    "fiber": "Fiber",
    "iec": "IEC",
    "trueone": "True1",
    "lemo": "Lemo",
    "phoenix": "Phoenix",
    "socapex": "Socapex",
    "triax": "Triax",
    "other": "Unknown",
    "": "Unknown",
}

_SIGNAL_MAP: dict[str, str] = {
    "analog-audio": "Analogue",
    "aes3": "AES3",
    "aes67": "AES67",
    "dante": "Dante",
    "ethernet": "Ethernet",
    "sdi": "SDI",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "dvi": "DVI",
    "vga": "VGA",
    "usb": "USB",
    "midi": "MIDI",
    "ltc": "LTC",
    "wordclock": "WordClock",
    "genlock": "Genlock",
    "madi": "MADI",
    "ndi": "NDI",
    "dmx": "DMX",
    "spdif": "S_PDIF",
    "rs232": "RS232",
    "rs422": "RS422",
    "gpio": "GPIO",
    "hdbaset": "HDBaseT",
    "power": "Power",
    "other": "Unknown",
    "": "Unknown",
}

_TYPE_TO_SIGNAL: dict[str, str] = {
    "audio": "Analogue",
    "video": "SDI",
    "ethernet": "Ethernet",
    "usb": "USB",
    "hdmi": "HDMI",
    "displayport": "DisplayPort",
    "sdi": "SDI",
    "dante": "Dante",
    "aes": "AES3",
    "midi": "MIDI",
    "dmx": "DMX",
    "ltc": "LTC",
    "wordclock": "WordClock",
    "genlock": "Genlock",
    "ndi": "NDI",
    "hdbaset": "HDBaseT",
    "intercom": "Intercom",
    "power": "Power",
    "rs232": "RS232",
    "smpte": "SMPTE",
    "speakon": "Analogue",
    "trueone": "Power",
    "trs": "Analogue",
    "ts": "Analogue",
    "xlr": "Analogue",
    "other": "Unknown",
}


def _map_connector(raw: str) -> str:
    return _CONNECTOR_MAP.get(raw.lower().strip(), "Unknown")


def _map_signal(raw_signal: str, raw_type: str) -> str:
    signal = _SIGNAL_MAP.get(raw_signal.lower().strip(), "")
    if signal and signal != "Unknown":
        return signal
    # Fall back to type-derived signal
    type_signal = _TYPE_TO_SIGNAL.get(raw_type.lower().strip(), "Unknown")
    if type_signal != "Unknown":
        return type_signal
    return "Unknown"


def _sanitize_name(label: str) -> str:
    """Convert a patchify label to a valid port name."""
    # Remove/replace characters that would break PatchLang identifiers
    cleaned = label.strip()
    # Replace common separators with underscore
    for ch in " /\\-().,":
        cleaned = cleaned.replace(ch, "_")
    # Collapse multiple underscores
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    # Remove leading/trailing underscores
    cleaned = cleaned.strip("_")
    # If empty after sanitization, use "Port"
    if not cleaned:
        cleaned = "Port"
    return cleaned


def _deduplicate_names(names: list[str]) -> list[str]:
    """Append _2, _3 etc. to duplicate names."""
    seen: dict[str, int] = {}
    result: list[str] = []
    for name in names:
        if name in seen:
            seen[name] += 1
            result.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 1
            result.append(name)
    return result


def extract_specs_from_patchify(device_id: str) -> Optional[dict]:
    """Build an extraction-schema dict from patchify inputs/outputs.

    Returns None if no patchify entry exists or it has zero ports.
    """
    item = get_patchify_item(device_id)
    if not item:
        return None

    inputs = item.get("inputs") or []
    outputs = item.get("outputs") or []

    if not inputs and not outputs:
        return None

    mfg = item.get("manufacturer", "")
    name = item.get("name", "")
    category = item.get("category", "")

    # Heuristic: when model is empty, name is the product name
    # When manufacturer is clearly wrong (e.g. "Lav K33 RX"), swap them
    model_number = name
    manufacturer = mfg

    # Detect swapped manufacturer/name
    _WELL_KNOWN_MFGS = {
        "sony", "shure", "sennheiser", "audio-technica", "akg",
        "rode", "yamaha", "mackie", "behringer", "avid", "blackmagic design",
        "blackmagic", "zoom", "tascam", "canon", "panasonic", "jbl",
        "netgear", "apple", "dell", "samsung", "barco", "extron",
        "crestron", "cisco", "ubiquiti", "lenovo", "hp",
    }
    name_lower = name.lower()
    mfg_lower = mfg.lower()
    if name_lower in _WELL_KNOWN_MFGS and mfg_lower not in _WELL_KNOWN_MFGS:
        # Swapped: name is actually the manufacturer
        manufacturer = name
        model_number = mfg

    ports: list[dict] = []
    raw_labels: list[str] = []

    for inp in inputs:
        label = inp.get("label", "In")
        direction = "in"
        if inp.get("bidirectional"):
            direction = "io"
        connector = _map_connector(inp.get("connector", ""))
        signal = _map_signal(inp.get("signal", ""), inp.get("type", ""))
        raw_labels.append(label)
        ports.append({
            "name": label,  # Will be deduplicated below
            "direction": direction,
            "connector": connector,
            "channels": 1,
            "attributes": [signal] if signal != "Unknown" else [],
        })

    for out in outputs:
        label = out.get("label", "Out")
        direction = "out"
        if out.get("bidirectional"):
            direction = "io"
        connector = _map_connector(out.get("connector", ""))
        signal = _map_signal(out.get("signal", ""), out.get("type", ""))
        raw_labels.append(label)
        ports.append({
            "name": label,
            "direction": direction,
            "connector": connector,
            "channels": 1,
            "attributes": [signal] if signal != "Unknown" else [],
        })

    # Deduplicate port names
    deduped = _deduplicate_names([_sanitize_name(lbl) for lbl in raw_labels])
    for port, new_name in zip(ports, deduped):
        port["name"] = new_name

    # Build bridges for simple pass-through devices (1-in → 1-out)
    bridges: list[str] = []
    in_ports = [p for p in ports if p["direction"] in ("in", "io")]
    out_ports = [p for p in ports if p["direction"] in ("out", "io")]
    if len(in_ports) == 1 and len(out_ports) == 1 and len(ports) == 2:
        bridges.append(f"{in_ports[0]['name']} -> {out_ports[0]['name']}")

    return {
        "device_metadata": {
            "manufacturer": manufacturer,
            "model_number": model_number,
            "label": f"{manufacturer} {model_number}".strip(),
            "device_type": "device",
            "category": category.capitalize() if category else "Device",
        },
        "signal_flow": {
            "ports": ports,
            "bridges": bridges,
        },
        "power_specs": {
            "power_draw_w": item.get("powerConsumption"),
            "voltage": None,
            "thermal_btuh": None,
            "poe_budget_w": None,
            "poe_draw_w": None,
        },
        "physical_specs": {
            "height_mm": None,
            "width_mm": None,
            "depth_mm": item.get("depth"),
            "weight_kg": item.get("weight"),
        },
        "extraction_confidence": "high",
        "notes": (
            "Signal flow extracted directly from patchify community entry "
            f"(id={item.get('id')}). {len(inputs)} input(s), {len(outputs)} output(s) defined by user."
        ),
    }
