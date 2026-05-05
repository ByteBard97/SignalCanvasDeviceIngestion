#!/usr/bin/env python3
"""Import device templates from EasySchematic's public API.

EasySchematic (easyschematic.live) exposes its full device library via a
public REST API at https://api.easyschematic.live/templates

The API returns 2,000+ structured device templates with ports, signal types,
directions, and connectors. This script:
1. Fetches the full template library from the API
2. Converts EasySchematic port definitions to PatchLang format
3. Cross-references with the patchify dataset
4. Outputs matched devices that can skip the PDF pipeline

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/easyschematic_importer.py
"""

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import httpx

OUTPUT_DIR = Path("output/easyschematic")
PATCHIFY_PATH = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
API_URL = "https://api.easyschematic.live/templates"

# Map EasySchematic signal types to PatchLang signal types
_SIGNAL_MAP = {
    "analog-audio": "analog",
    "aes": "aes",
    "aes3": "aes",
    "dante": "dante",
    "madi": "madi",
    "sdi": "sdi",
    "hdmi": "hdmi",
    "displayport": "displayport",
    "usb": "usb",
    "network": "network",
    "ethernet": "network",
    "power": "power",
    "spdif": "spdif",
    "optical-audio": "toslink",
    "midi": "midi",
    "dmx": "dmx",
    "artnet": "artnet",
    "sacn": "sacn",
    "ndi": "ndi",
    "hdbase-t": "hdBaseT",
    "fiber": "fiber",
    "coax": "coax",
    "bnc": "bnc",
    "xlr": "xlr",
    "xlr-3": "xlr",
    "xlr-5": "xlr",
    "trs": "trs",
    "ts": "ts",
    "rca": "rca",
    "speakon": "speakon",
    "ethercon": "ethercon",
    "rj45": "rj45",
    "vga": "vga",
    "composite": "composite",
    "component": "component",
    "dvi": "dvi",
    "thunderbolt": "thunderbolt",
    "firewire": "firewire",
    "euroblock": "euroblock",
    "terminal-block": "terminal",
    "sfp": "sfp",
    "sfp+": "sfp",
    "qsfp": "qsfp",
    "phoenix": "phoenix",
    "wireless": "wireless",
    "antenna": "antenna",
    "ir": "ir",
    "rs232": "rs232",
    "rs422": "rs422",
    "rs485": "rs485",
    "gpio": "gpio",
    "relay": "relay",
    "contact-closure": "contact",
    "dmx512": "dmx",
    "optical": "optical",
    "dual-fiber": "fiber",
    "cat6": "rj45",
    "rj11": "rj11",
    "3.5mm": "trs",
    "mini-jack": "trs",
    "speakon-nl4": "speakon",
    "none": None,
}


def _map_signal_type(es_type: str) -> str | None:
    """Map EasySchematic signal type to PatchLang signal type."""
    key = es_type.lower().strip() if es_type else ""
    return _SIGNAL_MAP.get(key, key)


def _to_patchlang(device: dict) -> str:
    """Convert an EasySchematic device to PatchLang format."""
    mfg = device.get("manufacturer", "Unknown")
    model = device.get("label", "Unknown")
    safe_id = f"{mfg}_{model}".lower().replace(" ", "_").replace("/", "_").replace("-", "_")
    safe_id = "".join(c for c in safe_id if c.isalnum() or c == "_")

    lines = [
        f"# Generated from EasySchematic API: {mfg} {model}",
        f"# Source: https://easyschematic.live",
        f"# API: https://api.easyschematic.live/templates",
        "",
        f"device {safe_id}",
        f"  class {device.get('deviceType', 'unknown')}",
        f"  label \"{model}\"",
    ]

    lines.append("")

    for port in device.get("ports", []):
        dir_map = {
            "input": "input",
            "output": "output",
            "bidirectional": "bidirectional",
        }
        dir_kw = dir_map.get(port.get("direction", ""), "unknown")
        sig = _map_signal_type(port.get("signalType", ""))
        if not sig:
            continue
        label = port.get("label", "")
        conn = port.get("connectorType", "")
        conn_str = f" {conn}" if conn and conn != "none" else ""
        lines.append(f"  {dir_kw} {sig} {label}{conn_str}")

    lines.append("")
    lines.append("end")
    return "\n".join(lines)


def _match_to_patchify(easy_devices: list[dict]) -> tuple[list[dict], list[dict]]:
    """Cross-reference EasySchematic devices with patchify dataset.

    Matching is strict to avoid false positives. We require:
    1. Manufacturer name must match exactly (case-insensitive)
    2. Model names must have substantial word overlap
    """
    if not PATCHIFY_PATH.exists():
        return [], easy_devices

    with open(PATCHIFY_PATH) as f:
        patchify = json.load(f)

    matched = []
    unmatched = []

    for ed in easy_devices:
        ed_mfg = ed.get("manufacturer", "").lower().strip()
        ed_label = ed.get("label", "").lower().strip()
        ed_model_num = ed.get("modelNumber", "").lower().strip()

        # Skip generic/unknown manufacturers
        if ed_mfg in ("", "generic", "unknown"):
            unmatched.append(ed)
            continue

        best_match = None
        best_score = 0

        for p_device in patchify:
            p_mfg = p_device.get("manufacturer", "").lower().strip()
            p_name = p_device.get("name", "").lower().strip()

            # Manufacturer must match
            if p_mfg != ed_mfg:
                continue

            # Calculate overlap score
            score = 0

            # Exact match is best
            if p_name == ed_label or p_name == ed_model_num:
                score = 100
            else:
                # Word-based overlap
                ed_words = set(ed_label.split()) | set(ed_model_num.split())
                p_words = set(p_name.split())
                common = ed_words & p_words
                # Require at least 2 meaningful words in common, or one very specific word
                if len(common) >= 2:
                    score = len(common) * 10
                elif len(common) == 1:
                    word = list(common)[0]
                    # Single-word match only counts if it's a substantial word (not "the", "a", etc.)
                    if len(word) >= 3 and word not in ("the", "and", "for", "with", "audio", "video", "digital", "channel"):
                        score = 5

                # Substring containment (substantial)
                if ed_label in p_name or p_name in ed_label:
                    score = max(score, 15)
                if ed_model_num and (ed_model_num in p_name or p_name in ed_model_num):
                    score = max(score, 20)

            if score > best_score:
                best_score = score
                best_match = p_device

        # Threshold: require a meaningful match
        if best_match and best_score >= 15:
            matched.append({**ed, "patchify_match": best_match, "match_score": best_score})
        else:
            unmatched.append(ed)

    return matched, unmatched


def main() -> int:
    print("Fetching EasySchematic template library from API...")

    resp = httpx.get(API_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=120)
    resp.raise_for_status()
    devices = resp.json()

    print(f"Total templates: {len(devices)}")

    # Quick stats
    mfg_counts = Counter(d.get("manufacturer", "Unknown") for d in devices)
    cat_counts = Counter(d.get("category", "Unknown") for d in devices)

    print(f"\nTop manufacturers:")
    for mfg, count in mfg_counts.most_common(15):
        print(f"  {mfg}: {count}")

    print(f"\nTop categories:")
    for cat, count in cat_counts.most_common(10):
        print(f"  {cat}: {count}")

    # Cross-reference with patchify
    print("\nCross-referencing with patchify dataset...")
    matched, unmatched = _match_to_patchify(devices)
    print(f"  Matched: {len(matched)}")
    print(f"  Unmatched: {len(unmatched)}")

    # Save outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_DIR / "all_devices.json", "w") as f:
        json.dump(
            {
                "source": "easyschematic_api",
                "api_url": API_URL,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "total_templates": len(devices),
                "matched_count": len(matched),
                "unmatched_count": len(unmatched),
                "manufacturer_breakdown": dict(mfg_counts.most_common()),
                "category_breakdown": dict(cat_counts.most_common()),
                "devices": devices,
            },
            f,
            indent=2,
        )

    with open(OUTPUT_DIR / "matched_devices.json", "w") as f:
        json.dump(matched, f, indent=2)

    with open(OUTPUT_DIR / "unmatched_devices.json", "w") as f:
        json.dump(unmatched, f, indent=2)

    # Generate PatchLang files for matched devices
    patchlang_dir = OUTPUT_DIR / "patchlang"
    patchlang_dir.mkdir(exist_ok=True)

    # Clear old files
    for f in patchlang_dir.glob("*.patch"):
        f.unlink()

    for device in matched:
        safe_name = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in f"{device['manufacturer']}_{device['label']}"
        ).lower()
        patchlang_path = patchlang_dir / f"{safe_name}.patch"
        with open(patchlang_path, "w") as f:
            f.write(_to_patchlang(device))

    print(f"\nSaved {len(devices)} templates to {OUTPUT_DIR}/")
    print(f"Generated {len(matched)} PatchLang files in {patchlang_dir}/")

    print("\nSample matched devices:")
    for d in matched[:15]:
        ports = len(d.get("ports", []))
        print(f"  {d['manufacturer']} {d['label']} ({d['deviceType']}) — {ports} ports")

    print("\nSample PatchLang output:")
    if matched:
        print(_to_patchlang(matched[0]))

    return 0


if __name__ == "__main__":
    sys.exit(main())
