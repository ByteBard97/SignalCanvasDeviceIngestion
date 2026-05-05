#!/usr/bin/env python3
"""Run a random batch of patchify devices through the full pipeline.

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/run_batch.py --count 10
"""

import argparse
import json
import random
import sys
from pathlib import Path

from src.harness.manifest import Manifest
from src.runner import run_pipeline

PATCHIFY_PATH = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
MANIFEST_DB = Path("output/ingestion.db")
CACHE_DIR = Path("output")


def _get_processed_device_ids() -> set[str]:
    """Get all device IDs already in the manifest."""
    if not MANIFEST_DB.exists():
        return set()
    manifest = Manifest(MANIFEST_DB)
    # List all nodes
    import sqlite3
    conn = sqlite3.connect(MANIFEST_DB)
    c = conn.cursor()
    c.execute("SELECT device_id FROM device_nodes")
    return set(row[0] for row in c.fetchall())


def _generate_device_id(manufacturer: str, model: str) -> str:
    """Generate a slug-style device ID from manufacturer and model."""
    mfg_slug = manufacturer.lower().replace(" ", "-").replace("_", "-")
    model_slug = model.lower().replace(" ", "-").replace("/", "-").replace("_", "-")
    # Remove repeated dashes and trailing dashes
    model_slug = "-".join(filter(None, model_slug.split("-")))
    # Keep under 64 chars for Ragscallion corpus_id compatibility
    return f"{mfg_slug}-{model_slug}"[:64]


# Manufacturers with proven pipeline success — focus batch testing here
# to surface actual pipeline bugs rather than "device doesn't exist" failures.
_PROVEN_MANUFACTURERS = {
    "allen & heath", "allen_heath", "allen-heath",
    "yamaha", "audinate", "qsc", "behringer", "blackmagic design",
    "zoom", "sennheiser", "soundcraft", "focusrite", "midas", "shure",
    "sony", "panasonic", "aja", "barco", "extron", "kramer",
    "biamp", "dbaudiotechnik", "d&b audiotechnik", "l-acoustics",
    "martin audio", "nexo", "jbl", "meyer sound", "adamson",
    "digico", "ssl", "neve", "rupert neve", "api", "universal audio",
    "apogee", "antelope", "rme", "motu", "presonus", "tascam",
    "roland", "korg", "kurzweil", "nord", "moog", "arturia",
    "ableton", "avid", "pro tools", "steinberg", "waves",
    "bose", " Electro-Voice", "ev", "jbl", "qsc", "rcf",
    "dante", "dante-enabled",  # some Audinate products
    "canon", "fujinon", "angenieux", "zeiss", "leica",
    "red digital cinema", "arri", "blackmagic", "panasonic", "sony",
    "tokina", "sigma", "tamron",
    "rode", "sennheiser", "audio-technica", "shure", "akg", "beyerdynamic",
    "neumann", "schoeps", "dpa", "earthworks", "audix", "electro-voice",
    "radial", "whirlwind", "rapco", "pro co", "hosa",
    "furman", "tripp lite", "apc", "liebert",
    "middle atlantic", "chief", "peerless", "omnimount",
    "crestron", "amx", "control4", "savant", "rti", "control",
    "extron", "kramer", "tvone", "coriomaster", "magewell",
    "lightware", "analog way", "spyder", "christie", "barco",
    "panasonic", "sony", "nec", "lg", "samsung", "christie", "christiedigital",
    "epson", "benq", "optoma", "viewsonic", "christie", "digital projection",
    "absen", "roeco", "unilumin", "leyard", "planar",
    "grandma", "ma lighting", "chamsys", "avolites", "hog", "etc",
    "cisco", "aruba", "ubiquiti", "meraki", "netgear", "tplink", "tp-link",
    "dell", "hp", "lenovo", "supermicro", "synology", "qnap",
    "snapav", "control4", "pakedge", "luma", "wattbox",
    "wisenet", "hanwha", "axis", "milestone", "avigilon",
    "shure", "sennheiser", "beyerdynamic", "akg", "audio-technica",
}


def _is_good_candidate(mfg: str, model: str) -> bool:
    """Filter out devices that are unlikely to have findable datasheets."""
    if not mfg or not model:
        return False

    mfg_lower = mfg.lower().strip()
    model_lower = model.lower().strip()

    # Only test proven manufacturers for now — this surfaces pipeline bugs
    # rather than "obscure device has no datasheet" failures.
    if mfg_lower not in _PROVEN_MANUFACTURERS:
        return False

    # Require ASCII-only
    try:
        mfg.encode("ascii")
        model.encode("ascii")
    except UnicodeEncodeError:
        return False

    # Skip very short or very long names
    if len(model) < 3 or len(model) > 80:
        return False

    # Skip power supplies, cables, adapters
    skip_keywords = {"power supply", "cable", "adapter", "dongle", "charger",
                     "battery", "case", "bag", "mount", "bracket", "stand",
                     "tripod", "cart", "rack shelf", "rack ear", "faceplate",
                     "wall plate", "keystone", "patch panel", "cable tester"}
    if any(kw in model_lower for kw in skip_keywords):
        return False

    # Skip purely descriptive/positional names (not real product models)
    descriptive_words = {"left", "right", "main", "sub", "center", "surround",
                         "front", "rear", "monitor", "speakers", "mix", "channel",
                         "input", "output", "return", "send", "aux", "group",
                         "foh", "stage", "house", "fill", "delay", "wedge"}
    model_words = set(model_lower.split())
    # If more than half the words are descriptive/positional, skip
    if len(model_words) > 0:
        desc_count = sum(1 for w in model_words if w in descriptive_words)
        if desc_count / len(model_words) >= 0.5:
            return False

    # Require at least one "specific" token — a number or product code.
    # Real model names always have numbers or acronyms (ULXD4, SQ-5, Rio1608-D2).
    # Descriptive names like "Left Main Speakers" don't.
    has_specific_token = False
    import re
    # Look for: numbers, hyphenated codes, all-caps acronyms 2+ chars
    if re.search(r'\d', model):
        has_specific_token = True
    if re.search(r'\b[A-Z]{2,}\b', model):
        has_specific_token = True
    if re.search(r'[a-zA-Z]+-\d', model):
        has_specific_token = True
    if not has_specific_token:
        return False

    return True


def _pick_random_batch(count: int, exclude: set[str]) -> list[tuple[str, str, str]]:
    """Pick N random patchify devices not already processed."""
    with open(PATCHIFY_PATH) as f:
        patchify = json.load(f)

    candidates = []
    for d in patchify:
        mfg = d.get("manufacturer", "")
        model = d.get("name", "")
        device_id = _generate_device_id(mfg, model)
        if device_id not in exclude and _is_good_candidate(mfg, model):
            candidates.append((mfg, model, device_id))

    if len(candidates) < count:
        print(f"Warning: only {len(candidates)} good candidates available")
        count = len(candidates)

    return random.sample(candidates, count)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=10, help="Number of devices to process")
    parser.add_argument("--devices-file", type=Path, default=Path("devices.txt"))
    args = parser.parse_args()

    processed = _get_processed_device_ids()
    print(f"Already in manifest: {len(processed)} devices")

    batch = _pick_random_batch(args.count, processed)
    print(f"\nSelected {len(batch)} random devices:\n")
    for mfg, model, device_id in batch:
        print(f"  {device_id}: {mfg} {model}")

    # Write to devices.txt (append mode)
    with open(args.devices_file, "a") as f:
        for mfg, model, device_id in batch:
            f.write(f"{mfg}|{model}|{device_id}\n")

    print(f"\nAppended to {args.devices_file}")
    print("Starting pipeline...\n")

    import asyncio
    asyncio.run(run_pipeline(args.devices_file, CACHE_DIR, MANIFEST_DB))
    return 0


if __name__ == "__main__":
    sys.exit(main())
