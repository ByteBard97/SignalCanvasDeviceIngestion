#!/usr/bin/env python3
"""Select N D-tier devices from the library for pipeline processing.

Reads:
    SignalCanvasDeviceLibrary/patches/   — source of D-tier devices
    SignalCanvasDeviceLibrary/excluded.csv — denylist (never selected)
    SignalCanvasDeviceIngestion/output/*/manifest.db — already-processed devices

Writes:
    <output-file>  (pipe-separated: manufacturer|model|device_id)

Usage:
    .venv/bin/python scripts/select_devices.py --n 20 --out my_batch.txt
    .venv/bin/python scripts/select_devices.py --n 20 --prioritize --out my_batch.txt
    .venv/bin/python scripts/select_devices.py --n 20 --prioritize --dry-run
"""

from __future__ import annotations

import argparse
import csv
import random
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
LIBRARY_ROOT = REPO_ROOT.parent / "SignalCanvasDeviceLibrary"
PATCHES_DIR = LIBRARY_ROOT / "patches"
EXCLUDED_CSV = LIBRARY_ROOT / "excluded.csv"
OUTPUT_DIR = REPO_ROOT / "output"

VALID_REASON_CODES = {"CIT", "OOS", "DUP", "DSC", "NDS"}

# Manufacturers known to produce core professional AV gear (+2 score each)
PRIORITY_MANUFACTURERS: set[str] = {
    "yamaha", "shure", "sennheiser", "qsc", "allen-heath", "allen_heath",
    "focusrite", "blackmagic-design", "blackmagic_design", "digico", "avid",
    "soundcraft", "behringer", "presonus", "midas", "ssl", "neve",
    "audinate", "luminex", "dante", "biamp", "bss", "crown", "lab-gruppen",
    "lab_gruppen", "powersoft", "meyer-sound", "meyer_sound", "d-b",
    "clear-com", "clear_com", "riedel", "rts", "green-go", "green_go",
    "ross-video", "ross_video", "sony", "panasonic", "jvc", "canon",
    "aja", "blackmagic", "atomos", "decimator", "magewell", "teradek",
    "vizio", "ndi", "vizrt", "newtek", "grass-valley", "grass_valley",
    "nevion", "harmonic", "generic-dante", "extron", "crestron", "kramer",
    "atlona", "lightware", "barco", "christie", "nec", "epson",
}

# Signal types that indicate a professional AV protocol (+1 score per unique hit)
PRIORITY_SIGNALS: set[str] = {
    "dante", "madi", "sdi", "aes67", "aes70", "oca", "ndi", "avb", "milan",
    "aes3", "ravenna", "ember", "nmos", "qsys", "q-sys", "hdbaset",
    "displayport", "genlock", "wordclock", "timecode", "ltc", "vitc",
    "rs422", "rs232",
}

_META_FIELD_RE = re.compile(r'(\w+):\s*"([^"]*)"')


def _parse_meta(patch_text: str) -> dict[str, str]:
    meta: dict[str, str] = {}
    in_meta = False
    for line in patch_text.splitlines():
        stripped = line.strip()
        if stripped == "meta {":
            in_meta = True
            continue
        if in_meta:
            if stripped == "}":
                break
            m = _META_FIELD_RE.match(stripped)
            if m:
                meta[m.group(1)] = m.group(2)
    return meta


def load_excluded(excluded_csv: Path) -> set[str]:
    excluded: set[str] = set()
    if not excluded_csv.exists():
        return excluded
    with excluded_csv.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row.get("reason", "").strip()
            if code and code not in VALID_REASON_CODES:
                print(f"Warning: unknown reason code '{code}' for {row.get('device_id')} — skipping row", file=sys.stderr)
                continue
            device_id = row.get("device_id", "").strip()
            if device_id:
                excluded.add(device_id)
    return excluded


def load_processed(output_dir: Path) -> set[str]:
    """Return device_ids that have reached queue=5 in any manifest DB."""
    processed: set[str] = set()
    for db_path in output_dir.rglob("manifest.db"):
        try:
            conn = sqlite3.connect(db_path)
            c = conn.cursor()
            c.execute("SELECT device_id FROM device_nodes WHERE queue = 5")
            for (did,) in c.fetchall():
                processed.add(did)
            conn.close()
        except Exception:
            pass
    return processed


def _priority_score(patch_text: str, mfg_slug: str) -> int:
    """Score a device for triage prioritization.

    +2  manufacturer is in the known pro-AV tier
    +1  per unique professional signal type found in port attributes
    """
    score = 0
    if mfg_slug.lower() in PRIORITY_MANUFACTURERS:
        score += 2
    found_signals: set[str] = set()
    for attr_block in re.findall(r'\[([^\]]+)\]', patch_text):
        for token in attr_block.split(","):
            t = token.strip().lower()
            if t in PRIORITY_SIGNALS:
                found_signals.add(t)
    score += len(found_signals)
    return score


def load_d_tier_devices(patches_dir: Path) -> list[dict]:
    """Return all D-tier devices from patches/ as list of dicts."""
    devices = []
    for patch_file in patches_dir.rglob("*.patch"):
        text = patch_file.read_text(errors="replace")
        meta = _parse_meta(text)
        if meta.get("quality") != "D":
            continue
        manufacturer = meta.get("manufacturer", "")
        model = meta.get("model", "")
        mfg_slug = patch_file.parent.name
        if mfg_slug == "_uncategorized":
            continue
        device_id = patch_file.stem
        devices.append({
            "device_id": device_id,
            "manufacturer": manufacturer,
            "model": model,
            "mfg_slug": mfg_slug,
            "priority_score": _priority_score(text, mfg_slug),
        })
    return devices


def select(
    devices: list[dict],
    excluded: set[str],
    processed: set[str],
    n: int,
    max_per_mfg: int | None,
    seed: int | None,
    prioritize: bool = False,
) -> list[dict]:
    rng = random.Random(seed)

    pool = [d for d in devices if d["device_id"] not in excluded and d["device_id"] not in processed]

    if prioritize:
        # Sort by score descending, break ties randomly
        rng.shuffle(pool)
        pool.sort(key=lambda d: d.get("priority_score", 0), reverse=True)
    else:
        rng.shuffle(pool)

    selected = []
    mfg_counts: Counter = Counter()

    for device in pool:
        if len(selected) >= n:
            break
        mfg = device["mfg_slug"]
        if max_per_mfg is not None and mfg_counts[mfg] >= max_per_mfg:
            continue
        selected.append(device)
        mfg_counts[mfg] += 1

    return selected


def main() -> int:
    parser = argparse.ArgumentParser(description="Select D-tier devices for pipeline processing")
    parser.add_argument("--n", type=int, default=200, help="Number of devices to select")
    parser.add_argument("--max-per-mfg", type=int, default=None, help="Max devices per manufacturer")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument("--out", type=Path, default=None, help="Output batch file path")
    parser.add_argument("--dry-run", action="store_true", help="Print selection without writing")
    parser.add_argument("--prioritize", action="store_true", help="Score and rank by pro-AV manufacturer tier and protocol presence")
    args = parser.parse_args()

    if not PATCHES_DIR.exists():
        print(f"Error: patches/ not found at {PATCHES_DIR}", file=sys.stderr)
        return 1

    print("Loading excluded devices...")
    excluded = load_excluded(EXCLUDED_CSV)
    print(f"  {len(excluded)} excluded")

    print("Loading already-processed devices...")
    processed = load_processed(OUTPUT_DIR)
    print(f"  {len(processed)} already processed")

    print("Loading D-tier devices from library...")
    devices = load_d_tier_devices(PATCHES_DIR)
    print(f"  {len(devices)} D-tier devices found")

    eligible = len([d for d in devices if d["device_id"] not in excluded and d["device_id"] not in processed])
    print(f"  {eligible} eligible for selection")

    selected = select(devices, excluded, processed, args.n, args.max_per_mfg, args.seed, args.prioritize)
    print(f"\nSelected {len(selected)} devices")

    if args.dry_run:
        for d in selected:
            score = d.get("priority_score", 0)
            score_str = f"  [score={score}]" if args.prioritize else ""
            print(f"  {d['device_id']}  ({d['manufacturer']} {d['model']}){score_str}")
        return 0

    if args.out is None:
        print("Error: --out required unless --dry-run", file=sys.stderr)
        return 1

    with args.out.open("w") as f:
        for d in selected:
            f.write(f"{d['manufacturer']}|{d['model']}|{d['device_id']}\n")

    print(f"Wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
