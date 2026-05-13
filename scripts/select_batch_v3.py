#!/usr/bin/env python3
"""Select a diverse batch of ~200 EasySchematic-only devices.

Upgrade from v2:
- Target size: 200 devices (not 40)
- Max per manufacturer: 5 (not 3)
- Random seed: 99
- Checks manifest DBs recursively under output/
- Checks both patches/ and intake/ in SignalCanvasDeviceLibrary
- Reads device_id column from all *.txt batch files (pipe-separated: mfg|label|device_id)
- Writes to batch_200_es_only_v1.txt
"""

import json
import random
import re
import sqlite3
import os
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = REPO_ROOT / "output"
POOL_FILE = OUTPUT_DIR / "easyschematic" / "unmatched_devices.json"
LIBRARY_ROOT = Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary")
PATCHES_DIR = LIBRARY_ROOT / "patches"
INTAKE_DIR = LIBRARY_ROOT / "intake"
OUTPUT_BATCH_FILE = REPO_ROOT / "batch_200_es_only_v1.txt"

TARGET_SIZE = 200
MAX_PER_MFG = 5
RANDOM_SEED = 99


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", "-", text)
    text = text.strip("-")
    return text


def make_device_id(mfg: str, model: str) -> str:
    mfg_slug = slugify(mfg)
    model_slug = slugify(model)
    if model_slug.startswith(mfg_slug + "-") or model_slug == mfg_slug:
        return model_slug[:64]
    return f"{mfg_slug}-{model_slug}"[:64]


# Known IT/networking out-of-scope patterns
IT_PATTERNS = [
    "switch",
    "router",
    "access point",
    "ap ",
    "poe",
    "sfp",
    " aggregator",
    "aggregation",
]


def is_likely_it(model: str, mfg: str) -> bool:
    lower = model.lower()
    for p in IT_PATTERNS:
        if p in lower:
            # Allow codecs, cameras, desks — these are AV even if they mention PoE/SFP
            if "codec" in lower or "camera" in lower or "desk" in lower:
                continue
            return True
    if mfg.lower() == "ubiquiti" and ("usw" in lower or ("unifi" in lower and "switch" in lower)):
        return True
    return False


# ── Collect already-processed device IDs ────────────────────────────────────
# Primary source: output/processed_devices.txt — regenerated after every batch
# by running: python scripts/select_batch_v3.py --update-processed
# Falls back to scanning manifests + batch txts + library if file is missing.

processed: set[str] = set()

PROCESSED_FILE = OUTPUT_DIR / "processed_devices.txt"
if PROCESSED_FILE.exists():
    processed.update(line.strip() for line in PROCESSED_FILE.read_text().splitlines() if line.strip())
    print(f"processed_devices.txt: {len(processed)} device IDs")
else:
    # Fallback: scan all sources
    for db_path in OUTPUT_DIR.rglob("*.db"):
        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()
            cur.execute("SELECT device_id FROM device_nodes")
            processed.update(row[0] for row in cur.fetchall())
            conn.close()
        except Exception:
            pass
    for txt_file in REPO_ROOT.glob("*.txt"):
        try:
            for line in txt_file.read_text().splitlines():
                parts = line.strip().split("|")
                if len(parts) == 3:
                    processed.add(parts[2].strip())
        except Exception:
            pass
    for search_root in (PATCHES_DIR, INTAKE_DIR):
        if search_root.exists():
            for patch_file in search_root.rglob("*.patch"):
                processed.add(patch_file.stem)
    print(f"Scanned sources: {len(processed)} already-processed device IDs")

print(f"\nTotal already-processed: {len(processed)}")

# ── Load pool and filter ──────────────────────────────────────────────────────

with open(POOL_FILE) as f:
    unmatched = json.load(f)

print(f"\nPool size: {len(unmatched)} devices")

candidates = []
for d in unmatched:
    mfg = d.get("manufacturer", "").strip()
    label = d.get("label", "").strip()

    if not mfg or mfg.lower() in ("unknown", "generic"):
        continue

    if is_likely_it(label, mfg):
        continue

    device_id = make_device_id(mfg, label)

    if device_id not in processed:
        candidates.append({**d, "_device_id": device_id})

print(f"Clean candidates after IT filter + exclusions: {len(candidates)}")

# ── Diversity sampling ────────────────────────────────────────────────────────

random.seed(RANDOM_SEED)
random.shuffle(candidates)

selected = []
mfg_counts: Counter = Counter()

for d in candidates:
    mfg = d.get("manufacturer", "Unknown")
    if mfg_counts[mfg] >= MAX_PER_MFG:
        continue
    selected.append(d)
    mfg_counts[mfg] += 1
    if len(selected) >= TARGET_SIZE:
        break

print(f"\nSelected {len(selected)} devices from {len(mfg_counts)} manufacturers")
print("\nManufacturer distribution (top 15):")
for mfg, count in mfg_counts.most_common(15):
    print(f"  {mfg}: {count}")

# ── Write output ──────────────────────────────────────────────────────────────

with open(OUTPUT_BATCH_FILE, "w") as f:
    for d in selected:
        f.write(f"{d['manufacturer']}|{d['label']}|{d['_device_id']}\n")

print(f"\nWrote {OUTPUT_BATCH_FILE}")
print("\nFirst 10 lines:")
with open(OUTPUT_BATCH_FILE) as f:
    for i, line in enumerate(f):
        if i >= 10:
            break
        print(f"  {line}", end="")
