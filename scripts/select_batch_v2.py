#!/usr/bin/env python3
"""Select a diverse batch of 40 EasySchematic-only devices."""

import json
import random
import re
import sqlite3
import os
from collections import Counter
from pathlib import Path

# Load unmatched devices
with open("output/easyschematic/unmatched_devices.json") as f:
    unmatched = json.load(f)

# Collect processed and library devices
processed = set()
for db_file in os.listdir("output"):
    if not db_file.endswith(".db"):
        continue
    try:
        conn = sqlite3.connect(f"output/{db_file}")
        cur = conn.cursor()
        cur.execute("SELECT device_id FROM device_nodes")
        for row in cur.fetchall():
            processed.add(row[0])
        conn.close()
    except Exception:
        pass

library_path = Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches")
already_in_lib = set()
for root, dirs, files in os.walk(library_path):
    for f in files:
        if f.endswith(".patch"):
            already_in_lib.add(f.replace(".patch", ""))


def slugify(text):
    text = text.lower().strip()
    text = text.replace("&", "and")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", "-", text)
    text = text.strip("-")
    return text


def make_device_id(mfg, model):
    mfg_slug = slugify(mfg)
    model_slug = slugify(model)
    if model_slug.startswith(mfg_slug + "-") or model_slug == mfg_slug:
        return model_slug[:64]
    return f"{mfg_slug}-{model_slug}"[:64]


# Known IT/networking out-of-scope patterns
IT_PATTERNS = ["switch", "router", "access point", "ap ", "poe", "sfp", " aggregator", "aggregation"]


def is_likely_it(model, mfg):
    lower = model.lower()
    for p in IT_PATTERNS:
        if p in lower:
            # Allow codecs, cameras, desks
            if "codec" in lower or "camera" in lower or "desk" in lower:
                continue
            return True
    if mfg.lower() == "ubiquiti" and ("usw" in lower or ("unifi" in lower and "switch" in lower)):
        return True
    return False


candidates = []
for d in unmatched:
    mfg = d.get("manufacturer", "").strip()
    label = d.get("label", "").strip()

    if not mfg or mfg.lower() in ("unknown", "generic"):
        continue

    if is_likely_it(label, mfg):
        continue

    device_id = make_device_id(mfg, label)

    if device_id not in processed and device_id not in already_in_lib:
        candidates.append({**d, "_device_id": device_id})

print(f"Clean candidates after IT filter: {len(candidates)}")

# Diversity sampling
random.seed(44)
random.shuffle(candidates)

selected = []
mfg_counts = Counter()
MAX_PER_MFG = 3

for d in candidates:
    mfg = d.get("manufacturer", "Unknown")
    if mfg_counts[mfg] >= MAX_PER_MFG:
        continue
    selected.append(d)
    mfg_counts[mfg] += 1
    if len(selected) >= 40:
        break

print(f"\nSelected {len(selected)} devices from {len(mfg_counts)} manufacturers")
print("\nManufacturer distribution:")
for mfg, count in mfg_counts.most_common():
    print(f"  {mfg}: {count}")

# Write batch file
with open("batch_40_es_only_v2.txt", "w") as f:
    for d in selected:
        f.write(f"{d['manufacturer']}|{d['label']}|{d['_device_id']}\n")

print("\nWrote batch_40_es_only_v2.txt")
