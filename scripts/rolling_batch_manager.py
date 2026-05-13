#!/usr/bin/env python3
"""Rolling batch manager — when devices complete or are correctly rejected,
automatically add new random devices from the unprocessed pool.

Usage:
    .venv/bin/python scripts/rolling_batch_manager.py \
        --batch-file batch_40_es_only_v2.txt \
        --manifest output/batch_40_es_only_v2/manifest.db \
        --target-size 40
"""

import argparse
import json
import os
import random
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from harness.manifest import Manifest, QUEUE_4_MANUAL_REVIEW, QUEUE_5_COMPLETED

BLACKLIST_PATH = Path("output/blacklisted_devices.txt")
EASYSCHEMATIC_PATH = Path("output/easyschematic/unmatched_devices.json")


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


def load_blacklist() -> set[str]:
    if not BLACKLIST_PATH.exists():
        return set()
    return {line.strip() for line in BLACKLIST_PATH.read_text().splitlines() if line.strip()}


def add_to_blacklist(device_id: str, reason: str) -> None:
    BLACKLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(BLACKLIST_PATH, "a") as f:
        f.write(f"{device_id}|{reason}\n")


def load_easyschematic_pool() -> list[dict]:
    with open(EASYSCHEMATIC_PATH) as f:
        return json.load(f)


def get_processed_ids(manifest_db: Path) -> set[str]:
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
    return processed


IT_PATTERNS = ["switch", "router", "access point", "ap ", "poe", "sfp", " aggregator", "aggregation"]


def is_likely_it(model: str, mfg: str) -> bool:
    lower = model.lower()
    for p in IT_PATTERNS:
        if p in lower:
            if "codec" in lower or "camera" in lower or "desk" in lower:
                continue
            return True
    if mfg.lower() == "ubiquiti" and ("usw" in lower or ("unifi" in lower and "switch" in lower)):
        return True
    return False


def select_replacements(
    count: int,
    manifest_db: Path,
    exclude_mfgs: dict[str, int] | None = None,
) -> list[dict]:
    blacklist = load_blacklist()
    processed = get_processed_ids(manifest_db)
    pool = load_easyschematic_pool()

    candidates = []
    for d in pool:
        mfg = d.get("manufacturer", "").strip()
        label = d.get("label", "").strip()
        if not mfg or mfg.lower() in ("unknown", "generic"):
            continue
        if is_likely_it(label, mfg):
            continue
        device_id = make_device_id(mfg, label)
        if device_id in processed or device_id in blacklist:
            continue
        candidates.append(d)

    random.seed(int(datetime.now(timezone.utc).timestamp()))
    random.shuffle(candidates)

    selected = []
    mfg_counts = Counter(exclude_mfgs or {})
    MAX_PER_MFG = 3

    for d in candidates:
        mfg = d.get("manufacturer", "Unknown")
        if mfg_counts[mfg] >= MAX_PER_MFG:
            continue
        device_id = make_device_id(mfg, d["label"])
        selected.append({
            "manufacturer": mfg,
            "label": d["label"],
            "device_id": device_id,
        })
        mfg_counts[mfg] += 1
        if len(selected) >= count:
            break

    return selected


def get_manifest_summary(manifest_db: Path) -> dict:
    conn = sqlite3.connect(manifest_db)
    cur = conn.cursor()
    cur.execute("""
        SELECT
            SUM(CASE WHEN queue = 5 THEN 1 ELSE 0 END) as completed,
            SUM(CASE WHEN queue = 4 AND failure_category = 'OUT_OF_SCOPE' THEN 1 ELSE 0 END) as oos,
            SUM(CASE WHEN queue = 4 AND failure_category != 'OUT_OF_SCOPE' THEN 1 ELSE 0 END) as other_failed,
            SUM(CASE WHEN queue = 0 THEN 1 ELSE 0 END) as initial,
            SUM(CASE WHEN queue = 2 THEN 1 ELSE 0 END) as polling,
            SUM(CASE WHEN queue = 3 THEN 1 ELSE 0 END) as ready,
            COUNT(*) as total
        FROM device_nodes
    """)
    row = cur.fetchone()
    conn.close()
    return {
        "completed": row[0] or 0,
        "out_of_scope": row[1] or 0,
        "other_failed": row[2] or 0,
        "initial": row[3] or 0,
        "polling": row[4] or 0,
        "ready": row[5] or 0,
        "total": row[6] or 0,
    }


def update_batch_file(batch_file: Path, new_devices: list[dict]) -> None:
    with open(batch_file, "a") as f:
        for d in new_devices:
            f.write(f"{d['manufacturer']}|{d['label']}|{d['device_id']}\n")


def add_devices_to_manifest(manifest_db: Path, devices: list[dict]) -> None:
    manifest = Manifest(manifest_db)
    for d in devices:
        from harness.manifest import DeviceNode
        node = DeviceNode(
            device_id=d["device_id"],
            manufacturer=d["manufacturer"],
            model=d["label"],
            created_at=datetime.now(timezone.utc).isoformat(),
            updated_at=datetime.now(timezone.utc).isoformat(),
        )
        manifest.add_node(node)
        print(f"  + {d['device_id']}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-file", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--target-size", type=int, default=40)
    args = parser.parse_args()

    summary = get_manifest_summary(args.manifest)
    print(f"Manifest summary: {summary}")

    # Count how many replacements we need:
    # completed + correctly rejected (OUT_OF_SCOPE)
    finished = summary["completed"] + summary["out_of_scope"]
    needed = args.target_size - (summary["total"] - summary["initial"])

    if needed <= 0:
        print(f"No replacements needed (target={args.target_size}, active={summary['total'] - summary['initial']})")
        return 0

    print(f"Need {needed} replacement(s)")

    # Blacklist correctly rejected devices
    conn = sqlite3.connect(args.manifest)
    cur = conn.cursor()
    cur.execute("""
        SELECT device_id, failure_message FROM device_nodes
        WHERE queue = 4 AND failure_category = 'OUT_OF_SCOPE'
    """)
    for device_id, msg in cur.fetchall():
        if device_id not in load_blacklist():
            add_to_blacklist(device_id, msg or "OUT_OF_SCOPE")
            print(f"  X {device_id} -> blacklist")
    conn.close()

    # Count current manufacturer distribution from active devices
    conn = sqlite3.connect(args.manifest)
    cur = conn.cursor()
    cur.execute("SELECT manufacturer FROM device_nodes WHERE queue IN (2, 3, 5)")
    mfg_counts = Counter(row[0] for row in cur.fetchall())
    conn.close()

    replacements = select_replacements(needed, args.manifest, mfg_counts)
    if not replacements:
        print("No suitable replacements found in pool")
        return 1

    print(f"Selected {len(replacements)} replacement(s):")
    for d in replacements:
        print(f"  {d['manufacturer']}|{d['label']}|{d['device_id']}")

    update_batch_file(args.batch_file, replacements)
    add_devices_to_manifest(args.manifest, replacements)

    return 0


if __name__ == "__main__":
    sys.exit(main())
