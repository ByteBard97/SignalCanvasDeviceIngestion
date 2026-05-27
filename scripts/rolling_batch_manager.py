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
import csv
import random
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))  # for sibling select_devices

from harness.manifest import Manifest, QUEUE_4_MANUAL_REVIEW, QUEUE_5_COMPLETED
# Share the single garbage-model filter so mid-run top-up selection cannot
# re-admit junk entries (bare connector/format model names) that the initial
# select_devices.py selection already excludes.
from select_devices import _is_garbage_model

REPO_ROOT = Path(__file__).resolve().parent.parent
LIBRARY_ROOT = REPO_ROOT.parent / "SignalCanvasDeviceLibrary"
PATCHES_DIR = LIBRARY_ROOT / "patches"
EXCLUDED_CSV = LIBRARY_ROOT / "excluded.csv"
OUTPUT_DIR = REPO_ROOT / "output"

VALID_REASON_CODES = {"CIT", "OOS", "DUP", "DSC", "NDS"}

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


def load_excluded() -> set[str]:
    excluded: set[str] = set()
    if not EXCLUDED_CSV.exists():
        return excluded
    with EXCLUDED_CSV.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            device_id = row.get("device_id", "").strip()
            if device_id:
                excluded.add(device_id)
    return excluded


def load_processed_from_dbs() -> set[str]:
    processed: set[str] = set()
    for db_path in OUTPUT_DIR.rglob("manifest.db"):
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


def load_d_tier_pool() -> list[dict]:
    devices = []
    for patch_file in PATCHES_DIR.rglob("*.patch"):
        mfg_slug = patch_file.parent.name
        if mfg_slug == "_uncategorized":
            continue
        text = patch_file.read_text(errors="replace")
        meta = _parse_meta(text)
        if meta.get("quality") != "D":
            continue
        devices.append({
            "device_id": patch_file.stem,
            "manufacturer": meta.get("manufacturer", ""),
            "label": meta.get("model", ""),
            "mfg_slug": mfg_slug,
        })
    return devices


def select_replacements(
    count: int,
    manifest_db: Path,
    exclude_mfgs: dict[str, int] | None = None,
    max_per_mfg: int = 3,
) -> list[dict]:
    excluded = load_excluded()
    processed = load_processed_from_dbs()
    pool = load_d_tier_pool()

    candidates = [
        d for d in pool
        if d["device_id"] not in excluded
        and d["device_id"] not in processed
        and not _is_garbage_model(d["label"])
    ]

    random.seed(int(datetime.now(timezone.utc).timestamp()))
    random.shuffle(candidates)

    selected = []
    mfg_counts = Counter(exclude_mfgs or {})

    for d in candidates:
        if len(selected) >= count:
            break
        if mfg_counts[d["mfg_slug"]] >= max_per_mfg:
            continue
        selected.append(d)
        mfg_counts[d["mfg_slug"]] += 1

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

    # Add pipeline-confirmed OOS devices to excluded.csv
    existing_excluded = load_excluded()
    conn = sqlite3.connect(args.manifest)
    cur = conn.cursor()
    cur.execute("""
        SELECT device_id, manufacturer, model FROM device_nodes
        WHERE queue = 4 AND failure_category = 'OUT_OF_SCOPE'
    """)
    oos_rows = cur.fetchall()
    conn.close()
    new_oos = [(did, mfg, model) for did, mfg, model in oos_rows if did not in existing_excluded]
    if new_oos:
        with EXCLUDED_CSV.open("a", newline="") as f:
            writer = csv.writer(f)
            for device_id, mfg, model in new_oos:
                writer.writerow([device_id, mfg or "", model or "", "OOS"])
                print(f"  X {device_id} -> excluded.csv (OOS)")


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
