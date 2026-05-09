#!/usr/bin/env python3
"""Backward pass: fix channel-to-port expansion bugs in all completed devices."""

import glob
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from stages.normalize_specs import normalize_extraction
from stages.generate_patch import generate_patch


def fix_db(db_path: str) -> list[dict]:
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute(
        "SELECT device_id, specs_json, patch_source FROM device_nodes WHERE queue = 5 AND specs_json IS NOT NULL AND specs_json != ''"
    )
    rows = c.fetchall()

    changes = []
    for device_id, specs_json_str, old_patch in rows:
        try:
            extracted = json.loads(specs_json_str)
        except json.JSONDecodeError:
            continue

        # Get device class from specs or default to generic
        device_class = extracted.get("device_metadata", {}).get("device_type", "generic")
        if not device_class:
            device_class = "generic"

        # Re-normalize with the new _correct_channels rules
        normalized = normalize_extraction(extracted, device_class)

        # Regenerate patch
        new_patch = generate_patch(normalized)

        if new_patch != old_patch:
            # Update DB
            c.execute(
                "UPDATE device_nodes SET patch_source = ?, specs_json = ? WHERE device_id = ?",
                (new_patch, json.dumps(normalized), device_id),
            )
            changes.append({
                "device_id": device_id,
                "db": db_path,
                "old_patch_len": len(old_patch) if old_patch else 0,
                "new_patch_len": len(new_patch),
            })

    conn.commit()
    conn.close()
    return changes


def main():
    dbs = sorted(glob.glob("output/*.db"))
    all_changes = []
    for db_path in dbs:
        changes = fix_db(db_path)
        if changes:
            print(f"{db_path}: {len(changes)} devices fixed")
            for ch in changes:
                print(f"  {ch['device_id']}: {ch['old_patch_len']} -> {ch['new_patch_len']} bytes")
            all_changes.extend(changes)
        else:
            print(f"{db_path}: no changes")

    print(f"\nTotal devices fixed across all DBs: {len(all_changes)}")


if __name__ == "__main__":
    main()
