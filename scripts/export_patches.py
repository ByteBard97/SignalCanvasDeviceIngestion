#!/usr/bin/env python3
"""Export validated patches + specs JSON from manifest DB to SignalCanvasDeviceLibrary.

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/export_patches.py \
        --db output/batch_20_v5.db \
        --out /Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches

Creates:
    patches/<first-letter>/<slug>/
        <device-id>.patch
        <device-id>.json
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path


def _slug_from_device_id(device_id: str) -> str:
    """Return a safe directory name from a device_id."""
    return device_id.lower().replace(" ", "-").replace("_", "-")


def _shard_prefix(device_id: str) -> str:
    """First letter of device_id for directory sharding."""
    first = device_id[0].lower() if device_id else "_"
    if first.isalpha():
        return first
    return "_"


def export(db_path: Path, out_dir: Path) -> tuple[int, int]:
    """Export all queue-5 nodes with patch_source from DB to out_dir.

    Returns (exported_count, skipped_count).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute(
        """
        SELECT device_id, manufacturer, model, specs_json, patch_source,
               canonical_sku, canonical_product_name, queue,
               stage_generate_patch, stage_validate_patch
        FROM device_nodes
        WHERE queue = 5
          AND patch_source IS NOT NULL
          AND patch_source != ''
          AND stage_generate_patch = 2
          AND stage_validate_patch = 2
        ORDER BY device_id
        """
    )

    exported = 0
    skipped = 0

    for row in c.fetchall():
        device_id = row["device_id"]
        patch = row["patch_source"]
        specs = row["specs_json"]

        if not patch:
            skipped += 1
            continue

        prefix = _shard_prefix(device_id)
        slug = _slug_from_device_id(device_id)
        device_dir = out_dir / prefix / slug
        device_dir.mkdir(parents=True, exist_ok=True)

        patch_path = device_dir / f"{device_id}.patch"
        patch_path.write_text(patch, encoding="utf-8")

        if specs:
            json_path = device_dir / f"{device_id}.json"
            json_path.write_text(specs, encoding="utf-8")

        exported += 1
        print(f"  {device_id}")

    conn.close()
    return exported, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description="Export patches from manifest DB")
    parser.add_argument("--db", type=Path, default=Path("output/ingestion.db"))
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches"),
    )
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: DB not found: {args.db}", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)

    print(f"Exporting from {args.db} → {args.out}")
    exported, skipped = export(args.db, args.out)
    print(f"\nDone: {exported} exported, {skipped} skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
