#!/usr/bin/env python3
"""Rolling pipeline runner — continuously process batches of devices,
replacing completed/rejected ones with new random selections.

Usage:
    .venv/bin/python scripts/run_rolling_pipeline.py \
        --batch-file batch_40_es_only_v2.txt \
        --manifest output/batch_40_es_only_v2/manifest.db \
        --target-size 40 \
        --max-batches 10
"""

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


def run_command(cmd: list[str]) -> int:
    print(f"\n{'='*60}")
    print(f"Running: {' '.join(cmd)}")
    print(f"{'='*60}\n")
    return subprocess.call(cmd)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-file", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--batch-name", type=str, required=True, help="Batch name written into meta (e.g. batch_300_v1)")
    parser.add_argument("--target-size", type=int, default=40)
    parser.add_argument("--max-batches", type=int, default=10)
    parser.add_argument("--cache-dir", type=Path, default=Path("output/pdfs"))
    args = parser.parse_args()

    for batch_num in range(1, args.max_batches + 1):
        print(f"\n{'#'*60}")
        print(f"# BATCH {batch_num}")
        print(f"{'#'*60}")

        # Run the pipeline
        ret = run_command([
            sys.executable, "-m", "src.runner",
            "--devices", str(args.batch_file),
            "--manifest-db", str(args.manifest),
            "--cache-dir", str(args.cache_dir),
        ])

        if ret != 0:
            print(f"Pipeline exited with code {ret}, stopping rolling runner")
            return ret

        # Export completed patches to library
        print(f"\n{'='*60}")
        print("Exporting completed patches...")
        print(f"{'='*60}\n")
        run_command([
            sys.executable, "scripts/export_patches.py",
            "--db", str(args.manifest),
            "--batch", args.batch_name,
        ])

        # Add replacements for completed + correctly rejected devices
        print(f"\n{'='*60}")
        print("Adding replacements...")
        print(f"{'='*60}\n")
        ret = run_command([
            sys.executable, "scripts/rolling_batch_manager.py",
            "--batch-file", str(args.batch_file),
            "--manifest", str(args.manifest),
            "--target-size", str(args.target_size),
        ])

        if ret != 0:
            print("No replacements added (pool may be exhausted or target already met)")
            # Check if there are any active nodes left
            import sqlite3
            conn = sqlite3.connect(args.manifest)
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM device_nodes WHERE queue IN (0, 2, 3)")
            active = cur.fetchone()[0]
            conn.close()
            if active == 0:
                print("No active nodes remaining. Rolling pipeline complete.")
                break

    return 0


if __name__ == "__main__":
    sys.exit(main())
