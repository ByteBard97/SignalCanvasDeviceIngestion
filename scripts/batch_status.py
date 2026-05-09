#!/usr/bin/env python3
"""Check batch status — list completed devices and diff against candidate lists.

Usage:
    # List all completed devices from a manifest
    PYTHONPATH=src .venv/bin/python scripts/batch_status.py --db output/batch_20_v3.db

    # Diff a candidate list against completed devices
    PYTHONPATH=src .venv/bin/python scripts/batch_status.py --db output/batch_20_v3.db --candidates candidates.txt

    # Generate a "remaining" file excluding completed devices
    PYTHONPATH=src .venv/bin/python scripts/batch_status.py --db output/batch_20_v3.db --candidates candidates.txt --remaining remaining.txt
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


def _get_status(db_path: Path) -> tuple[list[str], list[tuple[str, str]]]:
    """Return (completed_ids, [(failed_id, reason), ...])."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    c.execute("""
        SELECT device_id, stage_extract_specs, stage_generate_patch, stage_validate_patch,
               failure_category, failure_message
        FROM device_nodes
        ORDER BY device_id
    """)

    done: list[str] = []
    failed: list[tuple[str, str]] = []

    for row in c.fetchall():
        did, s5, s6, s7, fc, fm = row
        if s5 == 2 and s6 == 2 and s7 == 2:
            done.append(did)
        else:
            reason = fc or "incomplete"
            failed.append((did, reason))

    conn.close()
    return done, failed


def _load_candidates(path: Path) -> list[tuple[str, str, str]]:
    """Read pipe-delimited device list: manufacturer|model|device_id."""
    devices: list[tuple[str, str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) != 3:
                continue
            devices.append((parts[0], parts[1], parts[2]))
    return devices


def main() -> int:
    parser = argparse.ArgumentParser(description="Check batch completion status")
    parser.add_argument("--db", required=True, type=Path, help="Path to manifest SQLite DB")
    parser.add_argument("--candidates", type=Path, help="Pipe-delimited candidate device list to diff")
    parser.add_argument("--remaining", type=Path, help="Write remaining (unprocessed) devices to this file")
    parser.add_argument("--out-of-scope", action="store_true", help="Also include OUT_OF_SCOPE failures as 'done' (they were intentionally skipped)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: DB not found: {args.db}", file=sys.stderr)
        return 1

    done, failed = _get_status(args.db)

    print(f"=== Manifest: {args.db} ===\n")
    print(f"Fully completed: {len(done)}")
    for did in done:
        print(f"  ✓ {did}")

    print(f"\nNot completed: {len(failed)}")
    for did, reason in failed:
        print(f"  ✗ {did}: {reason}")

    if args.candidates:
        candidates = _load_candidates(args.candidates)
        done_set = set(done)

        already_done: list[tuple[str, str, str]] = []
        remaining: list[tuple[str, str, str]] = []

        for mfg, model, did in candidates:
            if did in done_set:
                already_done.append((mfg, model, did))
            else:
                remaining.append((mfg, model, did))

        print(f"\n=== Candidate diff: {args.candidates} ===")
        print(f"Already in manifest (done): {len(already_done)}")
        for mfg, model, did in already_done:
            print(f"  ✓ {mfg} | {model} | {did}")

        print(f"\nRemaining (not done): {len(remaining)}")
        for mfg, model, did in remaining:
            print(f"  → {mfg} | {model} | {did}")

        if args.remaining:
            with args.remaining.open("w", encoding="utf-8") as f:
                for mfg, model, did in remaining:
                    f.write(f"{mfg}|{model}|{did}\n")
            print(f"\nWrote {len(remaining)} remaining devices to {args.remaining}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
