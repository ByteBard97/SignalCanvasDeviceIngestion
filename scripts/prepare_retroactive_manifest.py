#!/usr/bin/env python3
"""Consolidate 183 retroactive devices into a fresh manifest DB for re-processing.

Resets extraction/patch stages but keeps PDF/Ragscallion artifacts so the
pipeline skips re-download and goes straight to Stage 5 extraction.
"""

import csv
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO_ROOT / "output"
RETRO_DIR = OUTPUT_DIR / "rolling_retroactive"
RETRO_DB = RETRO_DIR / "manifest.db"
DEVICE_LIST = REPO_ROOT / "rolling_retroactive.txt"


def _find_source_db(device_id: str) -> tuple[Path, sqlite3.Row] | None:
    """Scan all output/*/manifest.db for a completed device record."""
    for db_path in OUTPUT_DIR.rglob("manifest.db"):
        if db_path == RETRO_DB:
            continue
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT * FROM device_nodes WHERE device_id = ?",
                (device_id,),
            )
            row = cur.fetchone()
            conn.close()
            if row:
                return db_path, row
        except Exception:
            continue
    return None


def main() -> None:
    RETRO_DIR.mkdir(parents=True, exist_ok=True)

    # Read schema from an existing DB
    sample_db = next(OUTPUT_DIR.rglob("manifest.db"))
    conn_src = sqlite3.connect(str(sample_db))
    schema = conn_src.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='device_nodes'"
    ).fetchone()[0]
    src_cols = {r[1] for r in conn_src.execute("PRAGMA table_info(device_nodes)")}
    conn_src.close()

    # Create fresh retro DB
    if RETRO_DB.exists():
        RETRO_DB.unlink()
    conn = sqlite3.connect(str(RETRO_DB))
    conn.execute(schema)
    conn.commit()

    # Ensure all columns from source schema exist (some may be ALTER TABLE additions)
    dst_cols = {r[1] for r in conn.execute("PRAGMA table_info(device_nodes)")}
    for col in sorted(src_cols - dst_cols):
        conn.execute(f"ALTER TABLE device_nodes ADD COLUMN {col} TEXT")
    conn.commit()

    # Also create device_documents table so runner doesn't crash
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS device_documents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            device_id TEXT NOT NULL,
            doc_type TEXT NOT NULL,
            url TEXT,
            local_path TEXT,
            ragscallion_job_id TEXT,
            indexed_at TEXT,
            created_at TEXT NOT NULL,
            UNIQUE(device_id, doc_type, url)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_device_documents_device ON device_documents(device_id)"
    )
    conn.commit()

    now = datetime.now(timezone.utc).isoformat()
    copied = 0
    missing: list[str] = []

    with DEVICE_LIST.open("r", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="|")
        for row in reader:
            if len(row) != 3:
                continue
            manufacturer, model, device_id = row

            found = _find_source_db(device_id)
            if found is None:
                missing.append(device_id)
                continue

            db_path, src_row = found
            row_dict = dict(src_row)

            # Reset stages that will be re-run
            row_dict["stage_extract_specs"] = 0
            row_dict["stage_generate_patch"] = 0
            row_dict["stage_validate_patch"] = 0
            row_dict["queue"] = 3  # QUEUE_3_READY_FOR_EXTRACTION

            # Clear old failure state so it doesn't get stuck
            row_dict["failure_stage"] = None
            row_dict["failure_category"] = None
            row_dict["failure_message"] = None
            row_dict["failure_retryable"] = 0
            row_dict["failure_attempts"] = 0
            row_dict["failure_at"] = None
            row_dict["patch_source"] = None

            # Ensure timestamps exist
            row_dict["created_at"] = row_dict.get("created_at") or now
            row_dict["updated_at"] = now

            # Use only columns that exist in the destination DB
            dst_cols_final = {r[1] for r in conn.execute("PRAGMA table_info(device_nodes)")}
            present_cols = [c for c in dst_cols_final if c in row_dict]
            placeholders = ",".join([f":{c}" for c in present_cols])

            conn.execute(
                f"INSERT INTO device_nodes ({','.join(present_cols)}) VALUES ({placeholders})",
                {c: row_dict.get(c) for c in present_cols},
            )
            copied += 1

    conn.commit()
    conn.close()

    print(f"Copied {copied} devices into {RETRO_DB}")
    if missing:
        print(f"Missing: {len(missing)} — {missing[:5]}")


if __name__ == "__main__":
    main()
