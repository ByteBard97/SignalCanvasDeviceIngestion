#!/usr/bin/env python3
"""Reset 3 stuck devices so the pipeline can re-download and re-submit their PDFs."""

import sqlite3
import sys
from pathlib import Path

DB_PATH = Path("output/rolling_v8/manifest.db")
DEVICE_IDS = (
    "shure-ad4-q",
    "ereca-stage-racer-2-12-sdi",
    "blackmagic-design-hyper-deck-studio-hd-plus",
)


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # Verify pdf_url is populated for all 3
    placeholders = ",".join("?" * len(DEVICE_IDS))
    cur.execute(
        f"SELECT device_id, pdf_url FROM device_nodes WHERE device_id IN ({placeholders})",
        DEVICE_IDS,
    )
    rows = {r["device_id"]: r["pdf_url"] for r in cur.fetchall()}
    for did in DEVICE_IDS:
        if did not in rows:
            print(f"ERROR: device_id {did} not found in devices table", file=sys.stderr)
            return 1
        if not rows[did]:
            print(f"ERROR: device_id {did} has NULL pdf_url", file=sys.stderr)
            return 1
        print(f"OK: {did} pdf_url={rows[did]}")

    # Reset device fields
    cur.execute(
        f"""
        UPDATE device_nodes
        SET
            stage_download_pdf = 0,
            stage_convert_marker = 0,
            stage_index_rag = 0,
            failure_stage = NULL,
            failure_category = NULL,
            failure_message = NULL,
            failure_retryable = 0,
            failure_at = NULL,
            failure_attempts = 0,
            queue = 1,
            ragscallion_job_id = NULL,
            ragscallion_submitted_at = NULL,
            marked_suspicious = 0,
            pdf_path = NULL
        WHERE device_id IN ({placeholders})
        """,
        DEVICE_IDS,
    )
    print(f"Updated {cur.rowcount} device rows")

    # Delete orphan device_documents rows: ragscallion_job_id set but local_path NULL
    cur.execute(
        f"""
        DELETE FROM device_documents
        WHERE device_id IN ({placeholders})
          AND ragscallion_job_id IS NOT NULL
          AND local_path IS NULL
        """,
        DEVICE_IDS,
    )
    print(f"Deleted {cur.rowcount} orphan document rows")

    conn.commit()
    conn.close()
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
