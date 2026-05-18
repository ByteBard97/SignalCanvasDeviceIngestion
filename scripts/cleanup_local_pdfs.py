"""One-time cleanup: trash local PDF/doc files already uploaded to Ragscallion.

Targets any device_documents row where ragscallion_job_id IS NOT NULL (file
has been successfully submitted) but local_path IS NOT NULL (file still on disk).
Also nulls out device_nodes.pdf_path for the same devices.

Safe to run while the pipeline is running — only touches files for nodes that
have already moved past Stage 3-4.

Usage:
    python scripts/cleanup_local_pdfs.py [--manifest output/rolling_v3/manifest.db] [--dry-run]
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

from send2trash import send2trash


def run(manifest_db: Path, dry_run: bool) -> None:
    if not manifest_db.exists():
        print(f"ERROR: manifest not found: {manifest_db}")
        sys.exit(1)

    total_bytes = 0
    trashed = 0
    missing = 0
    errors = 0

    with sqlite3.connect(manifest_db) as conn:
        rows = conn.execute(
            "SELECT id, device_id, doc_type, local_path "
            "FROM device_documents "
            "WHERE ragscallion_job_id IS NOT NULL AND local_path IS NOT NULL"
        ).fetchall()

        print(f"Found {len(rows)} document(s) with local files to clean up.")

        for doc_id, device_id, doc_type, local_path in rows:
            p = Path(local_path)
            if not p.exists():
                print(f"  MISSING  {device_id} [{doc_type}] {p.name}")
                missing += 1
                if not dry_run:
                    conn.execute(
                        "UPDATE device_documents SET local_path = NULL WHERE id = ?",
                        (doc_id,),
                    )
                continue

            size = p.stat().st_size
            total_bytes += size
            label = f"{device_id} [{doc_type}] {p.name} ({size // 1024} KB)"

            if dry_run:
                print(f"  DRY-RUN  {label}")
                continue

            try:
                # Null DB reference first — crash safety.
                conn.execute(
                    "UPDATE device_documents SET local_path = NULL WHERE id = ?",
                    (doc_id,),
                )
                send2trash(str(p))
                print(f"  TRASHED  {label}")
                trashed += 1
            except Exception as e:
                print(f"  ERROR    {label}: {e}")
                errors += 1

        # Also clear device_nodes.pdf_path for devices whose spec_sheet was just cleaned.
        if not dry_run:
            cleaned_devices = {r[1] for r in rows}
            for device_id in cleaned_devices:
                conn.execute(
                    "UPDATE device_nodes SET pdf_path = NULL WHERE device_id = ?",
                    (device_id,),
                )
            conn.commit()

    print()
    if dry_run:
        print(f"Dry run complete. Would trash {len(rows) - missing} file(s), "
              f"{total_bytes // (1024 * 1024)} MB.")
    else:
        print(f"Done. Trashed {trashed} file(s), {total_bytes // (1024 * 1024)} MB freed. "
              f"{missing} already missing, {errors} error(s).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Trash local PDFs already uploaded to Ragscallion")
    parser.add_argument(
        "--manifest",
        type=Path,
        action="append",
        dest="manifests",
        help="Path to SQLite manifest database (repeatable). "
             "Defaults to all *.db files under output/.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be trashed without deleting anything",
    )
    args = parser.parse_args()

    if args.manifests:
        manifests = args.manifests
    else:
        manifests = list(Path("output").rglob("*.db"))
        # Exclude analytics/non-manifest DBs that lack device_documents table.
        manifests = [m for m in manifests if "analytics" not in m.name]
        print(f"Auto-discovered {len(manifests)} manifest(s).")

    for manifest in manifests:
        print(f"\n=== {manifest} ===")
        try:
            run(manifest, args.dry_run)
        except Exception as e:
            print(f"  SKIPPED ({e})")


if __name__ == "__main__":
    main()
