"""Aggregate ingestion outcomes across all batch manifests into a single analytics DB.

Run after any batch to update the central view:
    python scripts/analytics.py [--report]

Output: output/ingestion_analytics.db
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = REPO_ROOT / "output"
ANALYTICS_DB = OUTPUT_DIR / "ingestion_analytics.db"
EASYSCHEMATIC_JSON = OUTPUT_DIR / "easyschematic" / "unmatched_devices.json"


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_outcomes (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id                TEXT NOT NULL,
    device_id               TEXT NOT NULL,
    manufacturer            TEXT,
    model                   TEXT,

    -- EasySchematic metadata (joined on device_id match)
    es_device_type          TEXT,
    es_category             TEXT,

    -- Our pipeline's view
    device_class            TEXT,   -- from classifier (dante_stagebox, generic, etc.)
    outcome                 TEXT,   -- completed / failed / out_of_scope / in_progress

    -- Failure detail
    failure_stage           INTEGER,
    failure_stage_name      TEXT,
    failure_category        TEXT,
    failure_message         TEXT,
    failure_attempts        INTEGER,

    -- PDF intelligence
    pdf_url_tried           TEXT,
    pdf_url_domain          TEXT,   -- extracted domain for pattern grouping
    chunks_indexed          INTEGER, -- 0 = never indexed; low = marketing brochure

    -- Patch output
    patch_size_chars        INTEGER, -- 0 = no patch generated

    -- Timing
    created_at              TEXT,
    updated_at              TEXT,

    -- Dedup key
    UNIQUE(batch_id, device_id)
);

CREATE INDEX IF NOT EXISTS idx_manufacturer    ON device_outcomes(manufacturer);
CREATE INDEX IF NOT EXISTS idx_failure_cat     ON device_outcomes(failure_category);
CREATE INDEX IF NOT EXISTS idx_outcome         ON device_outcomes(outcome);
CREATE INDEX IF NOT EXISTS idx_es_device_type  ON device_outcomes(es_device_type);
CREATE INDEX IF NOT EXISTS idx_pdf_domain      ON device_outcomes(pdf_url_domain);
"""

STAGE_NAMES = {
    0: "scope_check",
    1: "find_pdf",
    2: "download_pdf",
    3: "index_rag",
    4: "submit_rag",
    5: "extract_specs",
    6: "generate_patch",
    7: "validate_patch",
    8: "html_fallback",
}


def _domain(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return urlparse(url).netloc or None
    except Exception:
        return None


def _outcome(row: dict) -> str:
    if row["failure_category"] == "OUT_OF_SCOPE":
        return "out_of_scope"
    if row["queue"] == 5 and row["patch_source"]:
        return "completed"
    if row["failure_category"]:
        return "failed"
    return "in_progress"


# ---------------------------------------------------------------------------
# Load EasySchematic lookup
# ---------------------------------------------------------------------------

def _load_es_lookup() -> dict[str, dict]:
    """device_id → {deviceType, category} from EasySchematic JSON."""
    if not EASYSCHEMATIC_JSON.exists():
        return {}
    with open(EASYSCHEMATIC_JSON) as f:
        items = json.load(f)
    lookup: dict[str, dict] = {}
    for item in items:
        mfr = item.get("manufacturer", "")
        model = item.get("modelNumber") or item.get("label", "")
        did = re.sub(r"[^a-z0-9-]", "-", f"{mfr}-{model}".lower())
        did = re.sub(r"-+", "-", did).strip("-")[:80]
        lookup[did] = {
            "device_type": item.get("deviceType"),
            "category": item.get("category"),
        }
    return lookup


# ---------------------------------------------------------------------------
# Ingest one manifest DB
# ---------------------------------------------------------------------------

def _ingest_manifest(analytics_conn: sqlite3.Connection, manifest_path: Path, es_lookup: dict) -> int:
    batch_id = manifest_path.parent.name
    try:
        src = sqlite3.connect(str(manifest_path))
        src.row_factory = sqlite3.Row
        cur = src.cursor()
        cur.execute("""
            SELECT device_id, manufacturer, model,
                   queue, patch_source,
                   failure_stage, failure_category, failure_message, failure_attempts,
                   pdf_url, specs_json, device_class,
                   created_at, updated_at
            FROM device_nodes
        """)
        rows = cur.fetchall()
        src.close()
    except Exception as e:
        print(f"  [skip] {manifest_path}: {e}")
        return 0

    # Try to get chunks_indexed from device_documents or specs_json
    inserted = 0
    acur = analytics_conn.cursor()
    for row in rows:
        row = dict(row)
        es = es_lookup.get(row["device_id"], {})

        # device_class: prefer the node field (set at scope check); fall back to specs_json
        device_class = row.get("device_class")
        chunks_indexed = None
        if not device_class and row.get("specs_json"):
            try:
                specs = json.loads(row["specs_json"])
                device_class = specs.get("device_class")
            except Exception:
                pass

        outcome = _outcome(row)
        stage_num = row.get("failure_stage")

        acur.execute("""
            INSERT INTO device_outcomes (
                batch_id, device_id, manufacturer, model,
                es_device_type, es_category,
                device_class, outcome,
                failure_stage, failure_stage_name,
                failure_category, failure_message, failure_attempts,
                pdf_url_tried, pdf_url_domain,
                chunks_indexed, patch_size_chars,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(batch_id, device_id) DO UPDATE SET
                outcome             = excluded.outcome,
                failure_stage       = excluded.failure_stage,
                failure_stage_name  = excluded.failure_stage_name,
                failure_category    = excluded.failure_category,
                failure_message     = excluded.failure_message,
                failure_attempts    = excluded.failure_attempts,
                pdf_url_tried       = excluded.pdf_url_tried,
                pdf_url_domain      = excluded.pdf_url_domain,
                chunks_indexed      = excluded.chunks_indexed,
                patch_size_chars    = excluded.patch_size_chars,
                device_class        = excluded.device_class,
                updated_at          = excluded.updated_at
        """, (
            batch_id,
            row["device_id"],
            row.get("manufacturer"),
            row.get("model"),
            es.get("device_type"),
            es.get("category"),
            device_class,
            outcome,
            stage_num,
            STAGE_NAMES.get(stage_num) if stage_num is not None else None,
            row.get("failure_category"),
            (row.get("failure_message") or "")[:200],
            row.get("failure_attempts"),
            row.get("pdf_url"),
            _domain(row.get("pdf_url")),
            chunks_indexed,
            len(row["patch_source"]) if row.get("patch_source") else 0,
            row.get("created_at"),
            row.get("updated_at"),
        ))
        inserted += 1

    analytics_conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Reports
# ---------------------------------------------------------------------------

def _report(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    print("\n" + "="*70)
    print("INGESTION ANALYTICS REPORT")
    print("="*70)

    # Overall
    cur.execute("SELECT outcome, COUNT(*) FROM device_outcomes GROUP BY outcome ORDER BY COUNT(*) DESC")
    print("\n--- Overall outcomes ---")
    for row in cur.fetchall():
        print(f"  {row[0]:<20} {row[1]:>5}")

    # Failures by manufacturer × stage
    print("\n--- Failures by manufacturer × stage (top 20) ---")
    cur.execute("""
        SELECT manufacturer, failure_stage_name, failure_category, COUNT(*) as n
        FROM device_outcomes
        WHERE outcome = 'failed'
        GROUP BY manufacturer, failure_stage_name, failure_category
        ORDER BY n DESC
        LIMIT 20
    """)
    for row in cur.fetchall():
        print(f"  {(row[0] or 'Unknown'):<28} {(row[1] or '?'):<16} {(row[2] or '?'):<30} ×{row[3]}")

    # Bad PDF domains
    print("\n--- Failed PDF URL domains (likely problem hosts) ---")
    cur.execute("""
        SELECT pdf_url_domain, failure_category, COUNT(*) as n
        FROM device_outcomes
        WHERE outcome = 'failed' AND pdf_url_domain IS NOT NULL
        GROUP BY pdf_url_domain, failure_category
        ORDER BY n DESC
        LIMIT 15
    """)
    for row in cur.fetchall():
        print(f"  {(row[0] or '?'):<45} {(row[1] or '?'):<25} ×{row[2]}")

    # EasySchematic device_type vs failure
    print("\n--- Failure rate by EasySchematic device type ---")
    cur.execute("""
        SELECT es_device_type,
               COUNT(*) as total,
               SUM(CASE WHEN outcome='completed' THEN 1 ELSE 0 END) as completed,
               SUM(CASE WHEN outcome='failed'    THEN 1 ELSE 0 END) as failed
        FROM device_outcomes
        WHERE es_device_type IS NOT NULL
        GROUP BY es_device_type
        HAVING total >= 2
        ORDER BY failed DESC, total DESC
        LIMIT 20
    """)
    for row in cur.fetchall():
        pct = int(100 * row[2] / row[1]) if row[1] else 0
        print(f"  {(row[0] or '?'):<28} total={row[1]:>4}  ✓{row[2]:>4}  ✗{row[3]:>4}  ({pct}% done)")

    # Classifier vs EasySchematic mismatch
    print("\n--- Classifier / EasySchematic device_type mismatches ---")
    cur.execute("""
        SELECT es_device_type, device_class, COUNT(*) as n
        FROM device_outcomes
        WHERE es_device_type IS NOT NULL
          AND device_class IS NOT NULL
          AND es_device_type != device_class
        GROUP BY es_device_type, device_class
        ORDER BY n DESC
        LIMIT 15
    """)
    for row in cur.fetchall():
        print(f"  es={row[0]:<25} classifier={row[1]:<25} ×{row[2]}")

    # Low-chunk extractions (marketing PDFs)
    print("\n--- Devices indexed with < 10 chunks (likely marketing PDFs) ---")
    cur.execute("""
        SELECT manufacturer, model, chunks_indexed, outcome
        FROM device_outcomes
        WHERE chunks_indexed IS NOT NULL AND chunks_indexed < 10 AND chunks_indexed > 0
        ORDER BY chunks_indexed, manufacturer
        LIMIT 20
    """)
    for row in cur.fetchall():
        print(f"  {(row[0] or '?'):<28} {(row[1] or '?'):<35} {row[2]} chunks  [{row[3]}]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _update_processed_file(conn: sqlite3.Connection) -> None:
    """Regenerate output/processed_devices.txt from the analytics DB.

    Called after every update so select_batch scripts always have a fresh
    exclusion list and never re-select already-attempted devices.
    """
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT device_id FROM device_outcomes")
    ids = sorted(row[0] for row in cur.fetchall())
    out = ANALYTICS_DB.parent / "processed_devices.txt"
    out.write_text("\n".join(ids) + "\n")
    print(f"Updated {out} ({len(ids)} device IDs)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Aggregate ingestion analytics")
    parser.add_argument("--report", action="store_true", help="Print pattern report after update")
    parser.add_argument("--report-only", action="store_true", help="Print report without updating")
    args = parser.parse_args()

    ANALYTICS_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(ANALYTICS_DB))
    conn.executescript(SCHEMA)
    conn.commit()

    if not args.report_only:
        es_lookup = _load_es_lookup()
        manifests = sorted(OUTPUT_DIR.rglob("manifest.db"))
        print(f"Found {len(manifests)} manifest DB(s)")
        total = 0
        for m in manifests:
            n = _ingest_manifest(conn, m, es_lookup)
            print(f"  {m.parent.name:<40} {n:>4} devices")
            total += n
        print(f"\nTotal rows upserted: {total}")

    if not args.report_only:
        _update_processed_file(conn)

    if args.report or args.report_only:
        _report(conn)

    conn.close()


if __name__ == "__main__":
    main()
