"""Ingestion manifest system - SQLite persistence of device state with crash recovery."""

import json
import sqlite3
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

from pydantic import BaseModel


class StageStatus(str, Enum):
    """Pipeline stage completion status."""
    NOT_STARTED = "NOT_STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class FailureCategory(str, Enum):
    """Categorized failure reasons for analysis and retry."""
    # Stage 0 failure (alias -> canonical SKU resolution)
    SKU_RESOLUTION_FAILED = "SKU_RESOLUTION_FAILED" # retryable: yes (user can supply SKU)

    # Stage 1-2 failures (PDF acquisition)
    PDF_NOT_FOUND = "PDF_NOT_FOUND"                 # retryable: yes (user can provide URL)
    PDF_DOWNLOAD_FAILED = "PDF_DOWNLOAD_FAILED"     # retryable: yes (retry download)
    PDF_INVALID = "PDF_INVALID"                     # retryable: no (file corrupt)

    # Stage 1b failures (HTML source acquisition — fallback when PDF not found)
    HTML_SOURCE_NOT_FOUND = "HTML_SOURCE_NOT_FOUND"       # retryable: yes (user can provide URL)
    HTML_SOURCE_FETCH_FAILED = "HTML_SOURCE_FETCH_FAILED" # retryable: yes (network/temp issue)

    # Stage 3 failure (PDF conversion)
    MARKER_FAILED = "MARKER_FAILED"                 # retryable: yes (Marker bug)
    MARKER_TIMEOUT = "MARKER_TIMEOUT"               # retryable: yes (longer timeout)

    # Stage 3-4 failures (Ragscallion submission)
    RAGDB_COLLISION = "RAGDB_COLLISION"             # retryable: no (human decides)
    RAGSCALLION_UNAVAILABLE = "RAGSCALLION_UNAVAILABLE"  # retryable: yes (server down)
    RAGDB_SUBMISSION_ERROR = "RAGDB_SUBMISSION_ERROR"    # retryable: no (validation error)
    RAGDB_INDEXING_FAILED = "RAGDB_INDEXING_FAILED"      # retryable: no (indexing logic)

    # Stage 5 failure (extraction)
    EXTRACTION_FAILED = "EXTRACTION_FAILED"         # retryable: yes (refine prompt)
    EXTRACTION_TIMEOUT = "EXTRACTION_TIMEOUT"       # retryable: yes (longer timeout)

    # Stage 6 failure (generation)
    PATCH_GENERATION_FAILED = "PATCH_GENERATION_FAILED"  # retryable: yes (different params)

    # Stage 7 failure (validation)
    PATCH_VALIDATION_FAILED = "PATCH_VALIDATION_FAILED"  # retryable: no (fix generation)

    OUT_OF_SCOPE = "OUT_OF_SCOPE"                   # retryable: no (product decision)

    UNKNOWN = "UNKNOWN"                             # retryable: unknown


# Stage numbering constants (no magic numbers per ClaudeCodeRules)
STAGE_FIND_PDF = 0
STAGE_DOWNLOAD_PDF = 1
STAGE_CONVERT_MARKER = 2
STAGE_INDEX_RAG = 3
STAGE_EXTRACT_SPECS = 4
STAGE_GENERATE_PATCH = 5
STAGE_VALIDATE_PATCH = 6
STAGE_RESOLVE_SKU = 7  # Conceptually first; numbered last to avoid renumbering historical data

QUEUE_0_INITIAL = 0
QUEUE_1_READY_FOR_PROCESSING = 1
QUEUE_1_CANNOT_FIND_PDF = 1  # Alias: devices that fail Stage 1/2 land here
QUEUE_2_POLLING_RAGSCALLION = 2
QUEUE_3_READY_FOR_EXTRACTION = 3  # After Ragscallion indexing complete
QUEUE_4_MANUAL_REVIEW = 4
QUEUE_5_COMPLETED = 5  # Terminal: extraction succeeded, specs_json populated

# Recovery action types
RECOVERY_RESUME_STAGE = "resume"
RECOVERY_RESTART_STAGE = "restart"
RECOVERY_REPOLL_RAGSCALLION = "repoll"


# Document types tracked in device_documents. Devices typically need at least
# a spec sheet; manuals and install guides cover bridge/routing and physical
# specs that the spec sheet often omits.
DOC_TYPE_SPEC_SHEET = "spec_sheet"
DOC_TYPE_USER_MANUAL = "user_manual"
DOC_TYPE_INSTALL_GUIDE = "install_guide"
DOC_TYPE_API_DOC = "api_doc"
DOC_TYPE_OTHER = "other"


@dataclass
class DeviceDocument:
    """One PDF associated with a device. A device may have many."""

    device_id: str
    doc_type: str
    url: Optional[str] = None
    local_path: Optional[str] = None
    ragscallion_job_id: Optional[str] = None
    indexed_at: Optional[str] = None
    id: Optional[int] = None
    created_at: Optional[str] = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc).isoformat()


@dataclass
class DeviceNode:
    """Persistent storage model for device ingestion state with crash recovery."""

    device_id: str
    manufacturer: str
    model: str
    corpus_id: Optional[str] = None

    # Stage tracking: 0=NOT_STARTED, 1=IN_PROGRESS, 2=COMPLETED, 3=FAILED
    stage_resolve_sku: int = 0
    stage_find_pdf: int = 0
    stage_download_pdf: int = 0
    stage_convert_marker: int = 0
    stage_index_rag: int = 0
    stage_extract_specs: int = 0
    stage_generate_patch: int = 0
    stage_validate_patch: int = 0

    # Canonical product identity (resolved from user-facing alias by Stage 0)
    canonical_sku: Optional[str] = None
    canonical_product_name: Optional[str] = None

    # Device classification (set by classify_device at scope check; persisted for analytics)
    device_class: Optional[str] = None

    # PDF artifacts
    pdf_url: Optional[str] = None
    pdf_path: Optional[str] = None

    # Ragscallion polling state
    ragscallion_job_id: Optional[str] = None
    ragscallion_submitted_at: Optional[str] = None
    ragscallion_completed_at: Optional[str] = None

    # Suspicious device tracking
    marked_suspicious: bool = False

    # Output artifacts
    specs_json: Optional[str] = None
    patch_source: Optional[str] = None

    # Failure tracking
    failure_stage: Optional[int] = None
    failure_category: Optional[str] = None
    failure_message: Optional[str] = None
    failure_retryable: bool = False
    failure_attempts: int = 0
    failure_at: Optional[str] = None

    # Queue assignment (0=initial, 1=ready, 2=polling)
    queue: int = QUEUE_0_INITIAL

    # Timestamps
    created_at: str = None
    updated_at: str = None

    def __post_init__(self):
        """Initialize timestamps if not set."""
        if self.created_at is None:
            self.created_at = datetime.now(timezone.utc).isoformat()
        if self.updated_at is None:
            self.updated_at = datetime.now(timezone.utc).isoformat()


class IngestionNode(BaseModel):
    """Represents a single device's journey through the 7-stage pipeline.

    Pydantic wrapper around DeviceNode for backwards compatibility.
    """

    device_id: str
    manufacturer: str
    model: str

    stage_find_pdf: StageStatus = StageStatus.NOT_STARTED
    stage_download_pdf: StageStatus = StageStatus.NOT_STARTED
    stage_convert_marker: StageStatus = StageStatus.NOT_STARTED
    stage_index_rag: StageStatus = StageStatus.NOT_STARTED
    stage_extract_specs: StageStatus = StageStatus.NOT_STARTED
    stage_generate_patch: StageStatus = StageStatus.NOT_STARTED
    stage_validate_patch: StageStatus = StageStatus.NOT_STARTED

    attempt_count: int = 0
    last_failure: Optional[FailureCategory] = None
    failure_history: list[dict] = []

    pdf_url: Optional[str] = None
    pdf_path: Optional[Path] = None
    markdown_path: Optional[Path] = None
    specs_json: Optional[str] = None
    patch_source: Optional[str] = None

    is_complete: bool = False
    is_valid: bool = False
    validation_errors: list[str] = []

    created_at: datetime = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = True

    def __init__(self, **data):
        super().__init__(**data)
        if self.created_at is None:
            self.created_at = datetime.now()

    def get_current_stage(self) -> int:
        """Return the index of the next incomplete stage (1-7)."""
        stages = [
            self.stage_find_pdf,
            self.stage_download_pdf,
            self.stage_convert_marker,
            self.stage_index_rag,
            self.stage_extract_specs,
            self.stage_generate_patch,
            self.stage_validate_patch,
        ]
        for i, status in enumerate(stages, 1):
            if status == StageStatus.NOT_STARTED:
                return i
        return 8

    def add_failure(self, stage: int, category: FailureCategory, message: str):
        """Record a failure for retry analysis."""
        self.failure_history.append({
            "stage": stage,
            "category": category.value,
            "message": message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self.last_failure = category
        self.attempt_count += 1


class Manifest:
    """SQLite-backed persistent manifest with WAL mode and crash recovery."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()
        self._recover_from_crash()

    def _init_schema(self):
        """Initialize SQLite schema with WAL mode for concurrent reads."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")

            conn.execute("""
                CREATE TABLE IF NOT EXISTS device_nodes (
                    device_id TEXT PRIMARY KEY,
                    manufacturer TEXT NOT NULL,
                    model TEXT NOT NULL,
                    corpus_id TEXT,
                    stage_find_pdf INTEGER DEFAULT 0,
                    stage_download_pdf INTEGER DEFAULT 0,
                    stage_convert_marker INTEGER DEFAULT 0,
                    stage_index_rag INTEGER DEFAULT 0,
                    stage_extract_specs INTEGER DEFAULT 0,
                    stage_generate_patch INTEGER DEFAULT 0,
                    stage_validate_patch INTEGER DEFAULT 0,
                    pdf_url TEXT,
                    pdf_path TEXT,
                    ragscallion_job_id TEXT,
                    ragscallion_submitted_at TEXT,
                    ragscallion_completed_at TEXT,
                    marked_suspicious BOOLEAN DEFAULT 0,
                    specs_json TEXT,
                    patch_source TEXT,
                    failure_stage INTEGER,
                    failure_category TEXT,
                    failure_message TEXT,
                    failure_retryable BOOLEAN DEFAULT 0,
                    failure_attempts INTEGER DEFAULT 0,
                    failure_at TEXT,
                    queue INTEGER DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    stage_resolve_sku INTEGER DEFAULT 0,
                    canonical_sku TEXT,
                    canonical_product_name TEXT
                )
            """)

            # Migration: add columns if missing (for DBs created before Stage 0).
            existing_cols = {
                row[1] for row in conn.execute("PRAGMA table_info(device_nodes)").fetchall()
            }
            migrations = [
                ("stage_resolve_sku", "INTEGER DEFAULT 0"),
                ("canonical_sku", "TEXT"),
                ("canonical_product_name", "TEXT"),
                ("device_class", "TEXT"),
            ]
            for col, ddl in migrations:
                if col not in existing_cols:
                    conn.execute(f"ALTER TABLE device_nodes ADD COLUMN {col} {ddl}")

            # Per-LLM-call token usage log. Append-only; one row per chat completion.
            # Lets us roll up cost per device, per stage, or across the whole run
            # without scraping log files.
            conn.execute("""
                CREATE TABLE IF NOT EXISTS usage_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    device_id TEXT,
                    stage TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    model TEXT NOT NULL,
                    prompt_tokens INTEGER NOT NULL,
                    completion_tokens INTEGER NOT NULL,
                    total_tokens INTEGER NOT NULL,
                    elapsed_ms INTEGER,
                    called_at TEXT NOT NULL
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_usage_log_device ON usage_log(device_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_usage_log_stage ON usage_log(stage)"
            )

            # Per-device document set. Each device may have many PDFs (spec sheet,
            # user manual, install guide, etc.). The pdf_url/pdf_path columns on
            # device_nodes remain as the "primary" doc for backward compat; this
            # table is the source of truth for the full coverage set.
            conn.execute("""
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
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_device_documents_device ON device_documents(device_id)"
            )

            self._backfill_device_documents(conn)

            conn.commit()

    def _backfill_device_documents(self, conn: sqlite3.Connection):
        """One-time backfill: copy each device's existing pdf_url/pdf_path into
        device_documents as a spec_sheet row, if not already present."""
        rows = conn.execute(
            "SELECT device_id, pdf_url, pdf_path FROM device_nodes "
            "WHERE pdf_url IS NOT NULL OR pdf_path IS NOT NULL"
        ).fetchall()
        now = datetime.now(timezone.utc).isoformat()
        for device_id, pdf_url, pdf_path in rows:
            existing = conn.execute(
                "SELECT 1 FROM device_documents WHERE device_id = ? AND doc_type = ?",
                (device_id, DOC_TYPE_SPEC_SHEET),
            ).fetchone()
            if existing:
                continue
            conn.execute(
                "INSERT INTO device_documents "
                "(device_id, doc_type, url, local_path, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (device_id, DOC_TYPE_SPEC_SHEET, pdf_url, pdf_path, now),
            )

    def _recover_from_crash(self):
        """Recover incomplete nodes from crash on startup.

        Recovery logic:
        - Stages 0-4 (not IN_PROGRESS): resume from current stage
        - Stages 0-4 (IN_PROGRESS): restart that stage
        - Queue_2 (polling): re-poll Ragscallion job
        """
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM device_nodes").fetchall()
            for row in rows:
                node = self._row_to_node(row)

                # Check if in incomplete stage
                if self._is_mid_stage(node):
                    self._restart_mid_stage(node, conn)
                elif node.queue == QUEUE_2_POLLING_RAGSCALLION:
                    self._restart_ragscallion_polling(node, conn)

            conn.commit()

    def _is_mid_stage(self, node: DeviceNode) -> bool:
        """Check if node is in middle of a stage (0=NOT_STARTED, 1=IN_PROGRESS)."""
        stages = [
            node.stage_resolve_sku,
            node.stage_find_pdf,
            node.stage_download_pdf,
            node.stage_convert_marker,
            node.stage_index_rag,
            node.stage_extract_specs,
            node.stage_generate_patch,
            node.stage_validate_patch,
        ]
        return any(s == 1 for s in stages)

    def _restart_mid_stage(self, node: DeviceNode, conn: sqlite3.Connection):
        """Restart a stage that was interrupted mid-execution."""
        stages = [
            "stage_resolve_sku",
            "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
            "stage_index_rag", "stage_extract_specs", "stage_generate_patch",
            "stage_validate_patch",
        ]
        for stage_col in stages:
            if getattr(node, stage_col) == 1:
                setattr(node, stage_col, 0)
        node.updated_at = datetime.now(timezone.utc).isoformat()
        self._persist_node(node, conn)

    def _restart_ragscallion_polling(self, node: DeviceNode, conn: sqlite3.Connection):
        """Re-poll Ragscallion for job status after crash."""
        if node.ragscallion_job_id:
            node.queue = QUEUE_2_POLLING_RAGSCALLION
            node.updated_at = datetime.now(timezone.utc).isoformat()
            self._persist_node(node, conn)

    def _node_to_row(self, node: DeviceNode) -> tuple:
        """Convert DeviceNode to SQLite row tuple.

        Column order matches the CREATE TABLE statement; new columns added by
        ALTER TABLE are appended (SQLite always appends), so Stage-0-related
        columns sit at the end.
        """
        return (
            node.device_id, node.manufacturer, node.model, node.corpus_id,
            node.stage_find_pdf, node.stage_download_pdf, node.stage_convert_marker,
            node.stage_index_rag, node.stage_extract_specs,
            node.stage_generate_patch, node.stage_validate_patch,
            node.pdf_url, node.pdf_path,
            node.ragscallion_job_id, node.ragscallion_submitted_at,
            node.ragscallion_completed_at, node.marked_suspicious,
            node.specs_json, node.patch_source,
            node.failure_stage, node.failure_category, node.failure_message,
            node.failure_retryable, node.failure_attempts, node.failure_at,
            node.queue, node.created_at, node.updated_at,
            node.stage_resolve_sku, node.canonical_sku, node.canonical_product_name,
        )

    def _row_to_node(self, row: tuple) -> DeviceNode:
        """Convert SQLite row to DeviceNode."""
        keys = [
            "device_id", "manufacturer", "model", "corpus_id",
            "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
            "stage_index_rag", "stage_extract_specs",
            "stage_generate_patch", "stage_validate_patch",
            "pdf_url", "pdf_path",
            "ragscallion_job_id", "ragscallion_submitted_at",
            "ragscallion_completed_at", "marked_suspicious",
            "specs_json", "patch_source",
            "failure_stage", "failure_category", "failure_message",
            "failure_retryable", "failure_attempts", "failure_at",
            "queue", "created_at", "updated_at",
            "stage_resolve_sku", "canonical_sku", "canonical_product_name",
        ]
        data = dict(zip(keys, row))
        # Convert SQLite booleans (0/1) to Python booleans
        data["marked_suspicious"] = bool(data["marked_suspicious"])
        data["failure_retryable"] = bool(data["failure_retryable"])
        return DeviceNode(**data)

    def _persist_node(self, node: DeviceNode, conn: sqlite3.Connection):
        """Persist DeviceNode to database via connection."""
        conn.execute("""
            INSERT OR REPLACE INTO device_nodes VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
        """, self._node_to_row(node))

    def add_node(self, node) -> bool:
        """Insert a new device node. Return True if inserted, False if already exists.

        Accepts both DeviceNode and IngestionNode for backwards compatibility.
        """
        device_node = self._ensure_device_node(node)
        with sqlite3.connect(self.db_path) as conn:
            existing = conn.execute(
                "SELECT device_id FROM device_nodes WHERE device_id = ?",
                (device_node.device_id,)
            ).fetchone()

            if existing:
                return False

            self._persist_node(device_node, conn)
            conn.commit()
            return True

    def _ensure_device_node(self, node) -> DeviceNode:
        """Convert IngestionNode to DeviceNode if needed."""
        if isinstance(node, DeviceNode):
            return node
        if isinstance(node, IngestionNode):
            return DeviceNode(
                device_id=node.device_id,
                manufacturer=node.manufacturer,
                model=node.model,
            )
        raise TypeError(f"Expected DeviceNode or IngestionNode, got {type(node)}")

    def persist(self, node):
        """Persist node to database (insert or update).

        Accepts both DeviceNode and IngestionNode for backwards compatibility.
        """
        device_node = self._ensure_device_node(node)
        with sqlite3.connect(self.db_path) as conn:
            device_node.updated_at = datetime.now(timezone.utc).isoformat()
            self._persist_node(device_node, conn)
            conn.commit()

    def get_node(self, device_id: str) -> Optional[DeviceNode]:
        """Retrieve a device node by ID."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM device_nodes WHERE device_id = ?",
                (device_id,)
            ).fetchone()
            if not row:
                return None
            return self._row_to_node(row)

    def get_node_by_corpus_id(self, corpus_id: str) -> Optional[DeviceNode]:
        """Retrieve a device node by its Ragscallion corpus ID."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM device_nodes WHERE corpus_id = ?",
                (corpus_id,)
            ).fetchone()
            if not row:
                return None
            return self._row_to_node(row)

    def list_by_queue(self, queue: int) -> list[DeviceNode]:
        """List all nodes in a specific queue."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT * FROM device_nodes WHERE queue = ? ORDER BY updated_at",
                (queue,)
            ).fetchall()
            return [self._row_to_node(row) for row in rows]

    def update_stage(self, device_id: str, stage: int, status: int):
        """Update a specific stage status for a device (status: 0=NOT_STARTED, 1=IN_PROGRESS, 2=COMPLETED, 3=FAILED)."""
        node = self.get_node(device_id)
        if not node:
            raise ValueError(f"Device {device_id} not found")

        stages = [
            "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
            "stage_index_rag", "stage_extract_specs",
            "stage_generate_patch", "stage_validate_patch",
            "stage_resolve_sku",  # index 7 = STAGE_RESOLVE_SKU
        ]
        if stage < 0 or stage >= len(stages):
            raise ValueError(f"Invalid stage {stage}")

        setattr(node, stages[stage], status)
        self.persist(node)

    def add_failure(self, device_id: str, failure_category: str, failure_message: str,
                    stage: int, retryable: bool = False):
        """Record a failure on a device."""
        node = self.get_node(device_id)
        if not node:
            raise ValueError(f"Device {device_id} not found")

        node.failure_stage = stage
        node.failure_category = failure_category
        node.failure_message = failure_message
        node.failure_retryable = retryable
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        self.persist(node)

    def log_usage(
        self,
        *,
        device_id: Optional[str],
        stage: str,
        provider: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        elapsed_ms: Optional[int] = None,
    ) -> None:
        """Append one chat-completion call's token usage to usage_log.

        device_id may be None for non-device-scoped calls (e.g. ad-hoc evals).
        stage is a free-form label like 'stage_5', 'stage_0_resolve_sku', 'bridge_inference'.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO usage_log (
                    device_id, stage, provider, model,
                    prompt_tokens, completion_tokens, total_tokens, elapsed_ms, called_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    device_id, stage, provider, model,
                    int(prompt_tokens), int(completion_tokens), int(total_tokens),
                    elapsed_ms, datetime.now(timezone.utc).isoformat(),
                ),
            )
            conn.commit()

    def usage_summary(self) -> dict:
        """Roll up token totals across the usage_log. Cheap; uses indexes."""
        with sqlite3.connect(self.db_path) as conn:
            total_in, total_out, calls = conn.execute(
                "SELECT COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(completion_tokens), 0), COUNT(*) FROM usage_log"
            ).fetchone()
            by_stage = {}
            for row in conn.execute(
                "SELECT stage, SUM(prompt_tokens), SUM(completion_tokens), COUNT(*) FROM usage_log GROUP BY stage"
            ).fetchall():
                by_stage[row[0]] = {
                    "prompt_tokens": row[1] or 0,
                    "completion_tokens": row[2] or 0,
                    "calls": row[3],
                }
            return {
                "total_prompt_tokens": total_in,
                "total_completion_tokens": total_out,
                "total_calls": calls,
                "by_stage": by_stage,
            }

    def stats(self) -> dict:
        """Return ingestion statistics."""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM device_nodes").fetchone()[0]
            failed = conn.execute(
                "SELECT COUNT(*) FROM device_nodes WHERE failure_stage IS NOT NULL"
            ).fetchone()[0]

            # Count devices where all stages are COMPLETED (status = 2)
            all_complete = conn.execute("""
                SELECT COUNT(*) FROM device_nodes WHERE
                    stage_find_pdf = 2 AND
                    stage_download_pdf = 2 AND
                    stage_convert_marker = 2 AND
                    stage_index_rag = 2 AND
                    stage_extract_specs = 2 AND
                    stage_generate_patch = 2 AND
                    stage_validate_patch = 2
            """).fetchone()[0]

            # Count by stage completion
            stage_counts = {}
            for stage_col in [
                "stage_resolve_sku",
                "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
                "stage_index_rag", "stage_extract_specs", "stage_generate_patch",
                "stage_validate_patch"
            ]:
                completed = conn.execute(
                    f"SELECT COUNT(*) FROM device_nodes WHERE {stage_col} = 2"
                ).fetchone()[0]
                stage_counts[stage_col] = completed

            return {
                "total_devices": total,
                "completed": all_complete,
                "valid_patches": all_complete,
                "failed": failed,
                "stages_completed": stage_counts,
            }


    def add_document(
        self,
        device_id: str,
        doc_type: str,
        *,
        url: Optional[str] = None,
        local_path: Optional[str] = None,
        ragscallion_job_id: Optional[str] = None,
    ) -> DeviceDocument:
        """Attach a PDF to a device. Idempotent on (device_id, doc_type, url)."""
        doc = DeviceDocument(
            device_id=device_id,
            doc_type=doc_type,
            url=url,
            local_path=local_path,
            ragscallion_job_id=ragscallion_job_id,
        )
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                "INSERT OR IGNORE INTO device_documents "
                "(device_id, doc_type, url, local_path, ragscallion_job_id, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (doc.device_id, doc.doc_type, doc.url, doc.local_path,
                 doc.ragscallion_job_id, doc.created_at),
            )
            conn.commit()
            doc.id = cur.lastrowid
        return doc

    def list_documents(
        self,
        device_id: str,
        doc_type: Optional[str] = None,
    ) -> list[DeviceDocument]:
        """All PDFs for a device, optionally filtered by doc_type."""
        with sqlite3.connect(self.db_path) as conn:
            if doc_type is None:
                rows = conn.execute(
                    "SELECT id, device_id, doc_type, url, local_path, "
                    "ragscallion_job_id, indexed_at, created_at "
                    "FROM device_documents WHERE device_id = ? ORDER BY id",
                    (device_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT id, device_id, doc_type, url, local_path, "
                    "ragscallion_job_id, indexed_at, created_at "
                    "FROM device_documents WHERE device_id = ? AND doc_type = ? "
                    "ORDER BY id",
                    (device_id, doc_type),
                ).fetchall()
        return [
            DeviceDocument(
                id=r[0], device_id=r[1], doc_type=r[2], url=r[3],
                local_path=r[4], ragscallion_job_id=r[5],
                indexed_at=r[6], created_at=r[7],
            )
            for r in rows
        ]

    def mark_document_indexed(
        self,
        doc_id: int,
        ragscallion_job_id: Optional[str] = None,
    ) -> None:
        """Stamp a document as indexed and optionally record its job id."""
        with sqlite3.connect(self.db_path) as conn:
            if ragscallion_job_id is None:
                conn.execute(
                    "UPDATE device_documents SET indexed_at = ? WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), doc_id),
                )
            else:
                conn.execute(
                    "UPDATE device_documents SET indexed_at = ?, ragscallion_job_id = ? "
                    "WHERE id = ?",
                    (datetime.now(timezone.utc).isoformat(), ragscallion_job_id, doc_id),
                )
            conn.commit()

    def set_document_job_id(self, doc_id: int, ragscallion_job_id: str) -> None:
        """Record a Ragscallion job_id for a document without marking it indexed.

        Use at submission time. Indexing completion is recorded separately via
        mark_document_indexed once the polling loop sees the job become ready.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE device_documents SET ragscallion_job_id = ? WHERE id = ?",
                (ragscallion_job_id, doc_id),
            )
            conn.commit()

    def set_document_local_path(
        self,
        device_id: str,
        doc_type: str,
        url: str,
        local_path: str,
    ) -> None:
        """Update the local_path of a matching device document."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "UPDATE device_documents SET local_path = ? "
                "WHERE device_id = ? AND doc_type = ? AND url = ?",
                (local_path, device_id, doc_type, url),
            )
            conn.commit()


# Backwards-compatibility alias for existing code
IngestionManifest = Manifest
