"""Ingestion manifest system - SQLite persistence of device state."""

import json
import sqlite3
from datetime import datetime
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
    PDF_NOT_FOUND = "PDF_NOT_FOUND"
    PDF_DOWNLOAD_FAILED = "PDF_DOWNLOAD_FAILED"
    PDF_INVALID = "PDF_INVALID"
    MARKER_FAILED = "MARKER_FAILED"
    MARKER_TIMEOUT = "MARKER_TIMEOUT"
    RAGDB_INDEXING_FAILED = "RAGDB_INDEXING_FAILED"
    EXTRACTION_FAILED = "EXTRACTION_FAILED"
    EXTRACTION_TIMEOUT = "EXTRACTION_TIMEOUT"
    PATCH_GENERATION_FAILED = "PATCH_GENERATION_FAILED"
    PATCH_VALIDATION_FAILED = "PATCH_VALIDATION_FAILED"
    UNKNOWN = "UNKNOWN"


class IngestionNode(BaseModel):
    """Represents a single device's journey through the 7-stage pipeline."""

    device_id: str  # "manufacturer-model" key
    manufacturer: str
    model: str

    # Stage tracking
    stage_find_pdf: StageStatus = StageStatus.NOT_STARTED
    stage_download_pdf: StageStatus = StageStatus.NOT_STARTED
    stage_convert_marker: StageStatus = StageStatus.NOT_STARTED
    stage_index_rag: StageStatus = StageStatus.NOT_STARTED
    stage_extract_specs: StageStatus = StageStatus.NOT_STARTED
    stage_generate_patch: StageStatus = StageStatus.NOT_STARTED
    stage_validate_patch: StageStatus = StageStatus.NOT_STARTED

    # Attempt tracking
    attempt_count: int = 0
    last_failure: Optional[FailureCategory] = None
    failure_history: list[dict] = []  # {stage, category, message, timestamp}

    # Output artifacts
    pdf_url: Optional[str] = None
    pdf_path: Optional[Path] = None
    markdown_path: Optional[Path] = None
    specs_json: Optional[str] = None  # JSON string of extracted specs
    patch_source: Optional[str] = None  # Generated .patch text

    # Status
    is_complete: bool = False
    is_valid: bool = False
    validation_errors: list[str] = []

    # Timestamps
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
        return 8  # All stages complete

    def add_failure(self, stage: int, category: FailureCategory, message: str):
        """Record a failure for retry analysis."""
        self.failure_history.append({
            "stage": stage,
            "category": category.value,
            "message": message,
            "timestamp": datetime.now().isoformat(),
        })
        self.last_failure = category
        self.attempt_count += 1


class IngestionManifest:
    """SQLite-backed persistent manifest of all device ingestion state."""

    def __init__(self, db_path: Path):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self):
        """Initialize SQLite schema if not exists."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_nodes (
                    device_id TEXT PRIMARY KEY,
                    manufacturer TEXT NOT NULL,
                    model TEXT NOT NULL,
                    stage_find_pdf TEXT DEFAULT 'NOT_STARTED',
                    stage_download_pdf TEXT DEFAULT 'NOT_STARTED',
                    stage_convert_marker TEXT DEFAULT 'NOT_STARTED',
                    stage_index_rag TEXT DEFAULT 'NOT_STARTED',
                    stage_extract_specs TEXT DEFAULT 'NOT_STARTED',
                    stage_generate_patch TEXT DEFAULT 'NOT_STARTED',
                    stage_validate_patch TEXT DEFAULT 'NOT_STARTED',
                    attempt_count INTEGER DEFAULT 0,
                    last_failure TEXT,
                    failure_history TEXT DEFAULT '[]',
                    pdf_url TEXT,
                    pdf_path TEXT,
                    markdown_path TEXT,
                    specs_json TEXT,
                    patch_source TEXT,
                    is_complete BOOLEAN DEFAULT 0,
                    is_valid BOOLEAN DEFAULT 0,
                    validation_errors TEXT DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    started_at TEXT,
                    completed_at TEXT
                )
            """)
            conn.commit()

    def add_node(self, node: IngestionNode) -> bool:
        """Insert or update a device node. Return True if inserted, False if updated."""
        with sqlite3.connect(self.db_path) as conn:
            existing = conn.execute(
                "SELECT device_id FROM ingestion_nodes WHERE device_id = ?",
                (node.device_id,)
            ).fetchone()

            if existing:
                self._update_node(conn, node)
                return False
            else:
                self._insert_node(conn, node)
                return True

    def _insert_node(self, conn: sqlite3.Connection, node: IngestionNode):
        """Insert a new node."""
        conn.execute("""
            INSERT INTO ingestion_nodes (
                device_id, manufacturer, model,
                stage_find_pdf, stage_download_pdf, stage_convert_marker,
                stage_index_rag, stage_extract_specs, stage_generate_patch,
                stage_validate_patch, attempt_count, last_failure,
                failure_history, pdf_url, pdf_path, markdown_path,
                specs_json, patch_source, is_complete, is_valid,
                validation_errors, created_at, started_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            node.device_id, node.manufacturer, node.model,
            node.stage_find_pdf.value, node.stage_download_pdf.value, node.stage_convert_marker.value,
            node.stage_index_rag.value, node.stage_extract_specs.value, node.stage_generate_patch.value,
            node.stage_validate_patch.value, node.attempt_count,
            node.last_failure.value if node.last_failure else None,
            json.dumps(node.failure_history),
            node.pdf_url, str(node.pdf_path) if node.pdf_path else None,
            str(node.markdown_path) if node.markdown_path else None,
            node.specs_json, node.patch_source,
            node.is_complete, node.is_valid, json.dumps(node.validation_errors),
            node.created_at.isoformat(),
            node.started_at.isoformat() if node.started_at else None,
            node.completed_at.isoformat() if node.completed_at else None
        ))
        conn.commit()

    def _update_node(self, conn: sqlite3.Connection, node: IngestionNode):
        """Update an existing node."""
        conn.execute("""
            UPDATE ingestion_nodes SET
                stage_find_pdf = ?, stage_download_pdf = ?, stage_convert_marker = ?,
                stage_index_rag = ?, stage_extract_specs = ?, stage_generate_patch = ?,
                stage_validate_patch = ?, attempt_count = ?, last_failure = ?,
                failure_history = ?, pdf_url = ?, pdf_path = ?, markdown_path = ?,
                specs_json = ?, patch_source = ?, is_complete = ?, is_valid = ?,
                validation_errors = ?, started_at = ?, completed_at = ?
            WHERE device_id = ?
        """, (
            node.stage_find_pdf.value, node.stage_download_pdf.value, node.stage_convert_marker.value,
            node.stage_index_rag.value, node.stage_extract_specs.value, node.stage_generate_patch.value,
            node.stage_validate_patch.value, node.attempt_count,
            node.last_failure.value if node.last_failure else None,
            json.dumps(node.failure_history),
            node.pdf_url, str(node.pdf_path) if node.pdf_path else None,
            str(node.markdown_path) if node.markdown_path else None,
            node.specs_json, node.patch_source,
            node.is_complete, node.is_valid, json.dumps(node.validation_errors),
            node.started_at.isoformat() if node.started_at else None,
            node.completed_at.isoformat() if node.completed_at else None,
            node.device_id
        ))
        conn.commit()

    def get_node(self, device_id: str) -> Optional[IngestionNode]:
        """Retrieve a device node by ID."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT * FROM ingestion_nodes WHERE device_id = ?",
                (device_id,)
            ).fetchone()
            if not row:
                return None
            return self._row_to_node(row)

    def get_all_nodes(self) -> list[IngestionNode]:
        """Retrieve all device nodes."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("SELECT * FROM ingestion_nodes").fetchall()
            return [self._row_to_node(row) for row in rows]

    def _row_to_node(self, row: tuple) -> IngestionNode:
        """Convert SQLite row to IngestionNode."""
        keys = [
            "device_id", "manufacturer", "model",
            "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
            "stage_index_rag", "stage_extract_specs", "stage_generate_patch",
            "stage_validate_patch", "attempt_count", "last_failure",
            "failure_history", "pdf_url", "pdf_path", "markdown_path",
            "specs_json", "patch_source", "is_complete", "is_valid",
            "validation_errors", "created_at", "started_at", "completed_at"
        ]
        data = dict(zip(keys, row))
        data["stage_find_pdf"] = StageStatus(data["stage_find_pdf"])
        data["stage_download_pdf"] = StageStatus(data["stage_download_pdf"])
        data["stage_convert_marker"] = StageStatus(data["stage_convert_marker"])
        data["stage_index_rag"] = StageStatus(data["stage_index_rag"])
        data["stage_extract_specs"] = StageStatus(data["stage_extract_specs"])
        data["stage_generate_patch"] = StageStatus(data["stage_generate_patch"])
        data["stage_validate_patch"] = StageStatus(data["stage_validate_patch"])
        data["last_failure"] = FailureCategory(data["last_failure"]) if data["last_failure"] else None
        data["failure_history"] = json.loads(data["failure_history"])
        data["validation_errors"] = json.loads(data["validation_errors"])
        if data["pdf_path"]:
            data["pdf_path"] = Path(data["pdf_path"])
        if data["markdown_path"]:
            data["markdown_path"] = Path(data["markdown_path"])
        data["created_at"] = datetime.fromisoformat(data["created_at"])
        if data["started_at"]:
            data["started_at"] = datetime.fromisoformat(data["started_at"])
        if data["completed_at"]:
            data["completed_at"] = datetime.fromisoformat(data["completed_at"])

        return IngestionNode(**data)

    def stats(self) -> dict:
        """Return ingestion statistics."""
        with sqlite3.connect(self.db_path) as conn:
            total = conn.execute("SELECT COUNT(*) FROM ingestion_nodes").fetchone()[0]
            complete = conn.execute("SELECT COUNT(*) FROM ingestion_nodes WHERE is_complete = 1").fetchone()[0]
            valid = conn.execute("SELECT COUNT(*) FROM ingestion_nodes WHERE is_valid = 1").fetchone()[0]
            failed = conn.execute("SELECT COUNT(*) FROM ingestion_nodes WHERE last_failure IS NOT NULL").fetchone()[0]

            # Count by stage
            stage_counts = {}
            for stage_col in [
                "stage_find_pdf", "stage_download_pdf", "stage_convert_marker",
                "stage_index_rag", "stage_extract_specs", "stage_generate_patch",
                "stage_validate_patch"
            ]:
                completed = conn.execute(
                    f"SELECT COUNT(*) FROM ingestion_nodes WHERE {stage_col} = 'COMPLETED'"
                ).fetchone()[0]
                stage_counts[stage_col] = completed

            return {
                "total_devices": total,
                "completed": complete,
                "valid_patches": valid,
                "failed": failed,
                "stages_completed": stage_counts,
            }
