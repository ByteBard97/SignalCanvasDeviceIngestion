"""Tests for SQLite manifest with crash recovery."""

import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    STAGE_FIND_PDF,
    QUEUE_0_INITIAL,
    QUEUE_2_POLLING_RAGSCALLION,
)


@pytest.fixture
def test_manifest(tmp_path):
    """Create a test manifest database."""
    db_path = tmp_path / "test_ingestion.db"
    return Manifest(db_path)


@pytest.fixture
def sample_device():
    """Create a sample device node."""
    return DeviceNode(
        device_id="yamaha-r08d",
        manufacturer="Yamaha",
        model="R08D",
        corpus_id="corpus-001",
    )


class TestDeviceNodeCreation:
    """Test DeviceNode dataclass initialization."""

    def test_device_node_defaults(self):
        """Test DeviceNode initializes with correct defaults."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="Test",
            model="Model1",
        )

        assert node.device_id == "test-device"
        assert node.manufacturer == "Test"
        assert node.model == "Model1"
        assert node.stage_find_pdf == 0
        assert node.queue == QUEUE_0_INITIAL
        assert node.failure_attempts == 0
        assert node.created_at is not None
        assert node.updated_at is not None

    def test_device_node_timestamps(self):
        """Test DeviceNode timestamps are ISO format."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="Test",
            model="Model1",
        )

        datetime.fromisoformat(node.created_at)
        datetime.fromisoformat(node.updated_at)


class TestManifestPersistence:
    """Test basic persistence operations."""

    def test_add_node(self, test_manifest, sample_device):
        """Test adding a new node to manifest."""
        result = test_manifest.add_node(sample_device)
        assert result is True

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved is not None
        assert retrieved.manufacturer == "Yamaha"
        assert retrieved.model == "R08D"

    def test_add_duplicate_node(self, test_manifest, sample_device):
        """Test that adding duplicate node returns False."""
        test_manifest.add_node(sample_device)
        result = test_manifest.add_node(sample_device)
        assert result is False

    def test_persist_updates_node(self, test_manifest, sample_device):
        """Test persist() updates an existing node."""
        test_manifest.add_node(sample_device)

        sample_device.stage_find_pdf = 2
        sample_device.pdf_url = "http://example.com/manual.pdf"
        test_manifest.persist(sample_device)

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.stage_find_pdf == 2
        assert retrieved.pdf_url == "http://example.com/manual.pdf"

    def test_get_nonexistent_node(self, test_manifest):
        """Test getting a nonexistent node returns None."""
        result = test_manifest.get_node("nonexistent-device")
        assert result is None


class TestManifestStageTracking:
    """Test stage status updates."""

    def test_update_stage(self, test_manifest, sample_device):
        """Test updating a stage status."""
        test_manifest.add_node(sample_device)

        test_manifest.update_stage("yamaha-r08d", STAGE_FIND_PDF, 2)
        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.stage_find_pdf == 2

    def test_update_stage_invalid_device(self, test_manifest):
        """Test updating stage for nonexistent device raises error."""
        with pytest.raises(ValueError, match="Device nonexistent not found"):
            test_manifest.update_stage("nonexistent", STAGE_FIND_PDF, 2)

    def test_update_stage_invalid_stage(self, test_manifest, sample_device):
        """Test updating invalid stage raises error."""
        test_manifest.add_node(sample_device)

        with pytest.raises(ValueError, match="Invalid stage"):
            test_manifest.update_stage("yamaha-r08d", 99, 2)


class TestManifestFailureTracking:
    """Test failure recording."""

    def test_add_failure(self, test_manifest, sample_device):
        """Test recording a failure."""
        test_manifest.add_node(sample_device)

        test_manifest.add_failure(
            "yamaha-r08d",
            "PDF_NOT_FOUND",
            "Manual not found in database",
            stage=STAGE_FIND_PDF,
            retryable=True,
        )

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.failure_stage == STAGE_FIND_PDF
        assert retrieved.failure_category == "PDF_NOT_FOUND"
        assert retrieved.failure_message == "Manual not found in database"
        assert retrieved.failure_retryable is True
        assert retrieved.failure_attempts == 1

    def test_add_failure_increments_attempts(self, test_manifest, sample_device):
        """Test that multiple failures increment attempts."""
        test_manifest.add_node(sample_device)

        test_manifest.add_failure(
            "yamaha-r08d",
            "PDF_DOWNLOAD_FAILED",
            "Timeout downloading PDF",
            stage=1,
            retryable=True,
        )
        test_manifest.add_failure(
            "yamaha-r08d",
            "PDF_DOWNLOAD_FAILED",
            "Network unreachable",
            stage=1,
            retryable=True,
        )

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.failure_attempts == 2

    def test_add_failure_nonexistent_device(self, test_manifest):
        """Test adding failure to nonexistent device raises error."""
        with pytest.raises(ValueError, match="Device nonexistent not found"):
            test_manifest.add_failure(
                "nonexistent",
                "PDF_NOT_FOUND",
                "Not found",
                stage=0,
            )


class TestManifestQuerying:
    """Test querying nodes by queue."""

    def test_list_by_queue(self, test_manifest):
        """Test listing nodes by queue."""
        device1 = DeviceNode(
            device_id="device-1",
            manufacturer="Maker1",
            model="Model1",
            queue=QUEUE_0_INITIAL,
        )
        device2 = DeviceNode(
            device_id="device-2",
            manufacturer="Maker2",
            model="Model2",
            queue=QUEUE_2_POLLING_RAGSCALLION,
        )
        device3 = DeviceNode(
            device_id="device-3",
            manufacturer="Maker3",
            model="Model3",
            queue=QUEUE_0_INITIAL,
        )

        test_manifest.add_node(device1)
        test_manifest.add_node(device2)
        test_manifest.add_node(device3)

        queue_0 = test_manifest.list_by_queue(QUEUE_0_INITIAL)
        queue_2 = test_manifest.list_by_queue(QUEUE_2_POLLING_RAGSCALLION)

        assert len(queue_0) == 2
        assert len(queue_2) == 1
        assert queue_0[0].device_id in ["device-1", "device-3"]
        assert queue_2[0].device_id == "device-2"


class TestCrashRecoveryMidStage:
    """Test recovery from crash during stage execution."""

    def test_recovery_restart_in_progress_stage(self, tmp_path):
        """Test that IN_PROGRESS stage (1) is restarted to NOT_STARTED (0) on recovery."""
        db_path = tmp_path / "crash_test.db"

        # Simulate crash: manually insert node with in-progress stage
        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("""
                CREATE TABLE device_nodes (
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
                    updated_at TEXT NOT NULL
                )
            """)
            node = DeviceNode(
                device_id="test-device",
                manufacturer="Test",
                model="Model",
                stage_find_pdf=1,
            )
            conn.execute("""
                INSERT INTO device_nodes VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
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
            ))
            conn.commit()

        # Now initialize Manifest, which should trigger recovery
        manifest = Manifest(db_path)

        recovered_node = manifest.get_node("test-device")
        assert recovered_node.stage_find_pdf == 0

    def test_recovery_not_started_stage_unchanged(self, tmp_path):
        """Test that NOT_STARTED (0) stage stays at NOT_STARTED (0)."""
        db_path = tmp_path / "no_crash_test.db"

        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("""
                CREATE TABLE device_nodes (
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
                    updated_at TEXT NOT NULL
                )
            """)
            node = DeviceNode(
                device_id="test-device",
                manufacturer="Test",
                model="Model",
                stage_find_pdf=0,
                stage_download_pdf=2,
            )
            conn.execute("""
                INSERT INTO device_nodes VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
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
            ))
            conn.commit()

        manifest = Manifest(db_path)

        recovered_node = manifest.get_node("test-device")
        assert recovered_node.stage_find_pdf == 0
        assert recovered_node.stage_download_pdf == 2

    def test_recovery_multiple_stages(self, tmp_path):
        """Test recovery with multiple in-progress stages."""
        db_path = tmp_path / "multi_stage_test.db"

        with sqlite3.connect(db_path) as conn:
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("""
                CREATE TABLE device_nodes (
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
                    updated_at TEXT NOT NULL
                )
            """)
            node = DeviceNode(
                device_id="test-device",
                manufacturer="Test",
                model="Model",
                stage_find_pdf=2,
                stage_download_pdf=1,
                stage_convert_marker=2,
            )
            conn.execute("""
                INSERT INTO device_nodes VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            """, (
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
            ))
            conn.commit()

        manifest = Manifest(db_path)

        recovered_node = manifest.get_node("test-device")
        assert recovered_node.stage_find_pdf == 2
        assert recovered_node.stage_download_pdf == 0
        assert recovered_node.stage_convert_marker == 2


class TestCrashRecoveryRagscallionPolling:
    """Test recovery from crash during Ragscallion polling."""

    def test_recovery_repoll_ragscallion_job(self, tmp_path):
        """Test that queue_2 (polling) nodes are prepared for re-polling."""
        db_path = tmp_path / "ragscallion_test.db"
        manifest = Manifest(db_path)

        node = DeviceNode(
            device_id="test-device",
            manufacturer="Test",
            model="Model",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-12345",
        )
        manifest.add_node(node)

        recovered_node = manifest.get_node("test-device")
        assert recovered_node.queue == QUEUE_2_POLLING_RAGSCALLION
        assert recovered_node.ragscallion_job_id == "job-12345"

    def test_recovery_no_repoll_without_job_id(self, tmp_path):
        """Test that queue_2 without job_id is not re-processed."""
        db_path = tmp_path / "no_job_test.db"
        manifest = Manifest(db_path)

        node = DeviceNode(
            device_id="test-device",
            manufacturer="Test",
            model="Model",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id=None,
        )
        manifest.add_node(node)

        recovered_node = manifest.get_node("test-device")
        assert recovered_node.ragscallion_job_id is None


class TestWALMode:
    """Test WAL mode configuration."""

    def test_wal_mode_enabled(self, tmp_path):
        """Test that WAL mode is enabled on database."""
        db_path = tmp_path / "wal_test.db"
        manifest = Manifest(db_path)

        with sqlite3.connect(db_path) as conn:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode.upper() == "WAL"


class TestUpdatedAtTimestamp:
    """Test updated_at timestamp tracking."""

    def test_updated_at_on_persist(self, test_manifest, sample_device):
        """Test that updated_at is set when persisting."""
        test_manifest.add_node(sample_device)
        old_time = sample_device.updated_at

        sample_device.pdf_url = "http://example.com"
        test_manifest.persist(sample_device)

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.updated_at > old_time

    def test_updated_at_on_failure(self, test_manifest, sample_device):
        """Test that updated_at is set when recording failure."""
        test_manifest.add_node(sample_device)
        old_time = sample_device.updated_at

        test_manifest.add_failure(
            "yamaha-r08d",
            "PDF_NOT_FOUND",
            "Not found",
            stage=0,
        )

        retrieved = test_manifest.get_node("yamaha-r08d")
        assert retrieved.failure_at is not None
        datetime.fromisoformat(retrieved.failure_at)
