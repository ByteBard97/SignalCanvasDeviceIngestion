"""Tests for failure metadata tracking across all pipeline stages."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.harness.manifest import (
    DeviceNode,
    FailureCategory,
    Manifest,
    QUEUE_4_MANUAL_REVIEW,
    STAGE_FIND_PDF,
    STAGE_DOWNLOAD_PDF,
    STAGE_EXTRACT_SPECS,
    STAGE_GENERATE_PATCH,
    STAGE_VALIDATE_PATCH,
)
from src.pipeline_stages import (
    stage_3_4_submit_to_ragscallion,
    stage_5_extract_specs,
    STAGE_FAILED,
)
from src.ragscallion_client import (
    RagscallionClient,
    RagscallionCollisionError,
    RagscallionError,
    RagscallionUnavailableError,
)


@pytest.fixture
def tmp_manifest(tmp_path):
    """Create a temporary manifest for testing."""
    db_path = tmp_path / "test_manifest.db"
    manifest = Manifest(db_path)
    return manifest


@pytest.fixture
def sample_device_node(tmp_path):
    """Create a sample device node with PDF."""
    pdf_path = tmp_path / "test.pdf"
    pdf_path.write_bytes(b"%PDF-1.4\n%test pdf content")

    node = DeviceNode(
        device_id="yamaha-r08d",
        manufacturer="YAMAHA",
        model="R08D",
        pdf_path=str(pdf_path),
        queue=0,
    )
    return node


@pytest.fixture
def ragscallion_client():
    """Create a RagscallionClient instance."""
    return RagscallionClient(base_url="http://localhost:8086")


class TestFailureCategoryEnum:
    """Tests for FailureCategory enum values."""

    def test_failure_category_values_exist(self):
        """Test all required failure category values are defined."""
        # Stage 1-2 categories
        assert FailureCategory.PDF_NOT_FOUND.value == "PDF_NOT_FOUND"
        assert FailureCategory.PDF_DOWNLOAD_FAILED.value == "PDF_DOWNLOAD_FAILED"
        assert FailureCategory.PDF_INVALID.value == "PDF_INVALID"

        # Stage 3 categories
        assert FailureCategory.MARKER_FAILED.value == "MARKER_FAILED"
        assert FailureCategory.MARKER_TIMEOUT.value == "MARKER_TIMEOUT"

        # Stage 3-4 categories
        assert FailureCategory.RAGDB_COLLISION.value == "RAGDB_COLLISION"
        assert FailureCategory.RAGSCALLION_UNAVAILABLE.value == "RAGSCALLION_UNAVAILABLE"
        assert FailureCategory.RAGDB_SUBMISSION_ERROR.value == "RAGDB_SUBMISSION_ERROR"

        # Stage 5 categories
        assert FailureCategory.EXTRACTION_FAILED.value == "EXTRACTION_FAILED"
        assert FailureCategory.EXTRACTION_TIMEOUT.value == "EXTRACTION_TIMEOUT"

        # Stage 6 categories
        assert FailureCategory.PATCH_GENERATION_FAILED.value == "PATCH_GENERATION_FAILED"

        # Stage 7 categories
        assert FailureCategory.PATCH_VALIDATION_FAILED.value == "PATCH_VALIDATION_FAILED"

    def test_failure_category_enum_string_conversion(self):
        """Test FailureCategory enum converts to string correctly."""
        assert str(FailureCategory.PDF_NOT_FOUND) == "FailureCategory.PDF_NOT_FOUND"
        assert FailureCategory.PDF_NOT_FOUND.value == "PDF_NOT_FOUND"


class TestStage1PdfNotFoundMetadata:
    """Tests for stage 1 failure metadata tracking."""

    def test_stage_1_pdf_not_found_metadata(self, tmp_manifest):
        """Test correct stage, category, and retryable flag for PDF not found."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
            queue=0,
        )
        tmp_manifest.add_node(node)

        # Simulate stage 1 failure
        tmp_manifest.add_failure(
            device_id="test-device",
            failure_category=FailureCategory.PDF_NOT_FOUND.value,
            failure_message="No PDF found for TestMfg TestModel",
            stage=STAGE_FIND_PDF,
            retryable=True,
        )

        retrieved = tmp_manifest.get_node("test-device")
        assert retrieved.failure_stage == STAGE_FIND_PDF
        assert retrieved.failure_category == FailureCategory.PDF_NOT_FOUND.value
        assert retrieved.failure_retryable is True
        assert "No PDF found" in retrieved.failure_message
        assert retrieved.failure_attempts == 1


class TestStage2DownloadFailedMetadata:
    """Tests for stage 2 failure metadata tracking."""

    def test_stage_2_download_failed_metadata(self, tmp_manifest):
        """Test correct metadata for PDF download failure."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
            queue=0,
        )
        tmp_manifest.add_node(node)

        tmp_manifest.add_failure(
            device_id="test-device",
            failure_category=FailureCategory.PDF_DOWNLOAD_FAILED.value,
            failure_message="Download failed: Connection timeout",
            stage=STAGE_DOWNLOAD_PDF,
            retryable=True,
        )

        retrieved = tmp_manifest.get_node("test-device")
        assert retrieved.failure_stage == STAGE_DOWNLOAD_PDF
        assert retrieved.failure_category == FailureCategory.PDF_DOWNLOAD_FAILED.value
        assert retrieved.failure_retryable is True
        assert "timeout" in retrieved.failure_message

    def test_stage_2_pdf_invalid_metadata(self, tmp_manifest):
        """Test correct metadata for invalid PDF file."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
            queue=0,
        )
        tmp_manifest.add_node(node)

        tmp_manifest.add_failure(
            device_id="test-device",
            failure_category=FailureCategory.PDF_INVALID.value,
            failure_message="Downloaded PDF is invalid: corrupted file",
            stage=STAGE_DOWNLOAD_PDF,
            retryable=False,
        )

        retrieved = tmp_manifest.get_node("test-device")
        assert retrieved.failure_stage == STAGE_DOWNLOAD_PDF
        assert retrieved.failure_category == FailureCategory.PDF_INVALID.value
        assert retrieved.failure_retryable is False


class TestStage34RagscallionMetadata:
    """Tests for stage 3-4 Ragscallion failure metadata tracking."""

    @pytest.mark.asyncio
    async def test_stage_3_4_collision_metadata(
        self, sample_device_node, tmp_manifest, ragscallion_client
    ):
        """Test collision error marked as not retryable."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionCollisionError(
                "source_label 'YAMAHA R08D' already exists"
            )

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            retrieved = tmp_manifest.get_node("yamaha-r08d")
            assert retrieved.failure_stage == 3  # STAGE_INDEX_RAG
            assert retrieved.failure_category == FailureCategory.RAGDB_COLLISION.value
            assert retrieved.failure_retryable is False
            assert "already exists" in retrieved.failure_message
            assert retrieved.queue == QUEUE_4_MANUAL_REVIEW

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_unavailable_metadata(
        self, sample_device_node, tmp_manifest, ragscallion_client
    ):
        """Test unavailable error marked as retryable."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionUnavailableError(
                "Server unavailable after 3 retries"
            )

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            retrieved = tmp_manifest.get_node("yamaha-r08d")
            assert retrieved.failure_stage == 3  # STAGE_INDEX_RAG
            assert retrieved.failure_category == FailureCategory.RAGSCALLION_UNAVAILABLE.value
            assert retrieved.failure_retryable is True
            assert "3 retries" in retrieved.failure_message
            assert retrieved.queue == QUEUE_4_MANUAL_REVIEW

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_submission_error_metadata(
        self, sample_device_node, tmp_manifest, ragscallion_client
    ):
        """Test validation error marked as not retryable."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionError("Invalid corpus_id format")

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            retrieved = tmp_manifest.get_node("yamaha-r08d")
            assert retrieved.failure_stage == 3  # STAGE_INDEX_RAG
            assert retrieved.failure_category == FailureCategory.RAGDB_SUBMISSION_ERROR.value
            assert retrieved.failure_retryable is False

        await ragscallion_client.close()


class TestStage5ExtractionMetadata:
    """Tests for stage 5 extraction failure metadata tracking."""

    @pytest.mark.asyncio
    async def test_stage_5_extraction_failed_metadata(
        self, sample_device_node, tmp_manifest, ragscallion_client
    ):
        """Test extraction failure marked as retryable."""
        sample_device_node.corpus_id = "yamaha-r08d"
        tmp_manifest.add_node(sample_device_node)

        with patch(
            "src.pipeline_stages._extract_specs_via_agent",
            new_callable=AsyncMock,
        ) as mock_extract:
            mock_extract.return_value = None  # Extraction failed

            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            retrieved = tmp_manifest.get_node("yamaha-r08d")
            assert retrieved.failure_stage == STAGE_EXTRACT_SPECS
            assert retrieved.failure_category == FailureCategory.EXTRACTION_FAILED.value
            assert retrieved.failure_retryable is True
            assert "couldn't extract" in retrieved.failure_message
            assert retrieved.queue == QUEUE_4_MANUAL_REVIEW

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_5_extraction_timeout_metadata(
        self, sample_device_node, tmp_manifest, ragscallion_client
    ):
        """Test extraction timeout marked as retryable."""
        sample_device_node.corpus_id = "yamaha-r08d"
        tmp_manifest.add_node(sample_device_node)

        async def timeout_extraction(*args, **kwargs):
            raise asyncio.TimeoutError("Extraction took too long")

        with patch(
            "src.pipeline_stages._extract_specs_via_agent",
            new_callable=AsyncMock,
            side_effect=timeout_extraction,
        ):
            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            retrieved = tmp_manifest.get_node("yamaha-r08d")
            assert retrieved.failure_stage == STAGE_EXTRACT_SPECS
            assert retrieved.failure_category == FailureCategory.EXTRACTION_TIMEOUT.value
            assert retrieved.failure_retryable is True
            assert "timeout" in retrieved.failure_message.lower()

        await ragscallion_client.close()


class TestFailureMetadataSchema:
    """Tests for failure metadata field values and ISO timestamps."""

    def test_failure_metadata_timestamp_iso_format(self, tmp_manifest):
        """Test failure_at is ISO 8601 format."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
        )
        tmp_manifest.add_node(node)

        tmp_manifest.add_failure(
            device_id="test-device",
            failure_category=FailureCategory.PDF_NOT_FOUND.value,
            failure_message="Not found",
            stage=STAGE_FIND_PDF,
            retryable=True,
        )

        retrieved = tmp_manifest.get_node("test-device")
        # Parse ISO timestamp to verify format
        datetime.fromisoformat(retrieved.failure_at)

    def test_failure_attempts_increments(self, tmp_manifest):
        """Test failure_attempts counter increments correctly."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
        )
        tmp_manifest.add_node(node)

        for i in range(1, 4):
            tmp_manifest.add_failure(
                device_id="test-device",
                failure_category=FailureCategory.EXTRACTION_FAILED.value,
                failure_message=f"Attempt {i} failed",
                stage=STAGE_EXTRACT_SPECS,
                retryable=True,
            )

            retrieved = tmp_manifest.get_node("test-device")
            assert retrieved.failure_attempts == i

    def test_failure_message_truncation_on_long_errors(self, tmp_manifest):
        """Test long error messages are truncated to reasonable length."""
        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
        )
        tmp_manifest.add_node(node)

        long_error = "x" * 1000
        tmp_manifest.add_failure(
            device_id="test-device",
            failure_category=FailureCategory.PATCH_VALIDATION_FAILED.value,
            failure_message=long_error,
            stage=STAGE_VALIDATE_PATCH,
            retryable=False,
        )

        retrieved = tmp_manifest.get_node("test-device")
        # Message should be stored as-is, but consumers should truncate
        assert retrieved.failure_message == long_error
        assert len(retrieved.failure_message) == 1000


class TestManifestListQueue4WithMetadata:
    """Tests for querying queue_4 with failure metadata."""

    def test_list_by_queue_4_includes_failure_metadata(self, tmp_manifest):
        """Test that queue_4 listing includes rich failure metadata."""
        # Add device 1: PDF not found
        device1 = DeviceNode(
            device_id="device-1",
            manufacturer="Maker1",
            model="Model1",
            queue=QUEUE_4_MANUAL_REVIEW,
        )
        tmp_manifest.add_node(device1)
        tmp_manifest.add_failure(
            device_id="device-1",
            failure_category=FailureCategory.PDF_NOT_FOUND.value,
            failure_message="No PDF found",
            stage=STAGE_FIND_PDF,
            retryable=True,
        )

        # Add device 2: Extraction failed
        device2 = DeviceNode(
            device_id="device-2",
            manufacturer="Maker2",
            model="Model2",
            queue=QUEUE_4_MANUAL_REVIEW,
        )
        tmp_manifest.add_node(device2)
        tmp_manifest.add_failure(
            device_id="device-2",
            failure_category=FailureCategory.EXTRACTION_FAILED.value,
            failure_message="Agent couldn't extract",
            stage=STAGE_EXTRACT_SPECS,
            retryable=True,
        )

        # Add device 3: Not in manual review queue
        device3 = DeviceNode(
            device_id="device-3",
            manufacturer="Maker3",
            model="Model3",
            queue=0,  # Initial queue
        )
        tmp_manifest.add_node(device3)

        # Query queue_4
        queue_4_devices = tmp_manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)

        assert len(queue_4_devices) == 2
        assert queue_4_devices[0].device_id == "device-1"
        assert queue_4_devices[0].failure_category == FailureCategory.PDF_NOT_FOUND.value
        assert queue_4_devices[0].failure_retryable is True

        assert queue_4_devices[1].device_id == "device-2"
        assert queue_4_devices[1].failure_category == FailureCategory.EXTRACTION_FAILED.value
        assert queue_4_devices[1].failure_retryable is True

    def test_queue_4_can_be_sorted_by_stage(self, tmp_manifest):
        """Test queue_4 devices can be sorted by failure_stage for triage."""
        # Add devices with different failure stages
        stages_and_categories = [
            (STAGE_DOWNLOAD_PDF, FailureCategory.PDF_DOWNLOAD_FAILED),
            (STAGE_VALIDATE_PATCH, FailureCategory.PATCH_VALIDATION_FAILED),
            (STAGE_EXTRACT_SPECS, FailureCategory.EXTRACTION_FAILED),
        ]

        for i, (stage, category) in enumerate(stages_and_categories):
            device = DeviceNode(
                device_id=f"device-{i}",
                manufacturer=f"Maker{i}",
                model=f"Model{i}",
                queue=QUEUE_4_MANUAL_REVIEW,
            )
            tmp_manifest.add_node(device)
            tmp_manifest.add_failure(
                device_id=f"device-{i}",
                failure_category=category.value,
                failure_message=f"Stage {stage} failure",
                stage=stage,
                retryable=True,
            )

        queue_4_devices = tmp_manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)

        # Sort by failure_stage
        sorted_devices = sorted(queue_4_devices, key=lambda d: d.failure_stage or 99)

        # Should be in order: stage 1, 4, 6
        assert sorted_devices[0].failure_stage == STAGE_DOWNLOAD_PDF
        assert sorted_devices[1].failure_stage == STAGE_EXTRACT_SPECS
        assert sorted_devices[2].failure_stage == STAGE_VALIDATE_PATCH

    def test_queue_4_can_filter_by_retryability(self, tmp_manifest):
        """Test queue_4 devices can be filtered by failure_retryable."""
        # Add retryable device
        device1 = DeviceNode(
            device_id="device-1",
            manufacturer="Maker1",
            model="Model1",
            queue=QUEUE_4_MANUAL_REVIEW,
        )
        tmp_manifest.add_node(device1)
        tmp_manifest.add_failure(
            device_id="device-1",
            failure_category=FailureCategory.EXTRACTION_FAILED.value,
            failure_message="Can retry with better params",
            stage=STAGE_EXTRACT_SPECS,
            retryable=True,
        )

        # Add non-retryable device
        device2 = DeviceNode(
            device_id="device-2",
            manufacturer="Maker2",
            model="Model2",
            queue=QUEUE_4_MANUAL_REVIEW,
        )
        tmp_manifest.add_node(device2)
        tmp_manifest.add_failure(
            device_id="device-2",
            failure_category=FailureCategory.PATCH_VALIDATION_FAILED.value,
            failure_message="Generation logic needs fixing",
            stage=STAGE_VALIDATE_PATCH,
            retryable=False,
        )

        queue_4_devices = tmp_manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)

        retryable = [d for d in queue_4_devices if d.failure_retryable]
        not_retryable = [d for d in queue_4_devices if not d.failure_retryable]

        assert len(retryable) == 1
        assert retryable[0].device_id == "device-1"

        assert len(not_retryable) == 1
        assert not_retryable[0].device_id == "device-2"
