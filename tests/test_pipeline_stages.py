"""Tests for pipeline stages, starting with stage 3-4 Ragscallion submission."""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_4_MANUAL_REVIEW,
    DOC_TYPE_SPEC_SHEET,
    DOC_TYPE_USER_MANUAL,
    DOC_TYPE_INSTALL_GUIDE,
)
from src.pipeline_stages import stage_3_4_submit_to_ragscallion
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
        queue=0,  # Initial queue
    )
    return node


@pytest.fixture
def ragscallion_client():
    """Create a RagscallionClient instance."""
    return RagscallionClient(base_url="http://192.168.0.200:8086")


class TestStage34SubmitToRagscallion:
    """Tests for stage_3_4_submit_to_ragscallion function."""

    @pytest.mark.asyncio
    async def test_stage_3_4_success(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test successful submission to Ragscallion."""
        tmp_manifest.add_node(sample_device_node)

        expected_job = {
            "job_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            "corpus_id": "yamaha-r08d",
            "status": "queued",
            "server_now": "2026-04-29T10:30:45.123Z",
        }

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = expected_job

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is True
            mock_submit.assert_called_once_with(
                pdf_path=sample_device_node.pdf_path,
                corpus_id="yamaha-r08d",
                source_label="YAMAHA R08D",
                on_conflict="error",
            )

            # Verify node state after submission
            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.ragscallion_job_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
            assert updated_node.corpus_id == "yamaha-r08d"
            assert updated_node.ragscallion_submitted_at is not None
            assert updated_node.queue == QUEUE_2_POLLING_RAGSCALLION
            assert updated_node.stage_convert_marker == 1  # IN_PROGRESS
            assert updated_node.stage_index_rag == 1  # IN_PROGRESS

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_collision(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test collision handling (409) → move to queue_4 with RAGDB_COLLISION."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionCollisionError(
                "source_label 'YAMAHA R08D' already exists in corpus"
            )

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            # Verify node moved to queue_4
            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.queue == QUEUE_4_MANUAL_REVIEW
            assert updated_node.stage_index_rag == 3  # FAILED
            assert updated_node.failure_category == "RAGDB_COLLISION"
            assert "already in corpus" in updated_node.failure_message

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_unavailable(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test unavailable error (5xx after retries) → move to queue_4."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionUnavailableError(
                "Server returned 503 after 3 attempts"
            )

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            # Verify node moved to queue_4 with retryable=True
            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.queue == QUEUE_4_MANUAL_REVIEW
            assert updated_node.stage_index_rag == 3  # FAILED
            assert updated_node.failure_category == "RAGSCALLION_UNAVAILABLE"
            assert updated_node.failure_retryable is True
            assert "after 3 retries" in updated_node.failure_message

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_validation_error(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test validation error (4xx) → move to queue_4."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = RagscallionError(
                "Client error 400: Invalid corpus_id format"
            )

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False

            # Verify node moved to queue_4
            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.queue == QUEUE_4_MANUAL_REVIEW
            assert updated_node.stage_index_rag == 3  # FAILED
            assert updated_node.failure_category == "RAGDB_SUBMISSION_ERROR"
            assert "Invalid corpus_id" in updated_node.failure_message

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_stores_job_id_and_corpus(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that job metadata is stored correctly."""
        tmp_manifest.add_node(sample_device_node)

        job_response = {
            "job_id": "test-job-123",
            "corpus_id": "yamaha-r08d",
            "status": "queued",
            "server_now": "2026-04-29T10:30:45.123Z",
        }

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = job_response

            await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.ragscallion_job_id == "test-job-123"
            assert updated_node.corpus_id == "yamaha-r08d"

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_sets_stage_in_progress(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that stage progress is set to IN_PROGRESS (1) on success."""
        tmp_manifest.add_node(sample_device_node)
        assert sample_device_node.stage_convert_marker == 0
        assert sample_device_node.stage_index_rag == 0

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = {
                "job_id": "test-123",
                "corpus_id": "yamaha-r08d",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z",
            }

            await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.stage_convert_marker == 1  # IN_PROGRESS
            assert updated_node.stage_index_rag == 1  # IN_PROGRESS

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_timestamp_recorded(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that ragscallion_submitted_at is recorded as ISO timestamp."""
        tmp_manifest.add_node(sample_device_node)
        assert sample_device_node.ragscallion_submitted_at is None

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = {
                "job_id": "test-123",
                "corpus_id": "yamaha-r08d",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z",
            }

            before_call = datetime.now(timezone.utc).isoformat()
            await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )
            after_call = datetime.now(timezone.utc).isoformat()

            updated_node = tmp_manifest.get_node("yamaha-r08d")
            assert updated_node.ragscallion_submitted_at is not None

            # Verify timestamp is roughly within expected range
            submitted_ts = datetime.fromisoformat(
                updated_node.ragscallion_submitted_at
            )
            before_dt = datetime.fromisoformat(before_call)
            after_dt = datetime.fromisoformat(after_call)
            assert before_dt <= submitted_ts <= after_dt

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_uses_device_id_as_corpus(self, tmp_manifest, ragscallion_client, tmp_path):
        """Test that corpus_id is derived from device_id."""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\ntest")

        node = DeviceNode(
            device_id="custom-device-id",
            manufacturer="Test",
            model="Model",
            pdf_path=str(pdf_path),
        )
        tmp_manifest.add_node(node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = {
                "job_id": "test-123",
                "corpus_id": "custom-device-id",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z",
            }

            await stage_3_4_submit_to_ragscallion(
                node,
                ragscallion_client,
                tmp_manifest,
            )

            # Verify corpus_id argument matches device_id
            mock_submit.assert_called_once()
            call_kwargs = mock_submit.call_args.kwargs
            assert call_kwargs["corpus_id"] == "custom-device-id"

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_uses_manufacturer_model_as_source_label(
        self, tmp_manifest, ragscallion_client, tmp_path
    ):
        """Test that source_label is formatted as 'MANUFACTURER MODEL'."""
        pdf_path = tmp_path / "test.pdf"
        pdf_path.write_bytes(b"%PDF-1.4\ntest")

        node = DeviceNode(
            device_id="test-device",
            manufacturer="TestMfg",
            model="TestModel",
            pdf_path=str(pdf_path),
        )
        tmp_manifest.add_node(node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = {
                "job_id": "test-123",
                "corpus_id": "test-device",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z",
            }

            await stage_3_4_submit_to_ragscallion(
                node,
                ragscallion_client,
                tmp_manifest,
            )

            # Verify source_label is formatted correctly
            mock_submit.assert_called_once()
            call_kwargs = mock_submit.call_args.kwargs
            assert call_kwargs["source_label"] == "TestMfg TestModel"

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_stage_3_4_uses_error_on_conflict(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that on_conflict is set to 'error' (reject re-submissions)."""
        tmp_manifest.add_node(sample_device_node)

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.return_value = {
                "job_id": "test-123",
                "corpus_id": "yamaha-r08d",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z",
            }

            await stage_3_4_submit_to_ragscallion(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            # Verify on_conflict='error'
            mock_submit.assert_called_once()
            call_kwargs = mock_submit.call_args.kwargs
            assert call_kwargs["on_conflict"] == "error"

        await ragscallion_client.close()


class TestStage34MultiDocSubmission:
    """Tests for Phase C — secondary doc submission alongside spec_sheet."""

    @pytest.mark.asyncio
    async def test_secondaries_submitted_after_spec_sheet(
        self, sample_device_node, tmp_manifest, ragscallion_client, tmp_path
    ):
        """Spec_sheet + user_manual + install_guide each get submit_ingest calls
        and per-doc job_ids are recorded in device_documents."""
        sample_device_node.corpus_id = "yamaha-r08d"
        tmp_manifest.add_node(sample_device_node)

        # Seed the spec_sheet doc row to mirror Stage 1/2 behavior.
        tmp_manifest.add_document(
            sample_device_node.device_id,
            DOC_TYPE_SPEC_SHEET,
            url="https://example.com/r08d-datasheet.pdf",
            local_path=sample_device_node.pdf_path,
        )
        manual_path = tmp_path / "manual.pdf"
        manual_path.write_bytes(b"%PDF-manual")
        install_path = tmp_path / "install.pdf"
        install_path.write_bytes(b"%PDF-install")
        tmp_manifest.add_document(
            sample_device_node.device_id,
            DOC_TYPE_USER_MANUAL,
            url="https://example.com/r08d-manual.pdf",
            local_path=str(manual_path),
        )
        tmp_manifest.add_document(
            sample_device_node.device_id,
            DOC_TYPE_INSTALL_GUIDE,
            url="https://example.com/r08d-install.pdf",
            local_path=str(install_path),
        )

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = [
                {"job_id": "spec-job", "corpus_id": "yamaha-r08d", "status": "queued"},
                {"job_id": "manual-job", "corpus_id": "yamaha-r08d", "status": "queued"},
                {"job_id": "install-job", "corpus_id": "yamaha-r08d", "status": "queued"},
            ]

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node, ragscallion_client, tmp_manifest
            )

            assert result is True
            assert mock_submit.await_count == 3

            docs = tmp_manifest.list_documents(sample_device_node.device_id)
            by_type = {d.doc_type: d for d in docs}
            assert by_type[DOC_TYPE_SPEC_SHEET].ragscallion_job_id == "spec-job"
            assert by_type[DOC_TYPE_USER_MANUAL].ragscallion_job_id == "manual-job"
            assert by_type[DOC_TYPE_INSTALL_GUIDE].ragscallion_job_id == "install-job"
            for doc in docs:
                assert doc.indexed_at is None  # not indexed until polling sees ready

        await ragscallion_client.close()

    @pytest.mark.asyncio
    async def test_secondary_failure_does_not_fail_device(
        self, sample_device_node, tmp_manifest, ragscallion_client, tmp_path
    ):
        """Secondary submit raising → device still advances on spec_sheet success."""
        sample_device_node.corpus_id = "yamaha-r08d"
        tmp_manifest.add_node(sample_device_node)
        tmp_manifest.add_document(
            sample_device_node.device_id,
            DOC_TYPE_SPEC_SHEET,
            url="https://example.com/r08d-datasheet.pdf",
            local_path=sample_device_node.pdf_path,
        )
        manual_path = tmp_path / "manual.pdf"
        manual_path.write_bytes(b"%PDF-manual")
        tmp_manifest.add_document(
            sample_device_node.device_id,
            DOC_TYPE_USER_MANUAL,
            url="https://example.com/r08d-manual.pdf",
            local_path=str(manual_path),
        )

        with patch.object(
            ragscallion_client, "submit_ingest", new_callable=AsyncMock
        ) as mock_submit:
            mock_submit.side_effect = [
                {"job_id": "spec-job", "corpus_id": "yamaha-r08d", "status": "queued"},
                RagscallionError("503 service unavailable"),
            ]

            result = await stage_3_4_submit_to_ragscallion(
                sample_device_node, ragscallion_client, tmp_manifest
            )

            assert result is True  # spec_sheet succeeded → device advances
            updated = tmp_manifest.get_node(sample_device_node.device_id)
            assert updated.queue == QUEUE_2_POLLING_RAGSCALLION

            docs = tmp_manifest.list_documents(sample_device_node.device_id)
            by_type = {d.doc_type: d for d in docs}
            assert by_type[DOC_TYPE_SPEC_SHEET].ragscallion_job_id == "spec-job"
            assert by_type[DOC_TYPE_USER_MANUAL].ragscallion_job_id is None

        await ragscallion_client.close()
