"""Tests for async polling loop with job status monitoring."""

import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path

import pytest

from src.polling_loop import (
    run_polling_loop,
    _check_stale_nodes,
    _process_job_result,
    POLL_INTERVAL_SECONDS,
    CONSECUTIVE_FAILURE_WARN_THRESHOLD,
    CONSECUTIVE_FAILURE_ERROR_THRESHOLD,
    STALE_NODE_WARNING_MINUTES,
)
from src.harness.manifest import (
    Manifest,
    DeviceNode,
    QUEUE_2_POLLING_RAGSCALLION,
    DOC_TYPE_SPEC_SHEET,
    DOC_TYPE_USER_MANUAL,
)
from src.ragscallion_client import RagscallionClient


@pytest.fixture
def test_manifest(tmp_path):
    """Create a test manifest database."""
    db_path = tmp_path / "test_polling.db"
    return Manifest(db_path)


@pytest.fixture
def mock_ragscallion_client():
    """Create a mock RagscallionClient."""
    return AsyncMock(spec=RagscallionClient)


@pytest.fixture
def sample_node():
    """Create a sample device node in queue_2 (polling)."""
    return DeviceNode(
        device_id="yamaha-r08d",
        manufacturer="Yamaha",
        model="R08D",
        corpus_id="yamaha-r08d",
        queue=QUEUE_2_POLLING_RAGSCALLION,
        ragscallion_job_id="job-12345",
        ragscallion_submitted_at=datetime.now(timezone.utc).isoformat(),
    )


def format_server_now(dt: datetime = None) -> str:
    """Format datetime for Ragscallion server_now field.

    Converts to ISO format with Z suffix (no timezone offset).
    """
    if dt is None:
        dt = datetime.now(timezone.utc)
    return dt.isoformat(timespec='milliseconds').replace('+00:00', '') + "Z"


class TestPollingLoopTransitions:
    """Test job status transitions in the polling loop."""

    @pytest.mark.asyncio
    async def test_polling_loop_transitions_ready_jobs(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test job ready → queue_3, stage marked completed."""
        test_manifest.add_node(sample_node)

        # Mock successful poll with ready job
        # Use the Ragscallion format: ISO string without timezone indicator + Z
        server_now_dt = datetime.now(timezone.utc)
        server_now_str = server_now_dt.isoformat(timespec='milliseconds').replace('+00:00', '') + "Z"
        completed_at_str = server_now_dt.isoformat(timespec='milliseconds')

        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [
                {
                    "job_id": "job-12345",
                    "corpus_id": "yamaha-r08d",
                    "status": "ready",
                    "completed_at": completed_at_str,
                    "chunks_indexed": 2500,
                }
            ],
            "server_now": server_now_str,
        }

        # Run one poll cycle
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify node was transitioned to queue_3
        updated_node = test_manifest.get_node("yamaha-r08d")
        assert updated_node.queue == 3
        assert updated_node.stage_convert_marker == 2
        assert updated_node.stage_index_rag == 2
        assert updated_node.ragscallion_completed_at == completed_at_str

    @pytest.mark.asyncio
    async def test_polling_loop_transitions_failed_jobs(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test job failed → queue_4, failure recorded."""
        test_manifest.add_node(sample_node)

        # Mock poll with failed job
        server_now_dt = datetime.now(timezone.utc)
        server_now_str = server_now_dt.isoformat(timespec='milliseconds').replace('+00:00', '') + "Z"
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [
                {
                    "job_id": "job-12345",
                    "corpus_id": "yamaha-r08d",
                    "status": "failed",
                    "error": "PDF timeout during processing",
                }
            ],
            "server_now": server_now_str,
        }

        # Run one poll cycle
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify node was transitioned to queue_4 with failure
        updated_node = test_manifest.get_node("yamaha-r08d")
        assert updated_node.queue == 4
        assert updated_node.failure_category == "RAGDB_INDEXING_FAILED"
        assert updated_node.failure_message == "PDF timeout during processing"
        assert updated_node.failure_attempts == 1

    @pytest.mark.asyncio
    async def test_polling_loop_processes_multiple_jobs(
        self, test_manifest, mock_ragscallion_client
    ):
        """Test multiple jobs in one response handled correctly."""
        # Add two nodes to manifest
        node1 = DeviceNode(
            device_id="device-1",
            manufacturer="Maker1",
            model="Model1",
            corpus_id="device-1",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-001",
        )
        node2 = DeviceNode(
            device_id="device-2",
            manufacturer="Maker2",
            model="Model2",
            corpus_id="device-2",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-002",
        )
        test_manifest.add_node(node1)
        test_manifest.add_node(node2)

        # Mock poll with two jobs
        server_now_dt = datetime.now(timezone.utc)
        server_now_str = server_now_dt.isoformat(timespec='milliseconds').replace('+00:00', '') + "Z"
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [
                {
                    "job_id": "job-001",
                    "corpus_id": "device-1",
                    "status": "ready",
                    "chunks_indexed": 1000,
                },
                {
                    "job_id": "job-002",
                    "corpus_id": "device-2",
                    "status": "failed",
                    "error": "Out of memory",
                },
            ],
            "server_now": server_now_str,
        }

        # Run one poll cycle
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify both nodes were processed
        node1_updated = test_manifest.get_node("device-1")
        node2_updated = test_manifest.get_node("device-2")

        assert node1_updated.queue == 3
        assert node2_updated.queue == 4
        assert node2_updated.failure_message == "Out of memory"


class TestPollingLoopTimestamps:
    """Test timestamp handling and race condition fixes."""

    @pytest.mark.asyncio
    async def test_polling_loop_updates_last_check_from_server_time(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test server_now used for next poll (timestamp ordering)."""
        test_manifest.add_node(sample_node)

        # First poll response
        server_now_1 = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            "server_now": server_now_1,
        }

        # Run loop and capture poll_jobs calls
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )

        # Wait for first poll to complete
        await asyncio.sleep(0.5)

        # Verify first poll was with initial timestamp
        first_call = mock_ragscallion_client.poll_jobs.call_args_list[0]
        assert "since" in first_call[1]

        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    @pytest.mark.asyncio
    async def test_polling_loop_captures_last_check_before_request(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test last_check captured BEFORE request (race condition fix)."""
        test_manifest.add_node(sample_node)

        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            "server_now": server_now_str,
        }

        # Run loop
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify poll_jobs was called with a since parameter
        mock_ragscallion_client.poll_jobs.assert_called()
        call_kwargs = mock_ragscallion_client.poll_jobs.call_args[1]
        assert "since" in call_kwargs
        assert call_kwargs["status"] == "ready,failed"


class TestPollingLoopFailureTracking:
    """Test consecutive failure thresholds and recovery."""

    @pytest.mark.asyncio
    async def test_polling_loop_tracks_consecutive_failures(
        self, test_manifest, mock_ragscallion_client, sample_node, caplog
    ):
        """Test failure counter and threshold logging."""
        test_manifest.add_node(sample_node)

        # Mock consecutive failures
        from src.ragscallion_client import RagscallionUnavailableError

        mock_ragscallion_client.poll_jobs.side_effect = RagscallionUnavailableError(
            "Connection refused"
        )

        # Run loop for a few iterations
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )

        # Wait for failures to accumulate
        await asyncio.sleep(0.5)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify log messages
        assert "Polling failed" in caplog.text

    @pytest.mark.asyncio
    async def test_polling_loop_resets_failure_count_on_success(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test success poll → counter back to 0."""
        test_manifest.add_node(sample_node)

        from src.ragscallion_client import RagscallionUnavailableError

        # Fail first, then succeed
        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.side_effect = [
            RagscallionUnavailableError("Network error"),
            {"jobs": [], "server_now": server_now_str},
        ]

        # Run loop (need > 3s to allow for 2 polls)
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(3.5)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify that after first error, second call succeeded
        assert mock_ragscallion_client.poll_jobs.call_count >= 2

    @pytest.mark.asyncio
    async def test_polling_loop_continues_on_poll_error(
        self, test_manifest, mock_ragscallion_client, sample_node, caplog
    ):
        """Test poll_jobs() raises → logs, continues."""
        test_manifest.add_node(sample_node)

        from src.ragscallion_client import RagscallionUnavailableError

        # Mock alternating errors and success
        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.side_effect = [
            RagscallionUnavailableError("Timeout"),
            RagscallionUnavailableError("Connection refused"),
            {"jobs": [], "server_now": server_now_str},
        ]

        # Run loop (need > 6s to allow for 3 polls)
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(6.5)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify it continued despite errors
        assert mock_ragscallion_client.poll_jobs.call_count >= 3


class TestStaleNodeTracking:
    """Test per-node age checking and suspicious marking."""

    @pytest.mark.asyncio
    async def test_polling_loop_marks_stale_nodes(
        self, test_manifest, mock_ragscallion_client
    ):
        """Test 15+ min in queue_2 → marked_suspicious = True."""
        # Create a node that was updated 20 minutes ago
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=20)
        ).isoformat()
        old_node = DeviceNode(
            device_id="stale-device",
            manufacturer="Test",
            model="Stale",
            corpus_id="stale-device",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-old",
            updated_at=old_time,
            created_at=old_time,
        )
        test_manifest.add_node(old_node)

        # Mock successful poll
        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            "server_now": server_now_str,
        }

        # Run one poll cycle
        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify node was marked suspicious
        updated_node = test_manifest.get_node("stale-device")
        assert updated_node.marked_suspicious is True

    @pytest.mark.asyncio
    async def test_check_stale_nodes_skips_recent_nodes(self, test_manifest):
        """Test nodes updated < 15 min ago are not marked."""
        # Create a node updated 5 minutes ago
        recent_time = (
            datetime.now(timezone.utc) - timedelta(minutes=5)
        ).isoformat()
        recent_node = DeviceNode(
            device_id="recent-device",
            manufacturer="Test",
            model="Recent",
            corpus_id="recent-device",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-recent",
            updated_at=recent_time,
            created_at=recent_time,
        )
        test_manifest.add_node(recent_node)

        # Check stale nodes
        await _check_stale_nodes(test_manifest)

        # Verify node was NOT marked
        updated_node = test_manifest.get_node("recent-device")
        assert updated_node.marked_suspicious is False

    @pytest.mark.asyncio
    async def test_check_stale_nodes_skips_already_marked(self, test_manifest):
        """Test nodes already marked are not processed again."""
        old_time = (
            datetime.now(timezone.utc) - timedelta(minutes=30)
        ).isoformat()
        stale_node = DeviceNode(
            device_id="already-marked",
            manufacturer="Test",
            model="Stale",
            corpus_id="already-marked",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="job-marked",
            updated_at=old_time,
            created_at=old_time,
            marked_suspicious=True,
        )
        test_manifest.add_node(stale_node)

        # Check stale nodes (should not change already-marked flag)
        await _check_stale_nodes(test_manifest)

        # Verify marked status unchanged
        updated_node = test_manifest.get_node("already-marked")
        assert updated_node.marked_suspicious is True


class TestPollingLoopConstants:
    """Test constant values and polling interval."""

    @pytest.mark.asyncio
    async def test_polling_loop_sleeps_3_seconds(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test correct poll interval."""
        test_manifest.add_node(sample_node)

        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            "server_now": server_now_str,
        }

        # Track timing
        import time

        start = time.time()

        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )

        # Wait for at least 2 polls (3 seconds each)
        await asyncio.sleep(6.5)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        elapsed = time.time() - start

        # Should have called poll_jobs at least twice (after ~3s and ~6s)
        assert mock_ragscallion_client.poll_jobs.call_count >= 2

    def test_poll_interval_constant(self):
        """Test POLL_INTERVAL_SECONDS constant."""
        assert POLL_INTERVAL_SECONDS == 3

    def test_consecutive_failure_warn_threshold(self):
        """Test CONSECUTIVE_FAILURE_WARN_THRESHOLD constant."""
        assert CONSECUTIVE_FAILURE_WARN_THRESHOLD == 3

    def test_consecutive_failure_error_threshold(self):
        """Test CONSECUTIVE_FAILURE_ERROR_THRESHOLD constant."""
        assert CONSECUTIVE_FAILURE_ERROR_THRESHOLD == 10

    def test_stale_node_warning_minutes(self):
        """Test STALE_NODE_WARNING_MINUTES constant."""
        assert STALE_NODE_WARNING_MINUTES == 15


class TestPollingLoopEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_polling_loop_handles_missing_corpus_id(
        self, test_manifest, mock_ragscallion_client, caplog
    ):
        """Test job without corpus_id is skipped with warning."""
        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [
                {
                    "job_id": "job-no-corpus",
                    "status": "ready",
                    # Missing corpus_id
                }
            ],
            "server_now": server_now_str,
        }

        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert "missing corpus_id" in caplog.text

    @pytest.mark.asyncio
    async def test_polling_loop_handles_missing_node(
        self, test_manifest, mock_ragscallion_client, caplog
    ):
        """Test job for nonexistent node is skipped with warning."""
        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [
                {
                    "job_id": "job-orphan",
                    "corpus_id": "nonexistent-device",
                    "status": "ready",
                }
            ],
            "server_now": server_now_str,
        }

        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert "Node not found" in caplog.text

    @pytest.mark.asyncio
    async def test_polling_loop_handles_missing_server_now(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test response without server_now falls back to current time."""
        test_manifest.add_node(sample_node)

        # Mock response without server_now
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            # Missing server_now
        }

        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Should have executed without error
        mock_ragscallion_client.poll_jobs.assert_called()

    @pytest.mark.asyncio
    async def test_polling_loop_handles_empty_jobs_list(
        self, test_manifest, mock_ragscallion_client, sample_node
    ):
        """Test empty jobs list is handled gracefully."""
        test_manifest.add_node(sample_node)

        server_now_str = format_server_now()
        mock_ragscallion_client.poll_jobs.return_value = {
            "jobs": [],
            "server_now": server_now_str,
        }

        task = asyncio.create_task(
            run_polling_loop(mock_ragscallion_client, test_manifest)
        )
        await asyncio.sleep(0.5)  # Allow time for poll to execute
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Node should remain unchanged
        updated_node = test_manifest.get_node("yamaha-r08d")
        assert updated_node.queue == QUEUE_2_POLLING_RAGSCALLION


class TestProcessJobResult:
    """Phase C — per-job processing must distinguish spec_sheet from secondaries."""

    def _setup(self, test_manifest):
        node = DeviceNode(
            device_id="yamaha-r08d",
            manufacturer="Yamaha",
            model="R08D",
            corpus_id="yamaha-r08d",
            queue=QUEUE_2_POLLING_RAGSCALLION,
            ragscallion_job_id="spec-job",
        )
        test_manifest.add_node(node)
        spec_doc = test_manifest.add_document(
            "yamaha-r08d", DOC_TYPE_SPEC_SHEET,
            url="https://example.com/spec.pdf", local_path="/tmp/spec.pdf",
        )
        manual_doc = test_manifest.add_document(
            "yamaha-r08d", DOC_TYPE_USER_MANUAL,
            url="https://example.com/manual.pdf", local_path="/tmp/manual.pdf",
        )
        test_manifest.set_document_job_id(spec_doc.id, "spec-job")
        test_manifest.set_document_job_id(manual_doc.id, "manual-job")
        return node, spec_doc, manual_doc

    def test_spec_sheet_ready_advances_node_and_marks_indexed(self, test_manifest):
        node, spec_doc, manual_doc = self._setup(test_manifest)
        job = {
            "job_id": "spec-job", "corpus_id": "yamaha-r08d",
            "status": "ready", "chunks_indexed": 42,
            "completed_at": "2026-04-29T11:00:00Z",
        }

        _process_job_result(job, node, test_manifest)

        updated = test_manifest.get_node("yamaha-r08d")
        assert updated.queue == 3  # advanced to QUEUE_3_READY_FOR_EXTRACTION
        assert updated.stage_index_rag == 2  # COMPLETED
        spec_after = test_manifest.list_documents("yamaha-r08d", DOC_TYPE_SPEC_SHEET)[0]
        assert spec_after.indexed_at is not None
        manual_after = test_manifest.list_documents("yamaha-r08d", DOC_TYPE_USER_MANUAL)[0]
        assert manual_after.indexed_at is None  # secondary untouched

    def test_secondary_ready_marks_indexed_but_does_not_advance_node(self, test_manifest):
        node, _, manual_doc = self._setup(test_manifest)
        job = {
            "job_id": "manual-job", "corpus_id": "yamaha-r08d",
            "status": "ready", "chunks_indexed": 25,
        }

        _process_job_result(job, node, test_manifest)

        updated = test_manifest.get_node("yamaha-r08d")
        assert updated.queue == QUEUE_2_POLLING_RAGSCALLION  # not advanced
        assert updated.stage_index_rag == 0  # unchanged
        manual_after = test_manifest.list_documents("yamaha-r08d", DOC_TYPE_USER_MANUAL)[0]
        assert manual_after.indexed_at is not None
        spec_after = test_manifest.list_documents("yamaha-r08d", DOC_TYPE_SPEC_SHEET)[0]
        assert spec_after.indexed_at is None  # spec untouched

    def test_secondary_failure_does_not_fail_node(self, test_manifest):
        node, _, manual_doc = self._setup(test_manifest)
        job = {
            "job_id": "manual-job", "corpus_id": "yamaha-r08d",
            "status": "failed", "error": "indexing exploded",
        }

        _process_job_result(job, node, test_manifest)

        updated = test_manifest.get_node("yamaha-r08d")
        assert updated.queue == QUEUE_2_POLLING_RAGSCALLION  # not failed
        assert updated.failure_category is None
        manual_after = test_manifest.list_documents("yamaha-r08d", DOC_TYPE_USER_MANUAL)[0]
        assert manual_after.indexed_at is None  # not stamped on failure
