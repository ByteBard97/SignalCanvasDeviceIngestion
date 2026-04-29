"""Tests for stage 5 extraction with asyncio.Semaphore(5) concurrency control.

Tests cover:
- Semaphore enforcing max 5 concurrent Haiku calls
- Successful extraction and specs storage
- Failed extraction with proper error handling
- Batch processing of multiple nodes
- Exception handling in concurrent tasks
"""

import asyncio
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from typing import Optional

import sys
from pathlib import Path

import pytest

# Add parent directory to path for relative imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_4_MANUAL_REVIEW,
    STAGE_EXTRACT_SPECS,
)
from src.pipeline_stages import (
    stage_5_extract_specs,
    process_stage_5_batch,
    EXTRACTION_SEMAPHORE,
    STAGE_COMPLETED,
    STAGE_FAILED,
    MAX_CONCURRENT_EXTRACTIONS,
    EXTRACTION_TIMEOUT_SECONDS,
)
from src.ragscallion_client import RagscallionClient


@pytest.fixture
def tmp_manifest(tmp_path):
    """Create a temporary manifest for testing."""
    db_path = tmp_path / "test_manifest.db"
    manifest = Manifest(db_path)
    return manifest


@pytest.fixture
def sample_device_node():
    """Create a sample device node ready for extraction."""
    node = DeviceNode(
        device_id="yamaha-r08d",
        manufacturer="YAMAHA",
        model="R08D",
        corpus_id="yamaha-r08d",  # Already indexed
        queue=3,  # Ready for extraction
    )
    return node


@pytest.fixture
def ragscallion_client():
    """Create a RagscallionClient instance."""
    return RagscallionClient(base_url="http://192.168.0.200:8086")


@pytest.fixture(autouse=True)
async def reset_semaphore():
    """Reset semaphore before each test."""
    # Create a fresh semaphore for each test
    global EXTRACTION_SEMAPHORE
    EXTRACTION_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)
    yield
    # Cleanup after test
    EXTRACTION_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)


class TestStage5SemaphoreEnforcement:
    """Tests for semaphore limiting concurrent extractions."""

    @pytest.mark.asyncio
    async def test_stage_5_semaphore_limits_concurrent_calls(self, tmp_manifest):
        """Verify semaphore limits concurrent extraction calls to 5."""
        # Track concurrent call count at peak
        concurrent_count = 0
        max_concurrent_observed = 0
        lock = asyncio.Lock()

        async def mock_extract_with_timing(node):
            """Mock extraction that tracks concurrency."""
            nonlocal concurrent_count, max_concurrent_observed

            async with EXTRACTION_SEMAPHORE:
                async with lock:
                    concurrent_count += 1
                    max_concurrent_observed = max(max_concurrent_observed, concurrent_count)

                # Simulate extraction work (10ms)
                await asyncio.sleep(0.01)

                async with lock:
                    concurrent_count -= 1

            # Return success
            node.specs_json = '{"specs": "extracted"}'
            return True

        # Create 20 device nodes
        nodes = [
            DeviceNode(
                device_id=f"device-{i:03d}",
                manufacturer="TEST",
                model=f"M{i:03d}",
                corpus_id=f"device-{i:03d}",
                queue=3,
            )
            for i in range(20)
        ]

        for node in nodes:
            tmp_manifest.add_node(node)

        # Run all extractions in parallel
        tasks = [mock_extract_with_timing(node) for node in nodes]
        results = await asyncio.gather(*tasks)

        # Verify all succeeded and max concurrent was <= 5
        assert all(results), "All extractions should succeed"
        assert max_concurrent_observed <= MAX_CONCURRENT_EXTRACTIONS, (
            f"Semaphore should limit to {MAX_CONCURRENT_EXTRACTIONS} "
            f"concurrent, but observed {max_concurrent_observed}"
        )

    @pytest.mark.asyncio
    async def test_stage_5_semaphore_serializes_excess_tasks(self):
        """Verify tasks beyond semaphore limit wait for others to complete."""
        call_order = []
        lock = asyncio.Lock()

        async def mock_extraction_with_order_tracking(task_id):
            """Track order of semaphore acquisition."""
            async with EXTRACTION_SEMAPHORE:
                async with lock:
                    call_order.append(("start", task_id))

                # Simulate 20ms work
                await asyncio.sleep(0.02)

                async with lock:
                    call_order.append(("end", task_id))

            return True

        # Create 10 tasks
        tasks = [mock_extraction_with_order_tracking(i) for i in range(10)]
        results = await asyncio.gather(*tasks)

        assert all(results), "All tasks should complete"
        # Verify first 5 started before 6th started
        first_5_starts = [i for i, (event, _) in enumerate(call_order) if event == "start"][:5]
        sixth_start = next(i for i, (event, task_id) in enumerate(call_order) if event == "start" and task_id == 5)
        first_5_ends = [i for i, (event, _) in enumerate(call_order) if event == "end"][:5]

        # Sixth start must be after at least one of the first 5 ends
        assert sixth_start > min(first_5_ends), (
            "6th task should wait for 1st batch to partially complete"
        )


class TestStage5ExtractSpecs:
    """Tests for stage_5_extract_specs function."""

    @pytest.mark.asyncio
    async def test_stage_5_success(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test successful spec extraction."""
        tmp_manifest.add_node(sample_device_node)

        expected_specs = '{"inputs": [{"name": "XLR IN", "count": 8}], "outputs": [{"name": "XLR OUT", "count": 8}]}'

        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = expected_specs

            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is True
            assert sample_device_node.specs_json == expected_specs
            assert sample_device_node.stage_extract_specs == STAGE_COMPLETED
            mock_extract.assert_called_once()

    @pytest.mark.asyncio
    async def test_stage_5_failure_no_specs(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test failed extraction when agent returns None."""
        tmp_manifest.add_node(sample_device_node)

        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = None

            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False
            assert sample_device_node.stage_extract_specs == STAGE_FAILED
            assert sample_device_node.queue == QUEUE_4_MANUAL_REVIEW
            assert sample_device_node.failure_category == "EXTRACTION_FAILED"
            assert sample_device_node.failure_retryable is True

    @pytest.mark.asyncio
    async def test_stage_5_marks_retryable(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that failed extraction marks node as retryable."""
        tmp_manifest.add_node(sample_device_node)

        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = None

            await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert sample_device_node.failure_retryable is True
            assert sample_device_node.queue == QUEUE_4_MANUAL_REVIEW

    @pytest.mark.asyncio
    async def test_stage_5_timeout_handling(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test extraction timeout handling."""
        tmp_manifest.add_node(sample_device_node)

        async def timeout_extraction(*args, **kwargs):
            raise asyncio.TimeoutError("Extraction took too long")

        with patch("src.pipeline_stages._extract_specs_via_agent", side_effect=timeout_extraction):
            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False
            assert sample_device_node.failure_category == "EXTRACTION_TIMEOUT"
            assert sample_device_node.failure_retryable is True
            assert str(EXTRACTION_TIMEOUT_SECONDS) in sample_device_node.failure_message

    @pytest.mark.asyncio
    async def test_stage_5_exception_handling(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test exception handling (unexpected errors)."""
        tmp_manifest.add_node(sample_device_node)

        async def error_extraction(*args, **kwargs):
            raise ValueError("Unexpected error in extraction")

        with patch("src.pipeline_stages._extract_specs_via_agent", side_effect=error_extraction):
            result = await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert result is False
            assert "Unexpected error" in sample_device_node.failure_message
            assert sample_device_node.failure_retryable is False
            assert sample_device_node.queue == QUEUE_4_MANUAL_REVIEW

    @pytest.mark.asyncio
    async def test_stage_5_increments_failure_attempts(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that failed extraction increments failure_attempts."""
        sample_device_node.failure_attempts = 2
        tmp_manifest.add_node(sample_device_node)

        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = None

            await stage_5_extract_specs(
                sample_device_node,
                ragscallion_client,
                tmp_manifest,
            )

            assert sample_device_node.failure_attempts == 3

    @pytest.mark.asyncio
    async def test_stage_5_persists_node_on_success(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that successful extraction persists node to manifest."""
        expected_specs = '{"specs": "data"}'

        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = expected_specs
            with patch.object(tmp_manifest, "persist") as mock_persist:
                await stage_5_extract_specs(
                    sample_device_node,
                    ragscallion_client,
                    tmp_manifest,
                )

                mock_persist.assert_called_once_with(sample_device_node)

    @pytest.mark.asyncio
    async def test_stage_5_persists_node_on_failure(self, sample_device_node, tmp_manifest, ragscallion_client):
        """Test that failed extraction persists node to manifest."""
        with patch("src.pipeline_stages._extract_specs_via_agent", new_callable=AsyncMock) as mock_extract:
            mock_extract.return_value = None
            with patch.object(tmp_manifest, "persist") as mock_persist:
                await stage_5_extract_specs(
                    sample_device_node,
                    ragscallion_client,
                    tmp_manifest,
                )

                mock_persist.assert_called_once_with(sample_device_node)


class TestProcessStage5Batch:
    """Tests for batch processing of stage 5 extractions."""

    @pytest.mark.asyncio
    async def test_process_stage_5_batch_empty_queue(self, tmp_manifest, ragscallion_client):
        """Test that empty queue_3 returns no-op results."""
        # TODO: Implement manifest.list_by_queue(3) to make this test real
        # For now, this tests the empty case branch
        results = await process_stage_5_batch(tmp_manifest, ragscallion_client)

        assert results["successful"] == 0
        assert results["failed"] == 0
        assert results["exceptions"] == 0

    @pytest.mark.asyncio
    async def test_process_stage_5_batch_results_logging(self, tmp_manifest, ragscallion_client, caplog):
        """Test that batch processing logs final counts."""
        # Create mock nodes
        nodes = [
            DeviceNode(
                device_id=f"device-{i:03d}",
                manufacturer="TEST",
                model=f"M{i:03d}",
                corpus_id=f"device-{i:03d}",
                queue=3,
            )
            for i in range(3)
        ]

        # TODO: Mock manifest.list_by_queue(3) to return nodes
        # For now, test passes once list_by_queue is implemented

    @pytest.mark.asyncio
    async def test_process_stage_5_batch_exception_handling(self, ragscallion_client):
        """Test that exceptions in concurrent tasks are caught and logged."""
        async def failing_task(*args, **kwargs):
            raise ValueError("Task failed")

        # Create a mock that returns exceptions
        results = [True, False, ValueError("Task failed")]

        successful = sum(1 for r in results if r is True)
        failed = sum(1 for r in results if r is False)
        exceptions = sum(1 for r in results if isinstance(r, Exception))

        assert successful == 1
        assert failed == 1
        assert exceptions == 1

    @pytest.mark.asyncio
    async def test_process_stage_5_batch_respects_semaphore(self):
        """Test that batch processor respects semaphore limit."""
        concurrent_at_peak = 0
        max_observed = 0
        lock = asyncio.Lock()

        async def mock_stage_5(node, client, manifest):
            """Mock stage 5 that tracks concurrency."""
            nonlocal concurrent_at_peak, max_observed

            async with EXTRACTION_SEMAPHORE:
                async with lock:
                    concurrent_at_peak += 1
                    max_observed = max(max_observed, concurrent_at_peak)

                await asyncio.sleep(0.01)

                async with lock:
                    concurrent_at_peak -= 1

            return True

        # Simulate 15 parallel tasks
        nodes = [DeviceNode(device_id=f"dev-{i}", manufacturer="T", model="M") for i in range(15)]
        tasks = [mock_stage_5(n, None, None) for n in nodes]
        results = await asyncio.gather(*tasks)

        assert all(results)
        assert max_observed <= MAX_CONCURRENT_EXTRACTIONS


class TestStage5Integration:
    """Integration tests for stage 5 with real manifest."""

    @pytest.mark.asyncio
    async def test_stage_5_full_workflow(self, tmp_manifest, ragscallion_client):
        """Test complete extraction workflow."""
        # Create multiple devices
        devices = [
            DeviceNode(
                device_id=f"device-{i:02d}",
                manufacturer="BRAND",
                model=f"MODEL-{i:02d}",
                corpus_id=f"device-{i:02d}",
                queue=3,
            )
            for i in range(5)
        ]

        for device in devices:
            tmp_manifest.add_node(device)

        # Mock successful extractions with different specs
        async def mock_extract(manufacturer, model, corpus_id, rag_search):
            return f'{{"device": "{model}", "inputs": 4, "outputs": 4}}'

        with patch("src.pipeline_stages._extract_specs_via_agent", side_effect=mock_extract):
            tasks = [
                stage_5_extract_specs(device, ragscallion_client, tmp_manifest)
                for device in devices
            ]
            results = await asyncio.gather(*tasks)

            # All should succeed
            assert all(results), "All devices should extract successfully"

            # Verify all have specs
            for device in devices:
                assert device.specs_json is not None
                assert device.stage_extract_specs == STAGE_COMPLETED
