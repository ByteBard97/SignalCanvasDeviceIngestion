"""Tests for Stage 1 — Find PDF URL via Kimi CLI."""

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_1_CANNOT_FIND_PDF,
)
from src.pipeline_stages import STAGE_COMPLETED, STAGE_FAILED
from src.pipeline_stages import stage_1_find_pdf


@pytest.fixture
def tmp_manifest(tmp_path):
    """Create a temporary manifest for testing."""
    db_path = tmp_path / "test_manifest.db"
    return Manifest(db_path)


@pytest.fixture
def sample_node():
    """Create a sample device node."""
    return DeviceNode(
        device_id="yamaha-rio1608-d2",
        manufacturer="YAMAHA",
        model="Rio1608-D2",
    )


class TestStage1FindPDF:
    """Tests for stage_1_find_pdf."""

    @pytest.mark.asyncio
    async def test_stage_1_success(self, sample_node, tmp_manifest):
        """Mock Kimi returning valid JSON; assert node fields are set correctly."""
        tmp_manifest.add_node(sample_node)

        mock_stdout = json.dumps({"pdf_url": "https://example.com/datasheet.pdf"})

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = mock_stdout

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.pdf_url == "https://example.com/datasheet.pdf"
            assert updated.stage_find_pdf == STAGE_COMPLETED
            assert updated.queue == 0  # Still in initial queue

    @pytest.mark.asyncio
    async def test_stage_1_garbage_response(self, sample_node, tmp_manifest):
        """Kimi returns garbage → node ends up in queue_1 with failure metadata."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = "This is not JSON at all"

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.queue == QUEUE_1_CANNOT_FIND_PDF
            assert updated.stage_find_pdf == STAGE_FAILED
            assert updated.failure_category == "PDF_NOT_FOUND"
            assert updated.failure_retryable is True

    @pytest.mark.asyncio
    async def test_stage_1_missing_pdf_url_key(self, sample_node, tmp_manifest):
        """Kimi returns JSON without pdf_url key → failure."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = json.dumps({"url": "https://example.com/datasheet.pdf"})

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.failure_category == "PDF_NOT_FOUND"

    @pytest.mark.asyncio
    async def test_stage_1_non_pdf_url_with_bad_head(self, sample_node, tmp_manifest):
        """URL doesn't end in .pdf and HEAD returns non-PDF → failure."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi, patch(
            "src.pipeline_stages._verify_pdf_content_type", new_callable=AsyncMock
        ) as mock_verify:
            mock_kimi.return_value = json.dumps({"pdf_url": "https://example.com/datasheet.html"})
            mock_verify.return_value = False

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.failure_category == "PDF_NOT_FOUND"
            mock_verify.assert_awaited_once_with("https://example.com/datasheet.html")

    @pytest.mark.asyncio
    async def test_stage_1_non_pdf_url_with_good_head(self, sample_node, tmp_manifest):
        """URL doesn't end in .pdf but HEAD confirms application/pdf → success."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi, patch(
            "src.pipeline_stages._verify_pdf_content_type", new_callable=AsyncMock
        ) as mock_verify:
            mock_kimi.return_value = json.dumps({"pdf_url": "https://example.com/datasheet?fmt=pdf"})
            mock_verify.return_value = True

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.pdf_url == "https://example.com/datasheet?fmt=pdf"
            assert updated.stage_find_pdf == STAGE_COMPLETED

    @pytest.mark.asyncio
    async def test_stage_1_kimi_failure(self, sample_node, tmp_manifest):
        """Kimi CLI itself fails (returns None) → failure."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = None

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.failure_category == "PDF_NOT_FOUND"
            assert "no output" in updated.failure_message.lower()
