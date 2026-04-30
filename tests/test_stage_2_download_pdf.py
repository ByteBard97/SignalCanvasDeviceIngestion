"""Tests for Stage 2 — Download PDF.

Uses respx to mock httpx HTTP calls.
"""

from pathlib import Path
from unittest.mock import patch

import httpx
import pytest
import respx

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_1_CANNOT_FIND_PDF,
)
from src.pipeline_stages import STAGE_COMPLETED, STAGE_FAILED
from src.pipeline_stages import stage_2_download_pdf


@pytest.fixture
def tmp_manifest(tmp_path):
    """Create a temporary manifest for testing."""
    db_path = tmp_path / "test_manifest.db"
    return Manifest(db_path)


@pytest.fixture
def sample_node():
    """Create a sample device node with a PDF URL."""
    return DeviceNode(
        device_id="test-device",
        manufacturer="TestMfg",
        model="TestModel",
        pdf_url="https://example.com/datasheet.pdf",
    )


class TestStage2DownloadPDF:
    """Tests for stage_2_download_pdf."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_stage_2_success(self, sample_node, tmp_manifest, tmp_path):
        """Download a valid PDF and verify node state."""
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        # Mock HTTP response with valid PDF content
        route = respx.get("https://example.com/datasheet.pdf").mock(
            return_value=httpx.Response(200, content=b"%PDF-1.4\ntest content")
        )

        result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

        assert result is True
        assert route.called

        updated = tmp_manifest.get_node("test-device")
        assert updated.stage_download_pdf == STAGE_COMPLETED
        assert updated.pdf_path is not None
        assert Path(updated.pdf_path).exists()
        assert Path(updated.pdf_path).read_bytes() == b"%PDF-1.4\ntest content"

    @respx.mock
    @pytest.mark.asyncio
    async def test_stage_2_invalid_pdf(self, sample_node, tmp_manifest, tmp_path):
        """Download a non-PDF file → failure with PDF_INVALID."""
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        respx.get("https://example.com/datasheet.pdf").mock(
            return_value=httpx.Response(200, content=b"<html>not a pdf</html>")
        )

        result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

        assert result is False
        updated = tmp_manifest.get_node("test-device")
        assert updated.queue == QUEUE_1_CANNOT_FIND_PDF
        assert updated.stage_download_pdf == STAGE_FAILED
        assert updated.failure_category == "PDF_INVALID"

    @respx.mock
    @pytest.mark.asyncio
    async def test_stage_2_http_error(self, sample_node, tmp_manifest, tmp_path):
        """HTTP 404 → failure with PDF_DOWNLOAD_FAILED."""
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        respx.get("https://example.com/datasheet.pdf").mock(
            return_value=httpx.Response(404, text="Not Found")
        )

        result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

        assert result is False
        updated = tmp_manifest.get_node("test-device")
        assert updated.failure_category == "PDF_DOWNLOAD_FAILED"
        assert updated.failure_retryable is True

    @pytest.mark.asyncio
    async def test_stage_2_no_pdf_url(self, sample_node, tmp_manifest, tmp_path):
        """Node with no pdf_url → immediate failure."""
        sample_node.pdf_url = None
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

        assert result is False
        updated = tmp_manifest.get_node("test-device")
        assert updated.failure_category == "PDF_DOWNLOAD_FAILED"
        assert "no pdf_url" in updated.failure_message.lower()
