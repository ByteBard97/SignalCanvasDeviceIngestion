"""Tests for Stage 2 — Download PDF.

Uses respx to mock httpx HTTP calls.
"""

from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_1_CANNOT_FIND_PDF,
    DOC_TYPE_SPEC_SHEET,
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

        # Mock HTTP response with valid PDF content (>30 KB threshold)
        pdf_content = b"%PDF-1.4\n" + b"x" * 31_000
        route = respx.get("https://example.com/datasheet.pdf").mock(
            return_value=httpx.Response(200, content=pdf_content)
        )

        result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

        assert result is True
        assert route.called

        updated = tmp_manifest.get_node("test-device")
        assert updated.stage_download_pdf == STAGE_COMPLETED
        assert updated.pdf_path is not None
        assert Path(updated.pdf_path).exists()
        assert Path(updated.pdf_path).read_bytes() == pdf_content

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


class TestStage2FallbackUrl:
    """Tests for the connection-error fallback path that asks Kimi for an alternate URL."""

    @respx.mock
    @pytest.mark.asyncio
    async def test_ssl_error_triggers_kimi_fallback_and_succeeds(
        self, sample_node, tmp_manifest, tmp_path
    ):
        """Audinate-style: first URL fails with cert error; Kimi returns manufacturer-direct URL; download succeeds."""
        sample_node.pdf_url = "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        # First URL: simulate SSL cert chain failure
        respx.get("https://cdn-docs.av-iq.com/dataSheet/ADP.pdf").mock(
            side_effect=httpx.ConnectError("SSL: CERTIFICATE_VERIFY_FAILED")
        )
        # Fallback URL Kimi suggests
        alt_url = "https://www.audinate.com/datasheet.pdf"
        alt_pdf_content = b"%PDF-1.7\n" + b"x" * 31_000
        respx.get(alt_url).mock(
            return_value=httpx.Response(200, content=alt_pdf_content)
        )

        with patch(
            "src.pipeline_stages._request_alternate_pdf_url", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = alt_url

            result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

            assert result is True
            mock_kimi.assert_awaited_once()
            # The fallback prompt must include the failed URL so Kimi avoids it
            kwargs = mock_kimi.await_args.kwargs
            assert kwargs["failed_url"] == "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"
            assert "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf" in kwargs["tried_urls"]

            updated = tmp_manifest.get_node("test-device")
            assert updated.stage_download_pdf == STAGE_COMPLETED
            # The successful URL is now persisted on the node
            assert updated.pdf_url == alt_url

            # Regression: device_documents must contain a spec_sheet row for
            # the URL that actually downloaded, with local_path set. Earlier
            # set_document_local_path was a strict UPDATE-by-URL and silently
            # no-op'd when the fallback URL didn't match the originally-
            # recorded one — leaving the audit row forever incomplete.
            spec_docs = tmp_manifest.list_documents("test-device", DOC_TYPE_SPEC_SHEET)
            working = [d for d in spec_docs if d.url == alt_url]
            assert len(working) == 1
            assert working[0].local_path is not None
            assert working[0].local_path.endswith(".pdf")

    @respx.mock
    @pytest.mark.asyncio
    async def test_kimi_returns_no_alternate_marks_failure(
        self, sample_node, tmp_manifest, tmp_path
    ):
        """Connection error + Kimi can't find an alternate → standard failure."""
        sample_node.pdf_url = "https://broken.example.com/datasheet.pdf"
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        respx.get("https://broken.example.com/datasheet.pdf").mock(
            side_effect=httpx.ConnectError("connection refused")
        )

        with patch(
            "src.pipeline_stages._request_alternate_pdf_url", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = None

            result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

            assert result is False
            mock_kimi.assert_awaited_once()
            updated = tmp_manifest.get_node("test-device")
            assert updated.failure_category == "PDF_DOWNLOAD_FAILED"
            assert "ConnectError" in updated.failure_message

    @respx.mock
    @pytest.mark.asyncio
    async def test_http_404_does_not_trigger_kimi_fallback(
        self, sample_node, tmp_manifest, tmp_path
    ):
        """A 4xx response is a definitive 'no' — don't waste a Kimi call."""
        tmp_manifest.add_node(sample_node)
        cache_dir = tmp_path / "pdfs"

        respx.get("https://example.com/datasheet.pdf").mock(
            return_value=httpx.Response(404, text="Not Found")
        )

        with patch(
            "src.pipeline_stages._request_alternate_pdf_url", new_callable=AsyncMock
        ) as mock_kimi:
            result = await stage_2_download_pdf(sample_node, tmp_manifest, cache_dir=cache_dir)

            assert result is False
            mock_kimi.assert_not_awaited()
            updated = tmp_manifest.get_node("test-device")
            assert updated.failure_category == "PDF_DOWNLOAD_FAILED"
