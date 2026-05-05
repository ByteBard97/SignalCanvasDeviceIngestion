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
from src.pipeline_stages import (
    stage_1_find_pdf,
    _find_secondary_doc,
)
from src.harness.manifest import DOC_TYPE_USER_MANUAL, DOC_TYPE_INSTALL_GUIDE


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


@pytest.fixture
def disable_av_iq(monkeypatch):
    """Force tests through the Kimi path by disabling Stage 1 fallbacks.

    Stage 1 short-circuits to a known URL when the device matches the AV-iQ
    index, and falls back to a real DuckDuckGo search if Kimi exhausts. These
    tests mock Kimi and assert on its result, so both fallbacks are disabled.
    """
    monkeypatch.setattr(
        "src.pipeline_stages._av_iq_url_for_device", lambda manufacturer, model: None
    )

    async def _noop_ddg(*args, **kwargs):
        return None
    monkeypatch.setattr(
        "src.pipeline_stages._search_duckduckgo_for_pdf", _noop_ddg
    )


class TestStage1FindPDF:
    """Tests for stage_1_find_pdf."""

    @pytest.fixture(autouse=True)
    def _disable_av_iq(self, disable_av_iq):
        pass

    @pytest.mark.asyncio
    async def test_stage_1_success(self, sample_node, tmp_manifest):
        """Mock Kimi returning valid JSON; assert node fields are set correctly."""
        tmp_manifest.add_node(sample_node)

        mock_stdout = json.dumps({"pdf_url": "https://example.com/Rio1608-D2-datasheet.pdf"})

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = mock_stdout

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.pdf_url == "https://example.com/Rio1608-D2-datasheet.pdf"
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
            mock_kimi.return_value = json.dumps({"url": "https://example.com/Rio1608-D2-datasheet.pdf"})

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
            mock_kimi.return_value = json.dumps({"pdf_url": "https://example.com/Rio1608-D2-datasheet.html"})
            mock_verify.return_value = False

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.failure_category == "PDF_NOT_FOUND"
            mock_verify.assert_awaited_with("https://example.com/Rio1608-D2-datasheet.html")

    @pytest.mark.asyncio
    async def test_stage_1_non_pdf_url_with_good_head(self, sample_node, tmp_manifest):
        """URL doesn't end in .pdf but HEAD confirms application/pdf → success."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi, patch(
            "src.pipeline_stages._verify_pdf_content_type", new_callable=AsyncMock
        ) as mock_verify:
            mock_kimi.return_value = json.dumps({"pdf_url": "https://example.com/Rio1608-D2-datasheet?fmt=pdf"})
            mock_verify.return_value = True

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.pdf_url == "https://example.com/Rio1608-D2-datasheet?fmt=pdf"
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


class TestStage1Retry:
    """Tests for retry behavior on empty output and rejected URLs."""

    @pytest.fixture(autouse=True)
    def _disable_av_iq(self, disable_av_iq):
        pass

    @pytest.mark.asyncio
    async def test_retries_on_empty_output_then_succeeds(self, sample_node, tmp_manifest, monkeypatch):
        """First call returns None (Yamaha-style flake); second call returns valid URL."""
        tmp_manifest.add_node(sample_node)
        monkeypatch.setattr("src.pipeline_stages.settings.find_secondary_docs", False)

        good = json.dumps({"pdf_url": "https://example.com/Rio1608-D2-datasheet.pdf"})
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.side_effect = [None, good]

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is True
            assert mock_kimi.await_count == 2
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.pdf_url == "https://example.com/Rio1608-D2-datasheet.pdf"

    @pytest.mark.asyncio
    async def test_accepts_agent_url_without_filename_filter(self, tmp_manifest, monkeypatch):
        """Agent-returned URLs are trusted; no filename-based rejection."""
        node = DeviceNode(
            device_id="audinate-avio-ao2",
            manufacturer="AUDINATE",
            model="AVIO-AO2",
        )
        tmp_manifest.add_node(node)
        monkeypatch.setattr("src.pipeline_stages.settings.find_secondary_docs", False)

        # Even though "ADP.pdf" doesn't match "AVIO-AO2", the agent chose it
        # and we no longer second-guess with filename heuristics.
        url = json.dumps({"pdf_url": "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"})
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = url

            result = await stage_1_find_pdf(node, tmp_manifest)

            assert result is True
            assert mock_kimi.await_count == 1
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.pdf_url == "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"

    @pytest.mark.asyncio
    async def test_all_retries_fail(self, sample_node, tmp_manifest):
        """All attempts return empty → failure with retryable=True."""
        tmp_manifest.add_node(sample_node)

        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = None

            result = await stage_1_find_pdf(sample_node, tmp_manifest)

            assert result is False
            assert mock_kimi.await_count >= 2
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.failure_category == "PDF_NOT_FOUND"
            assert updated.failure_retryable is True


class TestSecondaryDocSearch:
    """Tests for _find_secondary_doc — best-effort, single-attempt secondary lookups."""

    @pytest.mark.asyncio
    async def test_secondary_success_records_document(self, sample_node, tmp_manifest):
        """Valid Kimi JSON for user_manual → row added to device_documents."""
        tmp_manifest.add_node(sample_node)
        manual_url = "https://example.com/Rio1608-D2-user-manual.pdf"
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = json.dumps({"pdf_url": manual_url})
            result = await _find_secondary_doc(
                sample_node, tmp_manifest, DOC_TYPE_USER_MANUAL
            )
            assert result == manual_url
            assert mock_kimi.await_count == 1
            docs = tmp_manifest.list_documents(
                sample_node.device_id, DOC_TYPE_USER_MANUAL
            )
            assert len(docs) == 1
            assert docs[0].url == manual_url
            assert docs[0].local_path is None

    @pytest.mark.asyncio
    async def test_secondary_url_accepted_despite_filename_mismatch(self, sample_node, tmp_manifest):
        """Secondary docs skip filename filtering; agent judgment is trusted."""
        tmp_manifest.add_node(sample_node)
        url = "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = json.dumps({"pdf_url": url})
            result = await _find_secondary_doc(
                sample_node, tmp_manifest, DOC_TYPE_INSTALL_GUIDE
            )
            assert result == url
            docs = tmp_manifest.list_documents(
                sample_node.device_id, DOC_TYPE_INSTALL_GUIDE
            )
            assert len(docs) == 1
            assert docs[0].url == url

    @pytest.mark.asyncio
    async def test_secondary_kimi_exception_swallowed(self, sample_node, tmp_manifest):
        """Kimi raising → returns None, no row written, no exception bubbles up."""
        tmp_manifest.add_node(sample_node)
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.side_effect = RuntimeError("kimi exploded")
            result = await _find_secondary_doc(
                sample_node, tmp_manifest, DOC_TYPE_USER_MANUAL
            )
            assert result is None
            docs = tmp_manifest.list_documents(sample_node.device_id)
            assert docs == []
