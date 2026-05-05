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
    _url_likely_matches_model,
    _looks_opaque,
    _model_tokens,
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


class TestStage1FindPDF:
    """Tests for stage_1_find_pdf."""

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


class TestUrlRelevanceHeuristic:
    """Tests for _url_likely_matches_model and helpers."""

    def test_model_tokens_splits_on_punctuation_and_alpha_digit(self):
        # Splits on '-' AND on letter/digit boundaries; tokens of len < 2 are dropped.
        assert _model_tokens("AVIO-AO2") == ["AVIO", "AO"]
        assert _model_tokens("Rio1608-D2") == ["Rio", "1608"]
        assert _model_tokens("ULXD4") == ["ULXD"]
        assert _model_tokens("SQ-5") == ["SQ"]

    def test_looks_opaque_short_slugs_are_not_opaque(self):
        # Short slugs are product codes, not hashes
        assert _looks_opaque("adp") is False
        assert _looks_opaque("ULXD4") is False

    def test_looks_opaque_long_hex_is_opaque(self):
        assert _looks_opaque("1078f753f2bbafa663cc873b1299a43e1fd6") is True

    def test_looks_opaque_long_word_is_not_opaque(self):
        # 12-char "datasheetxyz" — letters dominate, not a hash
        assert _looks_opaque("datasheetxyz") is False

    def test_url_matches_when_token_in_path(self):
        # Real Stage 1 wins from prior runs
        assert _url_likely_matches_model(
            "https://pubs.shure.com/view/guide/ULXD/en-US.pdf", "ULXD4"
        ) is True
        assert _url_likely_matches_model(
            "https://www.allen-heath.com/uploads/SQ-5-Technical-Datasheet.pdf", "SQ-5"
        ) is True

    def test_url_rejected_when_filename_is_wrong_product_code(self):
        # The Audinate failure: AVIO-AO2 vs ADP.pdf
        assert _url_likely_matches_model(
            "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf", "AVIO-AO2"
        ) is False

    def test_url_accepted_when_filename_is_opaque_hash(self):
        # The Sennheiser case — gzhls.at content hash
        assert _url_likely_matches_model(
            "https://gzhls.at/blob/ldb/4/b/1/9/1078f753f2bbafa663cc873b1299a43e1fd6.pdf",
            "EW-DX-EM",
        ) is True


class TestStage1Retry:
    """Tests for retry behavior on empty output and rejected URLs."""

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
    async def test_retries_on_wrong_device_url_then_succeeds(self, tmp_manifest, monkeypatch):
        """First Kimi call returns Audinate-style wrong-device URL; retry succeeds with right one."""
        node = DeviceNode(
            device_id="audinate-avio-ao2",
            manufacturer="AUDINATE",
            model="AVIO-AO2",
        )
        tmp_manifest.add_node(node)
        monkeypatch.setattr("src.pipeline_stages.settings.find_secondary_docs", False)

        wrong = json.dumps({"pdf_url": "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"})
        right = json.dumps({"pdf_url": "https://www.audinate.com/avio-ao2-datasheet.pdf"})
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.side_effect = [wrong, right]

            result = await stage_1_find_pdf(node, tmp_manifest)

            assert result is True
            assert mock_kimi.await_count == 2
            # Second prompt must mention the rejected URL so Kimi avoids it
            second_prompt = mock_kimi.await_args_list[1].args[0]
            assert "ADP.pdf" in second_prompt
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.pdf_url == "https://www.audinate.com/avio-ao2-datasheet.pdf"

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
    async def test_secondary_url_mismatch_rejected(self, sample_node, tmp_manifest):
        """URL whose filename doesn't match model → returns None, no row written."""
        tmp_manifest.add_node(sample_node)
        wrong_url = "https://cdn-docs.av-iq.com/dataSheet/ADP.pdf"
        with patch(
            "src.kimi_runner.run_kimi", new_callable=AsyncMock
        ) as mock_kimi:
            mock_kimi.return_value = json.dumps({"pdf_url": wrong_url})
            result = await _find_secondary_doc(
                sample_node, tmp_manifest, DOC_TYPE_INSTALL_GUIDE
            )
            assert result is None
            docs = tmp_manifest.list_documents(
                sample_node.device_id, DOC_TYPE_INSTALL_GUIDE
            )
            assert docs == []

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
