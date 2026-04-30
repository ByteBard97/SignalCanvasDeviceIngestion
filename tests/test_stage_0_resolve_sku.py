"""Tests for Stage 0 — alias to canonical manufacturer SKU resolution."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_1_CANNOT_FIND_PDF,
    STAGE_RESOLVE_SKU,
)
from src.pipeline_stages import (
    STAGE_COMPLETED,
    STAGE_FAILED,
    stage_0_resolve_sku,
    _resolved_sku,
)


@pytest.fixture
def tmp_manifest(tmp_path):
    return Manifest(tmp_path / "test_manifest.db")


@pytest.fixture
def alias_node():
    """The motivating Audinate case — user typed an alias, not the canonical SKU."""
    return DeviceNode(
        device_id="audinate-avio-ao2",
        manufacturer="AUDINATE",
        model="AVIO-AO2",
    )


class TestResolvedSkuHelper:
    def test_falls_back_to_model_when_canonical_sku_missing(self):
        node = DeviceNode(device_id="x", manufacturer="X", model="Rio1608-D2")
        assert _resolved_sku(node) == "Rio1608-D2"

    def test_uses_canonical_sku_when_set(self):
        node = DeviceNode(
            device_id="x", manufacturer="X", model="AVIO-AO2",
            canonical_sku="ADP-DAO-AU-0X2",
        )
        assert _resolved_sku(node) == "ADP-DAO-AU-0X2"


class TestStage0ResolveSku:
    @pytest.mark.asyncio
    async def test_alias_resolved_to_canonical_sku(self, alias_node, tmp_manifest):
        """Audinate alias case: AVIO-AO2 -> ADP-DAO-AU-0X2."""
        tmp_manifest.add_node(alias_node)

        kimi_response = json.dumps({
            "canonical_sku": "ADP-DAO-AU-0X2",
            "product_name": "Dante AVIO 2Ch Output Adapter",
            "is_alias": True,
        })

        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            mock_kimi.return_value = kimi_response

            result = await stage_0_resolve_sku(alias_node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.canonical_sku == "ADP-DAO-AU-0X2"
            assert updated.canonical_product_name == "Dante AVIO 2Ch Output Adapter"
            assert updated.stage_resolve_sku == STAGE_COMPLETED

    @pytest.mark.asyncio
    async def test_already_canonical_input_passes_through(self, tmp_manifest):
        """Yamaha Rio1608-D2 is already the canonical SKU — Kimi echoes it back."""
        node = DeviceNode(
            device_id="yamaha-rio1608-d2",
            manufacturer="YAMAHA",
            model="Rio1608-D2",
        )
        tmp_manifest.add_node(node)

        kimi_response = json.dumps({
            "canonical_sku": "Rio1608-D2",
            "product_name": "Yamaha Rio1608-D2 I/O Rack",
            "is_alias": False,
        })
        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            mock_kimi.return_value = kimi_response

            result = await stage_0_resolve_sku(node, tmp_manifest)

            assert result is True
            updated = tmp_manifest.get_node("yamaha-rio1608-d2")
            assert updated.canonical_sku == "Rio1608-D2"

    @pytest.mark.asyncio
    async def test_idempotent_when_already_resolved(self, alias_node, tmp_manifest):
        """If canonical_sku is already set, Stage 0 returns True without calling Kimi."""
        alias_node.canonical_sku = "ADP-DAO-AU-0X2"
        tmp_manifest.add_node(alias_node)

        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            result = await stage_0_resolve_sku(alias_node, tmp_manifest)

            assert result is True
            mock_kimi.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_kimi_returns_null_sku_marks_failure(self, alias_node, tmp_manifest):
        """When Kimi can't identify the product, node lands in queue 1 with retryable failure."""
        tmp_manifest.add_node(alias_node)

        kimi_response = json.dumps({
            "canonical_sku": None, "product_name": None, "is_alias": None,
        })
        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            mock_kimi.return_value = kimi_response

            result = await stage_0_resolve_sku(alias_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.failure_category == "SKU_RESOLUTION_FAILED"
            assert updated.failure_retryable is True
            assert updated.failure_stage == STAGE_RESOLVE_SKU
            assert updated.queue == QUEUE_1_CANNOT_FIND_PDF

    @pytest.mark.asyncio
    async def test_kimi_empty_output_marks_failure(self, alias_node, tmp_manifest):
        tmp_manifest.add_node(alias_node)

        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            mock_kimi.return_value = None

            result = await stage_0_resolve_sku(alias_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.failure_category == "SKU_RESOLUTION_FAILED"
            assert "no output" in updated.failure_message.lower()

    @pytest.mark.asyncio
    async def test_kimi_garbage_output_marks_failure(self, alias_node, tmp_manifest):
        tmp_manifest.add_node(alias_node)

        with patch("src.kimi_runner.run_kimi", new_callable=AsyncMock) as mock_kimi:
            mock_kimi.return_value = "I have no idea what that is, sorry."

            result = await stage_0_resolve_sku(alias_node, tmp_manifest)

            assert result is False
            updated = tmp_manifest.get_node("audinate-avio-ao2")
            assert updated.failure_category == "SKU_RESOLUTION_FAILED"
