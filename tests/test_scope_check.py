"""Tests for the scope-check gate and already-completed device skipping."""

from __future__ import annotations

import pytest
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_0_INITIAL,
    QUEUE_4_MANUAL_REVIEW,
    QUEUE_5_COMPLETED,
)
from src.runner import _run_scope_check  # noqa: E402


@pytest.fixture
def test_manifest(tmp_path):
    """Create a test manifest database."""
    db_path = tmp_path / "test_ingestion.db"
    return Manifest(db_path)


@pytest.mark.asyncio
async def test_scope_check_rejects_networking_device(test_manifest):
    """A Cisco switch in QUEUE_0_INITIAL must be rejected as out-of-scope."""
    node = DeviceNode(
        device_id="cisco-sf200-24",
        manufacturer="Cisco",
        model="SF200-24",
        corpus_id="cisco-sf200-24",
        queue=QUEUE_0_INITIAL,
    )
    test_manifest.add_node(node)

    result = await _run_scope_check([node], test_manifest)

    assert len(result) == 0  # All rejected
    reloaded = test_manifest.get_node("cisco-sf200-24")
    assert reloaded.queue == QUEUE_4_MANUAL_REVIEW
    assert reloaded.failure_category == "OUT_OF_SCOPE"
    assert reloaded.failure_stage == 0


@pytest.mark.asyncio
async def test_scope_check_keeps_av_device(test_manifest):
    """An AV device in QUEUE_0_INITIAL must pass scope check."""
    node = DeviceNode(
        device_id="yamaha-rio1608-d2",
        manufacturer="Yamaha",
        model="Rio1608-D2",
        corpus_id="yamaha-rio1608-d2",
        queue=QUEUE_0_INITIAL,
    )
    test_manifest.add_node(node)

    result = await _run_scope_check([node], test_manifest)

    assert len(result) == 1
    assert result[0].device_id == "yamaha-rio1608-d2"
    assert result[0].queue == QUEUE_0_INITIAL  # unchanged
    assert result[0].failure_stage is None


@pytest.mark.asyncio
async def test_scope_check_bypasses_already_completed_devices(test_manifest):
    """Devices already in QUEUE_5_COMPLETED must bypass scope check entirely."""
    node = DeviceNode(
        device_id="behringer-ha8000-v2",
        manufacturer="Behringer",
        model="HA8000 v2",
        corpus_id="behringer-ha8000-v2",
        queue=QUEUE_5_COMPLETED,
        stage_extract_specs=2,
        stage_generate_patch=2,
        stage_validate_patch=2,
    )
    test_manifest.add_node(node)

    result = await _run_scope_check([node], test_manifest)

    assert len(result) == 1
    assert result[0].device_id == "behringer-ha8000-v2"
    assert result[0].queue == QUEUE_5_COMPLETED
    # No failure state should be written
    assert result[0].failure_stage is None
    assert result[0].failure_category is None


@pytest.mark.asyncio
async def test_scope_check_budget_exhaustion_fails_device_not_run(test_manifest, monkeypatch):
    """If the per-device LLM budget is exhausted at scope-check classification,
    the device must be failed gracefully (not crash the whole run).

    This guards runner.py's classify call site, which is NOT inside the broad
    try/except that the Stage 6-7 classify call sits in — an unhandled
    CallBudgetExceeded there would abort the entire `for node in nodes` loop.
    """
    import src.runner as runner
    from src.call_budget import CallBudgetExceeded

    # Force the LLM path (no EasySchematic AV-type bypass) and make the
    # classifier report the device is over its call budget.
    monkeypatch.setattr(runner, "get_combined_context", lambda *a, **k: None)

    async def _budget_exhausted(*args, **kwargs):
        raise CallBudgetExceeded("device over LLM budget")

    monkeypatch.setattr(runner, "classify", _budget_exhausted)

    node = DeviceNode(
        device_id="acme-over-budget",
        manufacturer="Acme",
        model="WidgetMatrix",
        corpus_id="acme-over-budget",
        queue=QUEUE_0_INITIAL,
    )
    test_manifest.add_node(node)

    # Must return normally — the run is not aborted.
    result = await _run_scope_check([node], test_manifest)

    assert result == []  # device skipped, not kept
    reloaded = test_manifest.get_node("acme-over-budget")
    assert reloaded.queue == QUEUE_4_MANUAL_REVIEW
    assert reloaded.failure_attempts == 1


@pytest.mark.asyncio
async def test_scope_check_mixed_batch(test_manifest):
    """A batch with AV + networking + already-completed must split correctly."""
    av_node = DeviceNode(
        device_id="qsc-core-110f",
        manufacturer="QSC",
        model="Core 110f",
        corpus_id="qsc-core-110f",
        queue=QUEUE_0_INITIAL,
    )
    net_node = DeviceNode(
        device_id="aruba-ap-535",
        manufacturer="Aruba",
        model="AP-535",
        corpus_id="aruba-ap-535",
        queue=QUEUE_0_INITIAL,
    )
    done_node = DeviceNode(
        device_id="shure-ulxd4",
        manufacturer="Shure",
        model="ULXD4",
        corpus_id="shure-ulxd4",
        queue=QUEUE_5_COMPLETED,
        stage_extract_specs=2,
        stage_generate_patch=2,
        stage_validate_patch=2,
    )
    for n in (av_node, net_node, done_node):
        test_manifest.add_node(n)

    result = await _run_scope_check([av_node, net_node, done_node], test_manifest)

    assert len(result) == 2
    assert {n.device_id for n in result} == {"qsc-core-110f", "shure-ulxd4"}

    reloaded_net = test_manifest.get_node("aruba-ap-535")
    assert reloaded_net.queue == QUEUE_4_MANUAL_REVIEW
    assert reloaded_net.failure_category == "OUT_OF_SCOPE"
