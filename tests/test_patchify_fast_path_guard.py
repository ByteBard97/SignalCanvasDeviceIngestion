"""
Comprehensive test for _run_patchify_fast_path guard logic.

This test verifies that the guard protects ALL previously-processed node states
from being overwritten by the patchify fast-path.

The guard condition is:
    if node.stage_find_pdf != STAGE_NOT_STARTED or node.specs_json:
        continue

This means a node is ONLY eligible for fast-tracking if:
    - stage_find_pdf == 0 (NOT_STARTED)
    - AND specs_json is falsy (None or "")

Any other state MUST be skipped (protected).

To test the guard independently of whether patchify data exists, we monkey-patch
extract_specs_from_patchify to always return dummy specs. This way every node
that passes the guard WILL be fast-tracked, making it easy to see which states
are protected and which aren't.
"""

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

# Ensure src is importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_0_INITIAL,
    QUEUE_1_CANNOT_FIND_PDF,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_3_READY_FOR_EXTRACTION,
    QUEUE_4_MANUAL_REVIEW,
    QUEUE_5_COMPLETED,
    FailureCategory,
)
from src.pipeline_stages import STAGE_NOT_STARTED
from src.runner import _run_patchify_fast_path

# Stage constants
STAGE_IN_PROGRESS = 1
STAGE_COMPLETED = 2
STAGE_FAILED = 3

def _dummy_specs(device_id: str) -> dict:
    """Return dummy patchify specs for a device_id."""
    # The generic-no-ports device should return zero ports to test garbage filter
    if device_id == "generic-no-ports":
        ports = []
    else:
        ports = [
            {"name": "In1", "direction": "in", "connector": "XLR", "channels": 1, "attributes": ["Analogue"]},
            {"name": "Out1", "direction": "out", "connector": "XLR", "channels": 1, "attributes": ["Analogue"]},
        ]
    return {
        "device_metadata": {"manufacturer": "Test", "model_number": "T"},
        "signal_flow": {
            "ports": ports,
            "bridges": [],
        },
        "power_specs": {},
        "physical_specs": {},
        "extraction_confidence": "high",
        "notes": "dummy",
    }


def _make_manifest() -> Manifest:
    """Create a fresh temporary manifest DB."""
    fd, db_path = tempfile.mkstemp(suffix=".db")
    Path(db_path).unlink(missing_ok=True)
    return Manifest(Path(db_path))


def _create_node(
    manifest: Manifest,
    device_id: str,
    manufacturer: str = "TestMfg",
    model: str = "TestModel",
    queue: int = QUEUE_0_INITIAL,
    stage_resolve_sku: int = STAGE_NOT_STARTED,
    stage_find_pdf: int = STAGE_NOT_STARTED,
    stage_download_pdf: int = STAGE_NOT_STARTED,
    stage_convert_marker: int = STAGE_NOT_STARTED,
    stage_index_rag: int = STAGE_NOT_STARTED,
    stage_extract_specs: int = STAGE_NOT_STARTED,
    stage_generate_patch: int = STAGE_NOT_STARTED,
    stage_validate_patch: int = STAGE_NOT_STARTED,
    specs_json: str | None = None,
    failure_stage: int | None = None,
    failure_category: str | None = None,
    failure_message: str | None = None,
    failure_retryable: bool = False,
    failure_attempts: int = 0,
    canonical_sku: str | None = None,
    canonical_product_name: str | None = None,
    pdf_url: str | None = None,
    pdf_path: str | None = None,
    ragscallion_job_id: str | None = None,
    ragscallion_submitted_at: str | None = None,
    ragscallion_completed_at: str | None = None,
    marked_suspicious: bool = False,
    patch_source: str | None = None,
) -> DeviceNode:
    """Create and persist a DeviceNode with the given state."""
    node = DeviceNode(
        device_id=device_id,
        manufacturer=manufacturer,
        model=model,
        corpus_id=device_id.lower().replace(" ", "_")[:64],
        queue=queue,
        stage_resolve_sku=stage_resolve_sku,
        stage_find_pdf=stage_find_pdf,
        stage_download_pdf=stage_download_pdf,
        stage_convert_marker=stage_convert_marker,
        stage_index_rag=stage_index_rag,
        stage_extract_specs=stage_extract_specs,
        stage_generate_patch=stage_generate_patch,
        stage_validate_patch=stage_validate_patch,
        specs_json=specs_json,
        failure_stage=failure_stage,
        failure_category=failure_category,
        failure_message=failure_message,
        failure_retryable=failure_retryable,
        failure_attempts=failure_attempts,
        canonical_sku=canonical_sku,
        canonical_product_name=canonical_product_name,
        pdf_url=pdf_url,
        pdf_path=pdf_path,
        ragscallion_job_id=ragscallion_job_id,
        ragscallion_submitted_at=ragscallion_submitted_at,
        ragscallion_completed_at=ragscallion_completed_at,
        marked_suspicious=marked_suspicious,
        patch_source=patch_source,
    )
    manifest.add_node(node)
    return node


def _node_state_dict(node: DeviceNode) -> dict:
    """Return a snapshot of the node's mutable state."""
    return {
        "queue": node.queue,
        "stage_resolve_sku": node.stage_resolve_sku,
        "stage_find_pdf": node.stage_find_pdf,
        "stage_download_pdf": node.stage_download_pdf,
        "stage_convert_marker": node.stage_convert_marker,
        "stage_index_rag": node.stage_index_rag,
        "stage_extract_specs": node.stage_extract_specs,
        "stage_generate_patch": node.stage_generate_patch,
        "stage_validate_patch": node.stage_validate_patch,
        "specs_json": node.specs_json,
        "failure_stage": node.failure_stage,
        "failure_category": node.failure_category,
        "failure_message": node.failure_message,
        "failure_retryable": node.failure_retryable,
        "failure_attempts": node.failure_attempts,
        "canonical_sku": node.canonical_sku,
        "pdf_url": node.pdf_url,
        "pdf_path": node.pdf_path,
        "ragscallion_job_id": node.ragscallion_job_id,
        "patch_source": node.patch_source,
        "marked_suspicious": node.marked_suspicious,
    }


def _reload_node(manifest: Manifest, device_id: str) -> DeviceNode:
    """Reload node from DB to capture any persisted changes."""
    return manifest.get_node(device_id)


def run_test() -> int:
    """Run all guard-state tests and report results."""
    manifest = _make_manifest()

    # We track each test case: description, node, expected_modified (bool), expected_fast_tracked (bool)
    test_cases = []

    # -------------------------------------------------------------------------
    # CASE 1: Truly fresh node → SHOULD fast-track
    # -------------------------------------------------------------------------
    node1 = _create_node(
        manifest,
        device_id="test-fresh",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Fresh node (q0, all stages=0, specs=None)",
        "node": node1,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "This is the intended fast-path target",
    })

    # -------------------------------------------------------------------------
    # CASE 2: Queue 5, all stages completed, specs_json set → SHOULD skip
    # -------------------------------------------------------------------------
    node2 = _create_node(
        manifest,
        device_id="test-q5-complete",
        queue=QUEUE_5_COMPLETED,
        stage_resolve_sku=STAGE_COMPLETED,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        stage_extract_specs=STAGE_COMPLETED,
        stage_generate_patch=STAGE_COMPLETED,
        stage_validate_patch=STAGE_COMPLETED,
        specs_json='{"device_metadata":{"model_number":"X"}}',
    )
    test_cases.append({
        "name": "Queue 5, all stages completed, specs_json set",
        "node": node2,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "Already fully processed — must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 3: Queue 4, failure set, specs_json set → SHOULD skip
    # -------------------------------------------------------------------------
    node3 = _create_node(
        manifest,
        device_id="test-q4-failed-with-specs",
        queue=QUEUE_4_MANUAL_REVIEW,
        stage_find_pdf=STAGE_COMPLETED,
        stage_extract_specs=STAGE_COMPLETED,
        stage_generate_patch=STAGE_FAILED,
        specs_json='{"some":"specs"}',
        failure_stage=6,
        failure_category=FailureCategory.PATCH_GENERATION_FAILED.value,
        failure_message="Patch gen failed",
        failure_retryable=True,
    )
    test_cases.append({
        "name": "Queue 4, failure set, specs_json set",
        "node": node3,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "Has specs_json → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 4: Queue 1, stage_find_pdf FAILED, no specs_json → SHOULD skip
    # -------------------------------------------------------------------------
    node4 = _create_node(
        manifest,
        device_id="test-q1-findpdf-failed",
        queue=QUEUE_1_CANNOT_FIND_PDF,
        stage_find_pdf=STAGE_FAILED,
        specs_json=None,
        failure_stage=0,
        failure_category=FailureCategory.PDF_NOT_FOUND.value,
        failure_message="No PDF found",
        failure_retryable=True,
    )
    test_cases.append({
        "name": "Queue 1, stage_find_pdf=FAILED, no specs_json",
        "node": node4,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf != 0 → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 5: Queue 0, stage_resolve_sku COMPLETED, stage_find_pdf=0, specs=None
    #          → Guard checks ONLY stage_find_pdf and specs_json. This passes!
    #          → WILL be fast-tracked. This is a GUARD VULNERABILITY.
    # -------------------------------------------------------------------------
    node5 = _create_node(
        manifest,
        device_id="test-q0-stage0-done",
        queue=QUEUE_0_INITIAL,
        stage_resolve_sku=STAGE_COMPLETED,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        canonical_sku="CANONICAL-123",
    )
    test_cases.append({
        "name": "Queue 0, stage_resolve_sku=COMPLETED, stage_find_pdf=0, specs=None",
        "node": node5,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Stage 0 completed but guard doesn't check stage_resolve_sku. "
                  "WILL be fast-tracked and pipeline state overwritten. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 6: Queue 0, stage_find_pdf=0, specs_json="" (empty string)
    #          → Empty string is falsy → passes guard!
    #          → WILL be fast-tracked. This is a GUARD VULNERABILITY.
    # -------------------------------------------------------------------------
    node6 = _create_node(
        manifest,
        device_id="test-empty-specs",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json="",
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs_json='' (empty string)",
        "node": node6,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Empty specs_json is falsy → passes guard. "
                  "WILL be overwritten by patchify fast-path. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 7: Queue 2, stage_index_rag IN_PROGRESS, no specs_json → SHOULD skip
    # -------------------------------------------------------------------------
    node7 = _create_node(
        manifest,
        device_id="test-q2-index-in-progress",
        queue=QUEUE_2_POLLING_RAGSCALLION,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_IN_PROGRESS,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 2, stage_index_rag=IN_PROGRESS, no specs_json",
        "node": node7,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=2 != 0 → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 8: Queue 3, stage_extract_specs COMPLETED, specs_json set → SHOULD skip
    # -------------------------------------------------------------------------
    node8 = _create_node(
        manifest,
        device_id="test-q3-extracted",
        queue=QUEUE_3_READY_FOR_EXTRACTION,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        stage_extract_specs=STAGE_COMPLETED,
        specs_json='{"extracted":"data"}',
    )
    test_cases.append({
        "name": "Queue 3, stage_extract_specs=COMPLETED, specs_json set",
        "node": node8,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "Has specs_json → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 9: stage_find_pdf IN_PROGRESS (1), no specs_json → SHOULD skip
    # -------------------------------------------------------------------------
    node9 = _create_node(
        manifest,
        device_id="test-findpdf-in-progress",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_IN_PROGRESS,
        specs_json=None,
    )
    test_cases.append({
        "name": "stage_find_pdf=IN_PROGRESS (1), no specs_json",
        "node": node9,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=1 != 0 → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 10: stage_find_pdf COMPLETED (2), no specs_json → SHOULD skip
    # -------------------------------------------------------------------------
    node10 = _create_node(
        manifest,
        device_id="test-findpdf-completed",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_COMPLETED,
        specs_json=None,
    )
    test_cases.append({
        "name": "stage_find_pdf=COMPLETED (2), no specs_json",
        "node": node10,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=2 != 0 → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 11: stage_download_pdf COMPLETED (2), stage_find_pdf=0
    #          (Abnormal state but possible after manual reset or bug)
    #          → Guard only checks stage_find_pdf=0 and specs_json=None → PASSES!
    #          → WILL be fast-tracked. GUARD VULNERABILITY.
    # -------------------------------------------------------------------------
    node11 = _create_node(
        manifest,
        device_id="test-download-done",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_download_pdf=STAGE_COMPLETED,
        specs_json=None,
    )
    test_cases.append({
        "name": "stage_download_pdf=COMPLETED (2), stage_find_pdf=0, specs=None",
        "node": node11,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Guard does NOT check stage_download_pdf. Abnormal state but would be overwritten. "
                  "CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 12: stage_generate_patch COMPLETED, specs_json set → SHOULD skip
    # -------------------------------------------------------------------------
    node12 = _create_node(
        manifest,
        device_id="test-genpatch-done",
        queue=QUEUE_5_COMPLETED,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        stage_extract_specs=STAGE_COMPLETED,
        stage_generate_patch=STAGE_COMPLETED,
        stage_validate_patch=STAGE_NOT_STARTED,
        specs_json='{"has":"specs"}',
    )
    test_cases.append({
        "name": "stage_generate_patch=COMPLETED, specs_json set",
        "node": node12,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "Has specs_json → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 13: specs_json = "{}" (non-empty), stage_find_pdf=0 → SHOULD skip
    # -------------------------------------------------------------------------
    node13 = _create_node(
        manifest,
        device_id="test-specs-empty-obj",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json="{}",
    )
    test_cases.append({
        "name": "specs_json='{}' (non-empty), stage_find_pdf=0",
        "node": node13,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "specs_json='{}' is truthy → must be protected",
    })

    # -------------------------------------------------------------------------
    # CASE 14: stage_validate_patch COMPLETED, specs_json set, stage_find_pdf=0
    #          (Abnormal: validation done but find_pdf never ran)
    #          → Guard: stage_find_pdf=0 but specs_json is truthy → SKIP
    # -------------------------------------------------------------------------
    node14 = _create_node(
        manifest,
        device_id="test-validate-done",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_validate_patch=STAGE_COMPLETED,
        specs_json='{"has":"specs"}',
    )
    test_cases.append({
        "name": "stage_validate_patch=COMPLETED, specs_json set, stage_find_pdf=0",
        "node": node14,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "Has specs_json → protected despite abnormal stage_find_pdf=0",
    })

    # -------------------------------------------------------------------------
    # CASE 15: Generic manufacturer + zero ports + no SKU → garbage filter
    #          With our dummy specs having 2 ports, this node has a non-Generic
    #          manufacturer, so it will fast-track normally. To test the garbage
    #          filter we need a Generic manufacturer node.
    # -------------------------------------------------------------------------
    node15 = _create_node(
        manifest,
        device_id="generic-no-ports",
        manufacturer="Generic",
        model="Nothing",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Generic manufacturer, zero ports, no SKU (guard passes, garbage filter catches)",
        "node": node15,
        "expected_modified": True,  # Guard passes, but garbage filter will mark as failed
        "expected_fast_tracked": False,  # Not fast-tracked — rejected as garbage
        "reason": "Guard passes, but garbage filter rejects (Generic + 0 ports + no SKU). "
                  "Node is MODIFIED (set to failed state) but NOT fast-tracked.",
    })

    # -------------------------------------------------------------------------
    # CASE 16: Queue 0, stage_resolve_sku=IN_PROGRESS, stage_find_pdf=0, specs=None
    #          → Guard passes (stage_find_pdf=0, specs=None)
    #          → WILL be fast-tracked while Stage 0 is running!
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node16 = _create_node(
        manifest,
        device_id="test-stage0-in-progress",
        queue=QUEUE_0_INITIAL,
        stage_resolve_sku=STAGE_IN_PROGRESS,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 0, stage_resolve_sku=IN_PROGRESS, stage_find_pdf=0, specs=None",
        "node": node16,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Stage 0 is in progress but guard doesn't check it. "
                  "Would be fast-tracked and overwrite in-progress work. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 17: Queue 0, all stages=0, specs=None, but pdf_url set
    #          (e.g., user manually provided PDF URL but pipeline hasn't started)
    #          → Guard: stage_find_pdf=0, specs=None → PASSES!
    #          → WILL fast-track and ignore user URL.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node17 = _create_node(
        manifest,
        device_id="test-pdf-url-set",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        pdf_url="https://example.com/manual.pdf",
    )
    test_cases.append({
        "name": "Queue 0, all stages=0, specs=None, but pdf_url set",
        "node": node17,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Guard does NOT check pdf_url. User-provided URL would be ignored. "
                  "CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 18: Queue 4, failure set, NO specs_json, stage_find_pdf=0
    #          (e.g., failed during Stage 6-7 but was reset)
    #          → Guard: stage_find_pdf=0, specs=None → PASSES!
    #          → WILL fast-track and ignore failure.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node18 = _create_node(
        manifest,
        device_id="test-failure-no-specs",
        queue=QUEUE_4_MANUAL_REVIEW,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_generate_patch=STAGE_FAILED,
        specs_json=None,
        failure_stage=6,
        failure_category=FailureCategory.PATCH_GENERATION_FAILED.value,
        failure_message="Patch gen failed",
        failure_retryable=True,
    )
    test_cases.append({
        "name": "Queue 4, failure set, NO specs_json, stage_find_pdf=0",
        "node": node18,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Failed node with no specs_json passes guard. "
                  "Would be fast-tracked and failure state lost. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 19: Queue 0, stage_resolve_sku=COMPLETED, stage_find_pdf=0, specs=None, canonical_sku set
    #          (Stage 0 resolved but nothing else ran)
        #          → Guard passes. WILL fast-track.
    #          → Stage 0 result (canonical_sku) is preserved but stages overwritten.
    #          → Arguably a bug because SKU resolution work was done for nothing.
    # -------------------------------------------------------------------------
    node19 = _create_node(
        manifest,
        device_id="test-canonical-sku",
        queue=QUEUE_0_INITIAL,
        stage_resolve_sku=STAGE_COMPLETED,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        canonical_sku="CANONICAL-123",
    )
    test_cases.append({
        "name": "Queue 0, stage_resolve_sku=COMPLETED, stage_find_pdf=0, specs=None, canonical_sku set",
        "node": node19,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Stage 0 completed but guard doesn't check it. "
                  "Fast-track overwrites pipeline state. canonical_sku is preserved "
                  "because fast-path doesn't clear it, but stages are clobbered. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 20: Queue 0, stage_find_pdf=0, specs=None, ragscallion_job_id set
    #          (Submitted to Ragscallion but queue was manually reset)
    #          → Guard passes. WILL fast-track.
    #          → Ragscallion job_id is preserved but stages overwritten.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node20 = _create_node(
        manifest,
        device_id="test-rag-job",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        ragscallion_job_id="job-abc-123",
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs=None, ragscallion_job_id set",
        "node": node20,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Ragscallion job_id exists but guard doesn't check it. "
                  "Would fast-track and clobber stages. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 21: Queue 1, stage_find_pdf=FAILED, specs_json="" (empty)
    #          → stage_find_pdf=3 != 0 → SKIP (guard protects)
    # -------------------------------------------------------------------------
    node21 = _create_node(
        manifest,
        device_id="test-q1-failed-empty",
        queue=QUEUE_1_CANNOT_FIND_PDF,
        stage_find_pdf=STAGE_FAILED,
        specs_json="",
        failure_stage=0,
        failure_category=FailureCategory.PDF_NOT_FOUND.value,
        failure_message="No PDF",
        failure_retryable=True,
    )
    test_cases.append({
        "name": "Queue 1, stage_find_pdf=FAILED, specs_json=''",
        "node": node21,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=3 != 0 → protected even with empty specs_json",
    })

    # -------------------------------------------------------------------------
    # CASE 22: Queue 0, stage_find_pdf=0, specs=None, patch_source set
    #          (Abnormal: has patch but no specs and find_pdf never ran)
    #          → Guard passes. WILL fast-track.
    #          → patch_source is preserved but stages overwritten.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node22 = _create_node(
        manifest,
        device_id="test-patch-source",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        patch_source="patch lang here",
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs=None, patch_source set",
        "node": node22,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "patch_source exists but guard doesn't check it. "
                  "Would fast-track and clobber stages. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 23: Queue 0, stage_find_pdf=0, specs=None, marked_suspicious=True
    #          → Guard passes. WILL fast-track.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node23 = _create_node(
        manifest,
        device_id="test-suspicious",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        marked_suspicious=True,
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs=None, marked_suspicious=True",
        "node": node23,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "marked_suspicious=True but guard doesn't check it. "
                  "Would fast-track suspicious device. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 24: Queue 0, stage_find_pdf=0, specs=None, pdf_path set
    #          (PDF downloaded but stage_download_pdf not marked completed)
    #          → Guard passes. WILL fast-track.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node24 = _create_node(
        manifest,
        device_id="test-pdf-path",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_download_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        pdf_path="/tmp/test.pdf",
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, stage_download_pdf=0, specs=None, pdf_path set",
        "node": node24,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "pdf_path exists but guard doesn't check it. "
                  "Would fast-track and clobber stages. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 25: Queue 5, all stages completed, specs_json=None
    #          (Abnormal: completed but no specs — maybe specs were cleared)
    #          → Guard: stage_find_pdf=2 != 0 → SKIP. Good.
    # -------------------------------------------------------------------------
    node25 = _create_node(
        manifest,
        device_id="test-q5-no-specs",
        queue=QUEUE_5_COMPLETED,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        stage_extract_specs=STAGE_COMPLETED,
        stage_generate_patch=STAGE_COMPLETED,
        stage_validate_patch=STAGE_COMPLETED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 5, all stages completed, specs_json=None (abnormal)",
        "node": node25,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=2 != 0 → protected even without specs_json",
    })

    # -------------------------------------------------------------------------
    # CASE 26: Queue 0, stage_find_pdf=0, specs=None, failure fields set
    #          (Abnormal: failure_stage set but queue reset to 0)
    #          → Guard passes. WILL fast-track.
    #          → Failure state preserved but stages clobbered.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node26 = _create_node(
        manifest,
        device_id="test-failure-fields",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
        failure_stage=0,
        failure_category=FailureCategory.PDF_NOT_FOUND.value,
        failure_message="Old failure",
        failure_retryable=False,
    )
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs=None, failure fields set (abnormal)",
        "node": node26,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Failure fields exist but guard doesn't check them. "
                  "Would fast-track and clobber stages. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 27: Queue 0, stage_find_pdf=0, specs=None, device_documents exist
    #          (Documents attached but stages not updated)
    #          → Guard passes. WILL fast-track.
    #          → Document records stay in DB but node stages clobbered.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node27 = _create_node(
        manifest,
        device_id="test-documents",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        specs_json=None,
    )
    manifest.add_document(node27.device_id, "spec_sheet", url="https://example.com/doc.pdf")
    manifest.persist(node27)
    test_cases.append({
        "name": "Queue 0, stage_find_pdf=0, specs=None, device_documents exist",
        "node": node27,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "Documents exist but guard doesn't check device_documents. "
                  "Would fast-track. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 28: Queue 2, stage_index_rag COMPLETED, no specs_json
    #          → stage_find_pdf=2 != 0 → SKIP. Good.
    # -------------------------------------------------------------------------
    node28 = _create_node(
        manifest,
        device_id="test-q2-index-done",
        queue=QUEUE_2_POLLING_RAGSCALLION,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 2, stage_index_rag=COMPLETED, no specs_json",
        "node": node28,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=2 != 0 → protected",
    })

    # -------------------------------------------------------------------------
    # CASE 29: Queue 3, stage_extract_specs=FAILED, no specs_json
    #          → stage_find_pdf=2 != 0 → SKIP. Good.
    # -------------------------------------------------------------------------
    node29 = _create_node(
        manifest,
        device_id="test-q3-extract-failed",
        queue=QUEUE_3_READY_FOR_EXTRACTION,
        stage_find_pdf=STAGE_COMPLETED,
        stage_download_pdf=STAGE_COMPLETED,
        stage_convert_marker=STAGE_COMPLETED,
        stage_index_rag=STAGE_COMPLETED,
        stage_extract_specs=STAGE_FAILED,
        specs_json=None,
        failure_stage=4,
        failure_category=FailureCategory.EXTRACTION_FAILED.value,
        failure_message="Extraction failed",
        failure_retryable=True,
    )
    test_cases.append({
        "name": "Queue 3, stage_extract_specs=FAILED, no specs_json",
        "node": node29,
        "expected_modified": False,
        "expected_fast_tracked": False,
        "reason": "stage_find_pdf=2 != 0 → protected",
    })

    # -------------------------------------------------------------------------
    # CASE 30: Queue 0, stage_convert_marker=COMPLETED, stage_find_pdf=0, specs=None
    #          (Abnormal: marker done but find_pdf never ran)
    #          → Guard passes (stage_find_pdf=0, specs=None)
    #          → WILL fast-track. CRITICAL BUG.
    # -------------------------------------------------------------------------
    node30 = _create_node(
        manifest,
        device_id="test-marker-done",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_convert_marker=STAGE_COMPLETED,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 0, stage_convert_marker=COMPLETED, stage_find_pdf=0, specs=None",
        "node": node30,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "stage_convert_marker=2 but guard doesn't check it. "
                  "Would fast-track and clobber. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # CASE 31: Queue 0, stage_extract_specs=IN_PROGRESS, stage_find_pdf=0, specs=None
    #          (Abnormal: extraction in progress but find_pdf not started)
    #          → Guard passes. WILL fast-track.
    #          → CRITICAL BUG.
    # -------------------------------------------------------------------------
    node31 = _create_node(
        manifest,
        device_id="test-extract-in-progress",
        queue=QUEUE_0_INITIAL,
        stage_find_pdf=STAGE_NOT_STARTED,
        stage_extract_specs=STAGE_IN_PROGRESS,
        specs_json=None,
    )
    test_cases.append({
        "name": "Queue 0, stage_extract_specs=IN_PROGRESS, stage_find_pdf=0, specs=None",
        "node": node31,
        "expected_modified": True,
        "expected_fast_tracked": True,
        "reason": "stage_extract_specs=1 but guard doesn't check it. "
                  "Would fast-track while extraction is running. CRITICAL BUG.",
        "guard_vulnerability": True,
    })

    # -------------------------------------------------------------------------
    # Capture BEFORE states
    # -------------------------------------------------------------------------
    before_states = {}
    for tc in test_cases:
        before_states[tc["node"].device_id] = _node_state_dict(tc["node"])

    # -------------------------------------------------------------------------
    # Run the fast-path with monkey-patched extract_specs_from_patchify
    # -------------------------------------------------------------------------
    with patch("src.stages.extract_patchify_ports.extract_specs_from_patchify", side_effect=_dummy_specs):
        all_nodes = [tc["node"] for tc in test_cases]
        fast_nodes = _run_patchify_fast_path(all_nodes, manifest)
        fast_node_ids = {n.device_id for n in fast_nodes}

    # -------------------------------------------------------------------------
    # Evaluate results
    # -------------------------------------------------------------------------
    protected_count = 0
    unprotected_count = 0
    critical_issues = []
    unexpected_results = []

    print("=" * 100)
    print("PATCHIFY FAST-PATH GUARD COMPREHENSIVE TEST REPORT")
    print("=" * 100)
    print()

    for tc in test_cases:
        node = tc["node"]
        before = before_states[node.device_id]
        after_node = _reload_node(manifest, node.device_id)
        after = _node_state_dict(after_node)

        actually_modified = before != after
        actually_fast_tracked = node.device_id in fast_node_ids

        status = "PASS" if (actually_modified == tc["expected_modified"] and actually_fast_tracked == tc["expected_fast_tracked"]) else "FAIL"

        is_vulnerable = tc.get("guard_vulnerability", False)

        print(f"[{status}] {tc['name']}")
        print(f"      Expected modified={tc['expected_modified']}, fast_tracked={tc['expected_fast_tracked']}")
        print(f"      Actual   modified={actually_modified}, fast_tracked={actually_fast_tracked}")
        print(f"      Reason: {tc['reason']}")

        if status == "FAIL":
            unexpected_results.append({
                "case": tc["name"],
                "expected_modified": tc["expected_modified"],
                "actual_modified": actually_modified,
                "expected_fast_tracked": tc["expected_fast_tracked"],
                "actual_fast_tracked": actually_fast_tracked,
            })

        if actually_modified:
            if tc["expected_modified"]:
                if is_vulnerable:
                    print("      → MODIFIED (exposes guard vulnerability — was modified when it shouldn't be)")
                    unprotected_count += 1
                    critical_issues.append({
                        "case": tc["name"],
                        "issue": "Node was modified when it should have been protected",
                    })
                else:
                    print("      → Correctly modified (intended fast-track)")
            else:
                print("      → CRITICAL: Node was MODIFIED when it should have been PROTECTED!")
                critical_issues.append({
                    "case": tc["name"],
                    "issue": "Node was modified when it should have been protected",
                    "before": before,
                    "after": after,
                })
                unprotected_count += 1
        else:
            if tc["expected_modified"]:
                if is_vulnerable:
                    print("      → UNEXPECTED: Node was NOT modified despite passing guard. "
                          "Guard vulnerability exists but wasn't triggered in this run.")
                else:
                    print("      → Node was NOT modified when it SHOULD have been (expected fast-track)")
            else:
                print("      → Correctly protected")
                protected_count += 1

        print()

    # -------------------------------------------------------------------------
    # Summary
    # -------------------------------------------------------------------------
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"Total test cases:        {len(test_cases)}")
    print(f"Correctly protected:     {protected_count}")
    print(f"Unprotected (CRITICAL):  {unprotected_count}")
    print(f"Unexpected results:      {len(unexpected_results)}")
    print(f"Fast-tracked nodes:      {len(fast_nodes)}")
    print()

    if critical_issues:
        print("🚨 CRITICAL ISSUES — These states were NOT protected by the guard:")
        for issue in critical_issues:
            print(f"   - {issue['case']}")
            print(f"     {issue['issue']}")
        print()

    if unexpected_results:
        print("⚠️  UNEXPECTED RESULTS:")
        for ur in unexpected_results:
            print(f"   - {ur['case']}")
            print(f"     Expected modified={ur['expected_modified']}, got {ur['actual_modified']}")
            print(f"     Expected fast_tracked={ur['expected_fast_tracked']}, got {ur['actual_fast_tracked']}")
        print()

    # Guard vulnerability analysis
    vulnerabilities = [tc for tc in test_cases if tc.get("guard_vulnerability")]
    actually_vulnerable = []
    for tc in vulnerabilities:
        node = tc["node"]
        before = before_states[node.device_id]
        after_node = _reload_node(manifest, node.device_id)
        after = _node_state_dict(after_node)
        actually_modified = before != after
        if actually_modified:
            actually_vulnerable.append(tc)

    if actually_vulnerable:
        print("🔓 GUARD VULNERABILITIES CONFIRMED — The guard is too narrow. These states were incorrectly modified:")
        for tc in actually_vulnerable:
            print(f"   - {tc['name']}")
            print(f"     {tc['reason']}")
        print()
        print("RECOMMENDATION: The guard should check ANY non-default state, not just stage_find_pdf and specs_json.")
        print("  Suggested additional checks:")
        print("    - stage_resolve_sku != 0")
        print("    - stage_download_pdf != 0")
        print("    - stage_convert_marker != 0")
        print("    - stage_index_rag != 0")
        print("    - stage_extract_specs != 0")
        print("    - stage_generate_patch != 0")
        print("    - stage_validate_patch != 0")
        print("    - failure_stage is not None")
        print("    - pdf_url is not None")
        print("    - pdf_path is not None")
        print("    - ragscallion_job_id is not None")
        print("    - patch_source is not None")
        print("    - marked_suspicious is True")
        print("    - device_documents exist for this node")
        print()
    else:
        print("✅ No guard vulnerabilities were triggered in this run.")
        print()

    # Final verdict
    if unprotected_count == 0 and not unexpected_results:
        print("✅ ALL TESTS PASSED — Guard behaves exactly as expected.")
        return 0
    else:
        print(f"❌ TEST FAILURES: {unprotected_count} critical unprotected + {len(unexpected_results)} unexpected")
        return 1


if __name__ == "__main__":
    sys.exit(run_test())
