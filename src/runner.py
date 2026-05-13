"""Device ingestion runner — drives Stages 1-5 end-to-end.

Usage:
    python -m src.runner --devices devices.txt --cache-dir output/pdfs

The runner reads a pipe-separated device list, creates DeviceNodes, and drives
them through the pipeline using manifest queues.  It runs Stages 1-2 locally,
starts the Ragscallion polling loop in the background, submits completed Stage-2
devices to Ragscallion (Stage 3-4), and runs Stage 5 extraction on queue_3 nodes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config import settings
from .harness.manifest import (
    DeviceNode,
    FailureCategory,
    Manifest,
    QUEUE_0_INITIAL,
    QUEUE_1_CANNOT_FIND_PDF,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_3_READY_FOR_EXTRACTION,
    QUEUE_4_MANUAL_REVIEW,
    QUEUE_5_COMPLETED,
    STAGE_FIND_PDF,
    STAGE_DOWNLOAD_PDF,
    STAGE_CONVERT_MARKER,
    STAGE_INDEX_RAG,
    STAGE_EXTRACT_SPECS,
)
from .pipeline_stages import STAGE_NOT_STARTED
from .pipeline_stages import (
    _set_stage_failure,
    stage_0_resolve_sku,
    stage_1_find_pdf,
    stage_1b_find_html_source,
    stage_2_download_pdf,
    stage_3_4_submit_to_ragscallion,
    stage_5_extract_specs,
    process_stage_5_batch,
)
from .polling_loop import run_polling_loop
from .ragscallion_client import RagscallionClient
from .stages.generate_patch import generate_patch
from .stages.normalize_specs import normalize_extraction
from .stages.validate_patch import validate_patch
from .stages.classify_device import classify
from .stages.combined_device_context import get_combined_context

# Stage status constants reused from pipeline_stages
STAGE_COMPLETED = 2
STAGE_FAILED = 3

logger = logging.getLogger(__name__)

# Wall-clock hard cap (minutes)
HARD_CAP_MINUTES = 360

# Concurrency limits
MAX_CONCURRENT_STAGE_1 = 3
MAX_CONCURRENT_STAGE_2 = 5
MAX_CONCURRENT_STAGE_34 = 5


async def _run_stage_0_batch(
    manifest: Manifest,
    nodes: list[DeviceNode],
) -> list[DeviceNode]:
    """Run Stage 0 (resolve alias to canonical SKU) on all nodes concurrently.

    Returns the subset that successfully resolved (or already had a canonical SKU).
    """
    if not nodes:
        return []

    tasks = [stage_0_resolve_sku(node, manifest) for node in nodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[DeviceNode] = []
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            logger.error(f"Device {node.device_id} Stage 0 exception: {result}")
        elif result:
            successful.append(node)

    logger.info(f"Stage 0 complete: {len(successful)}/{len(nodes)} resolved")
    return successful


async def _run_stage_1_batch(
    manifest: Manifest,
    nodes: list[DeviceNode],
) -> list[DeviceNode]:
    """Run Stage 1 (find PDF) on all nodes concurrently (max 3)."""
    if not nodes:
        return []

    tasks = [stage_1_find_pdf(node, manifest) for node in nodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[DeviceNode] = []
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            logger.error(f"Device {node.device_id} Stage 1 exception: {result}")
        elif result:
            successful.append(node)

    logger.info(f"Stage 1 complete: {len(successful)}/{len(nodes)} succeeded")
    return successful


async def _run_stage_1b_batch(
    manifest: Manifest,
    nodes: list[DeviceNode],
    cache_dir: Path,
) -> list[DeviceNode]:
    """Run Stage 1b (HTML fallback) on nodes that failed PDF search.

    For each node, searches for an HTML product page, then either:
    - Extracts clean Markdown (static pages) -> submits as .md
    - Renders to PDF via Playwright (JS-heavy pages) -> submits as .pdf
    """
    if not nodes:
        return []

    tasks = [stage_1b_find_html_source(node, manifest, cache_dir) for node in nodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[DeviceNode] = []
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            logger.error(f"Device {node.device_id} Stage 1b exception: {result}")
        elif result:
            successful.append(node)

    logger.info(f"Stage 1b complete: {len(successful)}/{len(nodes)} HTML sources resolved")
    return successful


async def _run_stage_2_batch(
    manifest: Manifest,
    nodes: list[DeviceNode],
    cache_dir: Path,
) -> list[DeviceNode]:
    """Run Stage 2 (download PDF) on all nodes concurrently (max 5)."""
    if not nodes:
        return []

    tasks = [stage_2_download_pdf(node, manifest, cache_dir=cache_dir) for node in nodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[DeviceNode] = []
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            logger.error(f"Device {node.device_id} Stage 2 exception: {result}")
        elif result:
            successful.append(node)

    logger.info(f"Stage 2 complete: {len(successful)}/{len(nodes)} succeeded")
    return successful


async def _run_stage_34_batch(
    manifest: Manifest,
    nodes: list[DeviceNode],
    ragscallion_client: RagscallionClient,
) -> list[DeviceNode]:
    """Run Stage 3-4 (submit to Ragscallion) on all nodes concurrently (max 5)."""
    if not nodes:
        return []

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_STAGE_34)

    async def _submit(node: DeviceNode) -> bool:
        async with semaphore:
            return await stage_3_4_submit_to_ragscallion(node, ragscallion_client, manifest)

    tasks = [_submit(node) for node in nodes]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    successful: list[DeviceNode] = []
    for node, result in zip(nodes, results):
        if isinstance(result, Exception):
            logger.error(
                f"Device {node.device_id} Stage 3-4 exception: {type(result).__name__}: {result}"
            )
        elif result:
            successful.append(node)

    logger.info(f"Stage 3-4 complete: {len(successful)}/{len(nodes)} submitted")
    return successful


async def _run_stage_5_batch(
    manifest: Manifest,
    ragscallion_client: RagscallionClient,
) -> dict[str, int]:
    """Run Stage 5 (extract specs) on all nodes in queue_3."""
    return await process_stage_5_batch(manifest, ragscallion_client)


async def _run_scope_check(nodes: list[DeviceNode], manifest: Manifest) -> list[DeviceNode]:
    """Classify devices and reject IT/networking gear before any expensive work."""
    kept: list[DeviceNode] = []
    for node in nodes:
        # Skip nodes that are already processed or failed
        if node.queue != QUEUE_0_INITIAL:
            kept.append(node)
            continue
        # Skip nodes that already passed scope check in a previous pass
        if node.stage_resolve_sku != STAGE_NOT_STARTED:
            kept.append(node)
            continue

        # Pull EasySchematic context so the classifier has more than just
        # manufacturer + model (e.g., "Cisco Codec Pro" gets deviceType=codec).
        es_ctx = get_combined_context(node.device_id, node.manufacturer, node.model)

        # Tier 0: Trust EasySchematic deviceType for clearly-AV categories.
        # This bypasses the LLM entirely for known-AV types, preventing
        # brand-bias misclassifications (e.g., Cisco codec, Crestron AV-over-IP).
        AV_DEVICE_TYPES = {
            "codec", "av-over-ip", "dsp", "mixer", "speaker", "camera",
            "wireless_rx", "dante_stagebox", "dante_adapter_input",
            "dante_adapter_output", "switcher", "router", "matrix",
            "processor", "recorder", "player", "monitor", "display",
            "projector", "microphone", "headset", "intercom", "stagebox",
        }
        es_device_type = (es_ctx.get("device_type") or "").lower().strip() if es_ctx else ""
        if es_device_type in AV_DEVICE_TYPES:
            logger.info(
                f"Device {node.device_id}: AV pre-filter (device_type={es_device_type}) — kept"
            )
            kept.append(node)
            continue

        excerpt = ""
        if es_ctx and es_ctx.get("sources"):
            parts = []
            if es_ctx.get("device_type"):
                parts.append(f"Device type: {es_ctx['device_type']}")
            if es_ctx.get("category"):
                parts.append(f"Category: {es_ctx['category']}")
            if es_ctx.get("ports"):
                port_summary = ", ".join(
                    f"{p['direction']} {p['signal_type']}"
                    for p in es_ctx["ports"][:8]
                )
                parts.append(f"Ports: {port_summary}")
            excerpt = "\n".join(parts)

        classification = await classify(
            node.manufacturer, node.model, markdown_excerpt=excerpt or None
        )
        if classification.class_ == "it_networking":
            logger.info(
                f"Device {node.device_id}: REJECTED — classified as {classification.class_} "
                f"({classification.source}, confidence={classification.confidence})"
            )
            node.failure_stage = STAGE_FIND_PDF  # 0 = before any real work
            node.failure_category = FailureCategory.OUT_OF_SCOPE.value
            node.failure_message = (
                f"Out of scope: IT/networking device ({classification.source})"
            )
            node.failure_retryable = False
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            manifest.persist(node)
        else:
            kept.append(node)

    rejected = len(nodes) - len(kept)
    if rejected:
        logger.info(f"Scope check: {rejected} device(s) rejected as out-of-scope")
    return kept


def _run_patchify_fast_path(
    nodes: list[DeviceNode],
    manifest: Manifest,
) -> list[DeviceNode]:
    """Fast-path: generate specs from patchify inputs/outputs, skip PDF hunt.

    For community entries that already have user-defined port data, we can
    bypass Stage 0-5 entirely and jump straight to Stage 6-7 (patch generation).

    Garbage filter:
        - manufacturer == "Generic" AND zero ports AND no SKU  → reject
        - Everything else with ports  → fast-track
    """
    from .stages.extract_patchify_ports import extract_specs_from_patchify
    from .harness.manifest import QUEUE_5_COMPLETED

    fast_nodes: list[DeviceNode] = []

    for node in nodes:
        # SAFETY GUARD — comprehensive check for any prior pipeline state.
        # A node is ONLY eligible for fast-pathing if it has never done any
        # pipeline work AND has no artifacts. Any hint of prior progress
        # (even failures, in-progress stages, or empty specs_json) means we
        # must skip it to avoid data loss.
        has_any_progress = (
            node.stage_resolve_sku != STAGE_NOT_STARTED
            or node.stage_find_pdf != STAGE_NOT_STARTED
            or node.stage_download_pdf != STAGE_NOT_STARTED
            or node.stage_convert_marker != STAGE_NOT_STARTED
            or node.stage_index_rag != STAGE_NOT_STARTED
            or node.stage_extract_specs != STAGE_NOT_STARTED
            or node.stage_generate_patch != STAGE_NOT_STARTED
            or node.stage_validate_patch != STAGE_NOT_STARTED
            or bool(node.specs_json)          # None or ""
            or bool(node.pdf_url)
            or bool(node.pdf_path)
            or bool(node.ragscallion_job_id)
            or bool(node.patch_source)
            or node.failure_stage is not None
            or node.marked_suspicious
            or bool(manifest.list_documents(node.device_id))
        )
        if has_any_progress:
            continue

        specs = extract_specs_from_patchify(node.device_id)
        if not specs:
            continue

        ports = specs.get("signal_flow", {}).get("ports", [])

        # Garbage filter: Generic manufacturer + zero ports + no SKU
        if (
            node.manufacturer.strip().lower() == "generic"
            and len(ports) == 0
            and not _get_patchify_sku(node.device_id)
        ):
            logger.info(
                f"Device {node.device_id}: rejected as garbage "
                f"(Generic + zero ports + no SKU)"
            )
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_FIND_PDF,
                category=FailureCategory.PDF_NOT_FOUND,
                message="Garbage entry: Generic manufacturer with no ports and no SKU",
                retryable=False,
            )
            continue

        # Store specs and mark stages 0-5 as completed
        node.specs_json = json.dumps(specs)
        node.stage_resolve_sku = STAGE_COMPLETED
        node.stage_find_pdf = STAGE_COMPLETED
        node.stage_download_pdf = STAGE_COMPLETED
        node.stage_index_rag = STAGE_COMPLETED
        node.stage_extract_specs = STAGE_COMPLETED
        node.queue = QUEUE_5_COMPLETED
        manifest.persist(node)
        fast_nodes.append(node)
        logger.info(
            f"Device {node.device_id}: fast-tracked from patchify "
            f"({len(ports)} port(s))"
        )

    return fast_nodes


def _get_patchify_sku(device_id: str) -> Optional[str]:
    """Return SKU from patchify entry, if any."""
    from .stages.extract_patchify_ports import get_patchify_item
    item = get_patchify_item(device_id)
    if item:
        return item.get("sku") or None
    return None


async def _run_stage_67_batch(manifest: Manifest) -> dict[str, int]:
    """Run Stage 6 (generate patch) and Stage 7 (validate) on queue_5 nodes."""
    from .stages.generate_patch import generate_patch
    from .stages.normalize_specs import normalize_extraction
    from .stages.validate_patch import validate_patch
    from .stages.classify_device import classify
    from .stages.review_completeness import check_completeness
    from .harness.manifest import (
        QUEUE_3_READY_FOR_EXTRACTION,
        QUEUE_4_MANUAL_REVIEW,
        QUEUE_5_COMPLETED,
        FailureCategory,
    )

    nodes = [
        n for n in manifest.list_by_queue(QUEUE_5_COMPLETED)
        if n.stage_generate_patch == 0 and n.specs_json
    ]
    # Also retry queue_4 nodes that failed Stage 6-7 but are retryable
    retry_nodes = [
        n for n in manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)
        if n.stage_generate_patch == 0 and n.specs_json and n.failure_retryable
    ]
    nodes = list({n.device_id: n for n in nodes + retry_nodes}.values())
    if not nodes:
        return {"successful": 0, "failed": 0}

    logger.info(f"Stage 6-7: processing {len(nodes)} nodes")
    successful = 0
    failed = 0

    for node in nodes:
        try:
            extracted = json.loads(node.specs_json)

            # Warn on low-confidence extractions — often means the PDF was a
            # marketing brochure rather than a technical manual.
            confidence = extracted.get("extraction_confidence", "").lower()
            ports = extracted.get("signal_flow", {}).get("ports", [])
            if confidence == "low" and len(ports) == 0:
                logger.warning(
                    f"Device {node.device_id}: extraction confidence is LOW and "
                    f"zero ports found — rejecting as insufficient data."
                )
                node.failure_stage = 5
                node.failure_category = FailureCategory.EXTRACTION_FAILED.value
                node.failure_message = (
                    "Low confidence extraction with zero ports. "
                    "The indexed PDF likely lacks technical I/O specifications."
                )
                node.failure_retryable = True
                node.failure_attempts += 1
                node.failure_at = datetime.now(timezone.utc).isoformat()
                node.queue = QUEUE_4_MANUAL_REVIEW
                node.stage_generate_patch = 3
                manifest.persist(node)
                failed += 1
                continue

            if confidence == "low" and len(ports) > 0:
                logger.warning(
                    f"Device {node.device_id}: extraction confidence is LOW — "
                    f"the PDF may be a marketing brochure. Consider re-searching."
                )

            classification = await classify(node.manufacturer, node.model)
            normalized = normalize_extraction(extracted, classification.class_)
            patch_source = generate_patch(normalized)
            node.stage_generate_patch = 2

            is_valid, errors = validate_patch(patch_source)
            if not is_valid:
                node.failure_stage = 7
                node.failure_category = FailureCategory.PATCH_VALIDATION_FAILED.value
                node.failure_message = f"Patch validation failed: {errors}"
                node.failure_retryable = False
                node.failure_attempts += 1
                node.failure_at = datetime.now(timezone.utc).isoformat()
                node.queue = QUEUE_4_MANUAL_REVIEW
                node.stage_validate_patch = 3
                node.patch_source = patch_source
                failed += 1
                logger.warning(f"Device {node.device_id} patch validation failed: {errors}")
                manifest.persist(node)
                continue

            # Stage 7.5: Completeness review
            node.stage_validate_patch = 2
            is_complete, concerns = check_completeness(normalized, classification.class_)
            if not is_complete:
                review_attempts = extracted.get("_completeness_review_attempts", 0) + 1
                logger.warning(
                    f"Device {node.device_id}: completeness review FAILED "
                    f"(attempt {review_attempts}) — {concerns}"
                )
                if review_attempts >= 2:
                    # Two strikes — move to manual review rather than loop forever
                    node.failure_stage = 7
                    node.failure_category = FailureCategory.PATCH_VALIDATION_FAILED.value
                    node.failure_message = (
                        f"Completeness review failed after {review_attempts} attempts: {concerns}"
                    )
                    node.failure_retryable = True
                    node.failure_attempts += 1
                    node.failure_at = datetime.now(timezone.utc).isoformat()
                    node.queue = QUEUE_4_MANUAL_REVIEW
                    node.stage_generate_patch = 3
                    manifest.persist(node)
                    failed += 1
                    continue

                # Embed concerns so the next extraction pass can critique itself
                extracted["_completeness_concerns"] = concerns
                extracted["_completeness_review_attempts"] = review_attempts
                node.specs_json = json.dumps(extracted)
                # Reset stages so the node is re-extracted on next pipeline pass
                node.stage_extract_specs = STAGE_NOT_STARTED
                node.stage_generate_patch = STAGE_NOT_STARTED
                node.stage_validate_patch = STAGE_NOT_STARTED
                node.queue = QUEUE_3_READY_FOR_EXTRACTION
                node.patch_source = ""
                manifest.persist(node)
                failed += 1
                continue

            node.patch_source = patch_source
            node.queue = QUEUE_5_COMPLETED
            successful += 1
            logger.info(f"Device {node.device_id} patch generated and validated")
        except Exception as e:
            node.failure_stage = 6
            node.failure_category = FailureCategory.PATCH_GENERATION_FAILED.value
            node.failure_message = f"Patch generation/validation error: {e}"
            node.failure_retryable = True
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            failed += 1
            logger.warning(f"Device {node.device_id} Stage 6-7 error: {e}")

        manifest.persist(node)

    logger.info(f"Stage 6-7 complete: {successful} successful, {failed} failed")
    return {"successful": successful, "failed": failed}


def _load_devices(path: Path) -> list[tuple[str, str, str]]:
    """Read devices.txt lines as (manufacturer, model, device_id)."""
    devices: list[tuple[str, str, str]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|")
            if len(parts) != 3:
                logger.warning(f"Skipping malformed line: {line}")
                continue
            devices.append((parts[0], parts[1], parts[2]))
    return devices


def _create_nodes(
    manifest: Manifest,
    devices: list[tuple[str, str, str]],
) -> list[DeviceNode]:
    """Create and persist DeviceNodes for each device."""
    nodes: list[DeviceNode] = []
    for manufacturer, model, device_id in devices:
        # Ragscallion corpus_id must match ^[a-z0-9][a-z0-9_-]{0,63}$
        # Sanitize: lowercase, replace invalid chars with underscore, truncate.
        corpus_id = re.sub(r"[^a-z0-9_-]", "_", device_id.lower())[:64]
        if corpus_id and not re.match(r"^[a-z0-9]", corpus_id):
            corpus_id = "d" + corpus_id[:63]

        node = DeviceNode(
            device_id=device_id,
            manufacturer=manufacturer,
            model=model,
            corpus_id=corpus_id,
            queue=QUEUE_0_INITIAL,
        )
        if manifest.add_node(node):
            nodes.append(node)
            logger.info(f"Added device {device_id}")
        else:
            # Node already exists — load it and backfill corpus_id if missing
            existing = manifest.get_node(device_id)
            if existing:
                if not existing.corpus_id:
                    existing.corpus_id = corpus_id
                    manifest.persist(existing)
                nodes.append(existing)
                logger.info(f"Loaded existing device {device_id}")
    return nodes


def _summarize_device_status(nodes: list[DeviceNode]) -> None:
    """Log how many input devices are new, in-progress, completed, or failed."""
    counts: dict[int, list[str]] = {
        QUEUE_0_INITIAL: [],
        QUEUE_1_CANNOT_FIND_PDF: [],
        QUEUE_2_POLLING_RAGSCALLION: [],
        QUEUE_3_READY_FOR_EXTRACTION: [],
        QUEUE_4_MANUAL_REVIEW: [],
        QUEUE_5_COMPLETED: [],
    }
    for node in nodes:
        counts.setdefault(node.queue, []).append(node.device_id)

    new = len(counts[QUEUE_0_INITIAL])
    stuck = len(counts[QUEUE_1_CANNOT_FIND_PDF])
    polling = len(counts[QUEUE_2_POLLING_RAGSCALLION])
    ready = len(counts[QUEUE_3_READY_FOR_EXTRACTION])
    failed = len(counts[QUEUE_4_MANUAL_REVIEW])
    completed = len(counts[QUEUE_5_COMPLETED])

    total = len(nodes)
    logger.info(
        f"Device status summary ({total} total): "
        f"{new} new, {stuck} stuck, {polling} polling, {ready} ready, "
        f"{failed} failed, {completed} completed"
    )
    if completed:
        logger.info(f"  Already completed (will skip): {', '.join(counts[QUEUE_5_COMPLETED])}")
    if failed:
        logger.info(f"  Previously failed (will retry if retryable): {', '.join(counts[QUEUE_4_MANUAL_REVIEW])}")


def _is_pipeline_done(manifest: Manifest) -> bool:
    """Return True if no nodes remain in queues 0, 1, 2, or 3."""
    for q in (QUEUE_0_INITIAL, QUEUE_1_CANNOT_FIND_PDF, QUEUE_2_POLLING_RAGSCALLION, QUEUE_3_READY_FOR_EXTRACTION):
        if manifest.list_by_queue(q):
            return False
    return True


def _print_summary(manifest: Manifest) -> None:
    """Print one-line summary per device."""
    all_nodes: list[DeviceNode] = []
    for q in range(6):
        all_nodes.extend(manifest.list_by_queue(q))

    # Deduplicate by device_id
    seen: set[str] = set()
    for node in all_nodes:
        if node.device_id in seen:
            continue
        seen.add(node.device_id)

        specs_bytes = len(node.specs_json.encode("utf-8")) if node.specs_json else 0
        stage_5_status = "COMPLETED" if node.stage_extract_specs == 2 else "NOT_DONE"

        if node.failure_stage is not None:
            reason = f"failure_stage={node.failure_stage} category={node.failure_category}"
        else:
            reason = f"stage_5={stage_5_status} specs_json_bytes={specs_bytes}"

        print(f"{node.device_id}: {reason}")


def _query_stage_0_nodes(manifest: Manifest) -> list[DeviceNode]:
    return list({
        n.device_id: n for n in (
            manifest.list_by_queue(QUEUE_0_INITIAL)
            + manifest.list_by_queue(QUEUE_1_CANNOT_FIND_PDF)
        )
        if n.stage_resolve_sku in (STAGE_NOT_STARTED, STAGE_FAILED)
    }.values())


def _query_stage_1_nodes(manifest: Manifest) -> list[DeviceNode]:
    return list({
        n.device_id: n for n in (
            manifest.list_by_queue(QUEUE_0_INITIAL)
            + manifest.list_by_queue(QUEUE_1_CANNOT_FIND_PDF)
        )
        if n.stage_resolve_sku == STAGE_COMPLETED
        and n.stage_find_pdf == STAGE_NOT_STARTED
        and n.canonical_sku
    }.values())


def _query_stage_1b_nodes(manifest: Manifest) -> list[DeviceNode]:
    return [
        n for n in (
            manifest.list_by_queue(QUEUE_1_CANNOT_FIND_PDF)
            + manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)
        )
        if n.failure_category in (
            FailureCategory.PDF_NOT_FOUND.value,
            "HTML_SOURCE_NOT_FOUND",
        )
        and n.failure_retryable
        and n.failure_attempts <= 2
    ]


def _query_stage_2_nodes(manifest: Manifest) -> list[DeviceNode]:
    return list({
        n.device_id: n for n in (
            manifest.list_by_queue(QUEUE_0_INITIAL)
            + manifest.list_by_queue(QUEUE_1_CANNOT_FIND_PDF)
        )
        if n.stage_find_pdf == STAGE_COMPLETED
        and n.stage_download_pdf not in (STAGE_COMPLETED, 1)
        and n.failure_attempts < 5
    }.values())


def _query_stage_34_nodes(manifest: Manifest) -> list[DeviceNode]:
    return list({
        n.device_id: n for n in (
            manifest.list_by_queue(QUEUE_0_INITIAL)
            + manifest.list_by_queue(QUEUE_2_POLLING_RAGSCALLION)
            + manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)
        )
        if n.stage_download_pdf == STAGE_COMPLETED
        and n.stage_index_rag not in (STAGE_COMPLETED, 1)
        and not (n.queue == QUEUE_4_MANUAL_REVIEW and not n.failure_retryable)
    }.values())


async def run_pipeline(
    devices_path: Path,
    cache_dir: Path,
    manifest_db: Optional[Path] = None,
) -> None:
    """Drive the full pipeline for devices listed in *devices_path*."""
    manifest_db = manifest_db or settings.manifests_db
    manifest = Manifest(manifest_db)
    ragscallion_client = RagscallionClient()

    # Load/create nodes
    devices = _load_devices(devices_path)
    nodes = _create_nodes(manifest, devices)

    if not nodes:
        logger.warning("No devices to process")
        return

    # Scope check: reject IT/networking gear before spending money on PDF search/index/extract
    nodes = await _run_scope_check(nodes, manifest)

    if not nodes:
        logger.warning("All devices were rejected as out-of-scope")
        return

    # Fast-path: generate specs directly from patchify inputs/outputs when available.
    patchify_fast_nodes = _run_patchify_fast_path(nodes, manifest)
    if patchify_fast_nodes:
        logger.info(
            f"Patchify fast-path: {len(patchify_fast_nodes)} device(s) skipped "
            f"PDF hunt (ports extracted from patchify inputs/outputs)"
        )
        fast_ids = {n.device_id for n in patchify_fast_nodes}
        nodes = [n for n in nodes if n.device_id not in fast_ids]

    _summarize_device_status(nodes)

    logger.info(f"Starting pipeline for {len(nodes)} devices")

    # Start polling loop in background
    polling_task = asyncio.create_task(
        run_polling_loop(ragscallion_client, manifest),
        name="ragscallion_polling",
    )

    start_time = datetime.now(timezone.utc)
    stop_event = asyncio.Event()

    # Per-stage workers: each polls the DB continuously and processes devices
    # as soon as they become ready. Device A advances to Stage 2 the moment
    # Stage 1 finishes for it — no waiting for the rest of the batch.

    async def _stage_0_worker() -> None:
        while not stop_event.is_set():
            try:
                batch = _query_stage_0_nodes(manifest)
                if batch:
                    await _run_stage_0_batch(manifest, batch)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Stage 0 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_1_worker() -> None:
        while not stop_event.is_set():
            try:
                batch = _query_stage_1_nodes(manifest)
                if batch:
                    await _run_stage_1_batch(manifest, batch[:MAX_CONCURRENT_STAGE_1])
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Stage 1 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_1b_worker() -> None:
        while not stop_event.is_set():
            try:
                batch = _query_stage_1b_nodes(manifest)
                if batch:
                    logger.info(f"Stage 1b: HTML fallback for {len(batch)} device(s)")
                    await _run_stage_1b_batch(manifest, batch, cache_dir)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Stage 1b worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_2_worker() -> None:
        while not stop_event.is_set():
            try:
                batch = _query_stage_2_nodes(manifest)
                if batch:
                    await _run_stage_2_batch(manifest, batch[:MAX_CONCURRENT_STAGE_2], cache_dir)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Stage 2 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_34_worker() -> None:
        while not stop_event.is_set():
            try:
                batch = _query_stage_34_nodes(manifest)
                if batch:
                    await _run_stage_34_batch(manifest, batch[:MAX_CONCURRENT_STAGE_34], ragscallion_client)
                await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"Stage 3-4 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_5_worker() -> None:
        while not stop_event.is_set():
            try:
                result = await _run_stage_5_batch(manifest, ragscallion_client)
                total = result.get("successful", 0) + result.get("failed", 0) + result.get("exceptions", 0)
                if total == 0:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Stage 5 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _stage_67_worker() -> None:
        while not stop_event.is_set():
            try:
                result = await _run_stage_67_batch(manifest)
                if result.get("successful", 0) + result.get("failed", 0) == 0:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Stage 6-7 worker error (will retry): {e}")
                await asyncio.sleep(2)

    async def _completion_checker() -> None:
        while True:
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed > HARD_CAP_MINUTES * 60:
                logger.warning(f"Hard cap of {HARD_CAP_MINUTES} minutes reached")
                stop_event.set()
                break
            if _is_pipeline_done(manifest):
                logger.info("Pipeline complete — no nodes remain in active queues")
                stop_event.set()
                break
            await asyncio.sleep(5)

    try:
        await asyncio.gather(
            _stage_0_worker(),
            _stage_1_worker(),
            _stage_1b_worker(),
            _stage_2_worker(),
            _stage_34_worker(),
            _stage_5_worker(),
            _stage_67_worker(),
            _completion_checker(),
            return_exceptions=True,
        )
    finally:
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass
        await ragscallion_client.close()

    _print_summary(manifest)


def main_cli() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="SignalCanvas device ingestion runner")
    parser.add_argument(
        "--devices",
        type=Path,
        required=True,
        help="Path to pipe-separated device list (manufacturer|model|device_id)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("output/pdfs"),
        help="Directory to cache downloaded PDFs",
    )
    parser.add_argument(
        "--manifest-db",
        type=Path,
        default=None,
        help="Path to SQLite manifest database (default: output/ingestion.db)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    asyncio.run(
        run_pipeline(
            devices_path=args.devices,
            cache_dir=args.cache_dir,
            manifest_db=args.manifest_db,
        )
    )


if __name__ == "__main__":
    main_cli()
