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
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .config import settings
from .harness.manifest import (
    DeviceNode,
    Manifest,
    QUEUE_0_INITIAL,
    QUEUE_1_CANNOT_FIND_PDF,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_3_READY_FOR_EXTRACTION,
    QUEUE_4_MANUAL_REVIEW,
    STAGE_FIND_PDF,
    STAGE_DOWNLOAD_PDF,
    STAGE_CONVERT_MARKER,
    STAGE_INDEX_RAG,
    STAGE_EXTRACT_SPECS,
)
from .pipeline_stages import (
    stage_0_resolve_sku,
    stage_1_find_pdf,
    stage_2_download_pdf,
    stage_3_4_submit_to_ragscallion,
    stage_5_extract_specs,
    process_stage_5_batch,
)
from .polling_loop import run_polling_loop
from .ragscallion_client import RagscallionClient

logger = logging.getLogger(__name__)

# Wall-clock hard cap (minutes)
HARD_CAP_MINUTES = 30

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
            logger.error(f"Device {node.device_id} Stage 3-4 exception: {result}")
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
        node = DeviceNode(
            device_id=device_id,
            manufacturer=manufacturer,
            model=model,
            corpus_id=device_id,
            queue=QUEUE_0_INITIAL,
        )
        if manifest.add_node(node):
            nodes.append(node)
            logger.info(f"Added device {device_id}")
        else:
            # Node already exists — load it
            existing = manifest.get_node(device_id)
            if existing:
                nodes.append(existing)
                logger.info(f"Loaded existing device {device_id}")
    return nodes


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

    logger.info(f"Starting pipeline for {len(nodes)} devices")

    # Start polling loop in background
    polling_task = asyncio.create_task(
        run_polling_loop(ragscallion_client, manifest),
        name="ragscallion_polling",
    )

    start_time = datetime.now(timezone.utc)

    try:
        # Stage 0: Resolve user-facing aliases to canonical manufacturer SKUs
        queue_0_nodes = manifest.list_by_queue(QUEUE_0_INITIAL)
        stage_0_success = await _run_stage_0_batch(manifest, queue_0_nodes)

        # Stage 1: Find PDF URLs (only for nodes that resolved their SKU)
        # Re-load to pick up canonical_sku written by Stage 0.
        stage_0_done = [
            manifest.get_node(n.device_id)
            for n in stage_0_success
            if manifest.get_node(n.device_id) and manifest.get_node(n.device_id).canonical_sku
        ]
        stage_1_success = await _run_stage_1_batch(manifest, stage_0_done)

        # Stage 2: Download PDFs
        # Re-load nodes that completed stage 1 (queue may have changed)
        stage_1_done = [
            manifest.get_node(n.device_id)
            for n in stage_1_success
            if manifest.get_node(n.device_id) and manifest.get_node(n.device_id).stage_find_pdf == 2
        ]
        stage_2_success = await _run_stage_2_batch(manifest, stage_1_done, cache_dir)

        # Stage 3-4: Submit to Ragscallion
        stage_2_done = [
            manifest.get_node(n.device_id)
            for n in stage_2_success
            if manifest.get_node(n.device_id) and manifest.get_node(n.device_id).stage_download_pdf == 2
        ]
        await _run_stage_34_batch(manifest, stage_2_done, ragscallion_client)

        # Wait for queue_3 nodes to appear (polling loop moves them from queue_2)
        logger.info("Waiting for Ragscallion indexing to complete...")
        while True:
            elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
            if elapsed > HARD_CAP_MINUTES * 60:
                logger.warning(f"Hard cap of {HARD_CAP_MINUTES} minutes reached")
                break

            queue_3_nodes = manifest.list_by_queue(QUEUE_3_READY_FOR_EXTRACTION)
            queue_2_nodes = manifest.list_by_queue(QUEUE_2_POLLING_RAGSCALLION)

            if queue_3_nodes:
                # Stage 5: Extract specs
                await _run_stage_5_batch(manifest, ragscallion_client)

            if _is_pipeline_done(manifest):
                logger.info("Pipeline complete — no nodes remain in active queues")
                break

            if not queue_2_nodes and not queue_3_nodes:
                # Nothing in flight and nothing to extract
                break

            await asyncio.sleep(5)

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
