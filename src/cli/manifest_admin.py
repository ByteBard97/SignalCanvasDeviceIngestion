#!/usr/bin/env python3
"""Manifest admin CLI — manipulate device nodes in the ingestion database.

Usage:
    python -m src.cli.manifest_admin --db output/batch_20_v3.db status
    python -m src.cli.manifest_admin --db output/batch_20_v3.db oos cisco-sf200-24 ubiquiti-usw-enterprise-8-poe
    python -m src.cli.manifest_admin --db output/batch_20_v3.db reset-queue 0 cisco-sf200-24
    python -m src.cli.manifest_admin --db output/batch_20_v3.db clear device-id-1 device-id-2
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.harness.manifest import (
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


QUEUE_LABELS = {
    QUEUE_0_INITIAL: "INITIAL",
    QUEUE_1_CANNOT_FIND_PDF: "CANNOT_FIND_PDF",
    QUEUE_2_POLLING_RAGSCALLION: "POLLING_RAG",
    QUEUE_3_READY_FOR_EXTRACTION: "READY_EXTRACT",
    QUEUE_4_MANUAL_REVIEW: "MANUAL_REVIEW",
    QUEUE_5_COMPLETED: "COMPLETED",
}


def cmd_status(args: argparse.Namespace) -> None:
    """Print one-line status for every device in the manifest."""
    manifest = Manifest(Path(args.db))
    all_nodes = []
    for q in range(6):
        all_nodes.extend(manifest.list_by_queue(q))

    seen: set[str] = set()
    nodes = [n for n in all_nodes if not (n.device_id in seen or seen.add(n.device_id))]

    if not nodes:
        print("No devices in manifest.")
        return

    print(f"{'Device ID':<50} {'Queue':<16} {'Stage':<6} {'Category':<25} {'Retry':<6} {'Specs':>6} {'Patch':>6}")
    print("-" * 120)

    for n in sorted(nodes, key=lambda x: x.device_id):
        queue_label = QUEUE_LABELS.get(n.queue, f"Q{n.queue}")
        stage = f"F{n.failure_stage}" if n.failure_stage is not None else "-"
        category = n.failure_category or "-"
        retry = "yes" if n.failure_retryable else "no"
        specs = len(n.specs_json) if n.specs_json else 0
        patch = len(n.patch_source) if n.patch_source else 0
        print(
            f"{n.device_id:<50} {queue_label:<16} {stage:<6} {category:<25} {retry:<6} {specs:>6} {patch:>6}"
        )

    # Summary
    from collections import Counter

    queues = Counter(n.queue for n in nodes)
    print()
    print("Summary:")
    for q, c in sorted(queues.items()):
        print(f"  {QUEUE_LABELS.get(q, f'Q{q}')}: {c}")


def cmd_oos(args: argparse.Namespace) -> None:
    """Mark device(s) as out-of-scope."""
    manifest = Manifest(Path(args.db))
    for device_id in args.device_ids:
        node = manifest.get_node(device_id)
        if not node:
            print(f"[SKIP] {device_id}: not found in manifest")
            continue

        node.queue = QUEUE_4_MANUAL_REVIEW
        node.failure_stage = 0
        node.failure_category = FailureCategory.OUT_OF_SCOPE.value
        node.failure_message = "Device classified as IT/networking infrastructure — out of scope for SignalCanvas"
        node.failure_retryable = False
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.patch_source = None
        node.specs_json = None
        node.stage_generate_patch = STAGE_NOT_STARTED
        node.stage_validate_patch = STAGE_NOT_STARTED
        node.stage_extract_specs = STAGE_NOT_STARTED
        manifest.persist(node)
        print(f"[OOS]  {device_id}: marked out-of-scope")


def cmd_clear(args: argparse.Namespace) -> None:
    """Hard-reset device(s) to queue_0, clearing all progress and failure state."""
    manifest = Manifest(Path(args.db))
    for device_id in args.device_ids:
        node = manifest.get_node(device_id)
        if not node:
            print(f"[SKIP] {device_id}: not found in manifest")
            continue

        node.queue = QUEUE_0_INITIAL
        node.failure_stage = None
        node.failure_category = None
        node.failure_message = None
        node.failure_retryable = False
        node.failure_attempts = 0
        node.failure_at = None
        node.pdf_url = None
        node.pdf_path = None
        node.specs_json = None
        node.patch_source = None
        node.ragscallion_job_id = None
        node.stage_resolve_sku = STAGE_NOT_STARTED
        node.stage_find_pdf = STAGE_NOT_STARTED
        node.stage_download_pdf = STAGE_NOT_STARTED
        node.stage_convert_marker = STAGE_NOT_STARTED
        node.stage_index_rag = STAGE_NOT_STARTED
        node.stage_extract_specs = STAGE_NOT_STARTED
        node.stage_generate_patch = STAGE_NOT_STARTED
        node.stage_validate_patch = STAGE_NOT_STARTED
        manifest.persist(node)
        print(f"[CLR]  {device_id}: reset to INITIAL")


def cmd_reset_queue(args: argparse.Namespace) -> None:
    """Move device(s) to a specific queue without clearing other state."""
    manifest = Manifest(Path(args.db))
    target_queue = int(args.queue_id)
    for device_id in args.device_ids:
        node = manifest.get_node(device_id)
        if not node:
            print(f"[SKIP] {device_id}: not found in manifest")
            continue

        old = QUEUE_LABELS.get(node.queue, f"Q{node.queue}")
        node.queue = target_queue
        manifest.persist(node)
        new = QUEUE_LABELS.get(target_queue, f"Q{target_queue}")
        print(f"[MV]   {device_id}: {old} -> {new}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Manifest admin CLI")
    parser.add_argument("--db", type=Path, default=Path("output/ingestion.db"), help="Manifest SQLite DB path")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("status", help="Show all devices and their queue/status")

    p_oos = sub.add_parser("oos", help="Mark device(s) as out-of-scope")
    p_oos.add_argument("device_ids", nargs="+", help="Device IDs to mark")

    p_clear = sub.add_parser("clear", help="Hard-reset device(s) to queue_0 (wipes all progress)")
    p_clear.add_argument("device_ids", nargs="+", help="Device IDs to clear")

    p_reset = sub.add_parser("reset-queue", help="Move device(s) to a specific queue")
    p_reset.add_argument("queue_id", type=int, choices=range(6), help="Target queue ID (0-5)")
    p_reset.add_argument("device_ids", nargs="+", help="Device IDs to move")

    args = parser.parse_args()

    handlers = {
        "status": cmd_status,
        "oos": cmd_oos,
        "clear": cmd_clear,
        "reset-queue": cmd_reset_queue,
    }
    handlers[args.cmd](args)


if __name__ == "__main__":
    main()
