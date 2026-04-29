"""Stage implementations for device ingestion pipeline.

Each stage is a pure async function that processes a device node and persists results.
Stages use constants for queue IDs and stage codes (no magic numbers).
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from .harness.manifest import DeviceNode, Manifest, QUEUE_2_POLLING_RAGSCALLION, QUEUE_4_MANUAL_REVIEW, STAGE_INDEX_RAG
from .ragscallion_client import (
    RagscallionClient,
    RagscallionCollisionError,
    RagscallionError,
    RagscallionUnavailableError,
)

logger = logging.getLogger(__name__)

# Stage progress constants (no magic numbers)
STAGE_NOT_STARTED = 0
STAGE_IN_PROGRESS = 1
STAGE_COMPLETED = 2
STAGE_FAILED = 3


async def stage_3_4_submit_to_ragscallion(
    node: DeviceNode,
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
) -> bool:
    """Submit PDF to Ragscallion for indexing. Retry up to 3 times on transient failures.

    Input:
        node: Device node with pdf_path from stage 2
        ragscallion_client: RagscallionClient instance (handles retries)
        manifest: Manifest instance for persistence

    Output:
        True if successfully submitted to queue_2 (polling)
        False if moved to queue_4 (manual review) due to collision or failure

    Processing:
        - Submit PDF with corpus_id (device_id) and source_label (manufacturer model)
        - Success: store job metadata, move to queue_2 for polling
        - Collision (409): move to queue_4 with RAGDB_COLLISION category
        - Transient failures: RagscallionClient retries with backoff
        - After 3 failures: move to queue_4 for manual review

    Side Effects:
        - Updates node: ragscallion_job_id, corpus_id, ragscallion_submitted_at, stage markers, queue
        - Persists node to manifest
        - Logs submission result
    """
    corpus_id = node.device_id  # e.g., "yamaha-r08d"
    source_label = f"{node.manufacturer} {node.model}"

    try:
        # Submit PDF to Ragscallion with retry logic handled by client
        job = await ragscallion_client.submit_ingest(
            pdf_path=node.pdf_path,
            corpus_id=corpus_id,
            source_label=source_label,
            on_conflict="error",  # Reject accidental re-submissions
        )

        # Success: store job metadata
        node.ragscallion_job_id = job["job_id"]
        node.corpus_id = corpus_id
        node.ragscallion_submitted_at = datetime.now(timezone.utc).isoformat()
        node.stage_convert_marker = STAGE_IN_PROGRESS
        node.stage_index_rag = STAGE_IN_PROGRESS
        node.queue = QUEUE_2_POLLING_RAGSCALLION

        manifest.persist(node)
        logger.info(
            f"Device {node.device_id} submitted to Ragscallion, job_id={job['job_id']}"
        )
        return True

    except RagscallionCollisionError as e:
        # source_label already exists in corpus → manual review required
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = "RAGDB_COLLISION"
        node.failure_message = f"source_label '{source_label}' already in corpus '{corpus_id}'"
        node.failure_retryable = False
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} collision: {e}")
        return False

    except RagscallionUnavailableError as e:
        # Ragscallion unavailable after 3 retries
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = "RAGSCALLION_UNAVAILABLE"
        node.failure_message = f"Failed to submit after 3 retries: {e}"
        node.failure_retryable = True
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission failed: {e}")
        return False

    except RagscallionError as e:
        # Other Ragscallion errors (validation, etc.)
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = "RAGDB_SUBMISSION_ERROR"
        node.failure_message = str(e)
        node.failure_retryable = False
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission error: {e}")
        return False
