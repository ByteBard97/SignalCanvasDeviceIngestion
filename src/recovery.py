"""Self-healing recovery router for failed pipeline nodes."""

from datetime import datetime, timezone

from .harness.manifest import (
    DeviceNode,
    Manifest,
    FailureCategory,
    QUEUE_0_INITIAL,
)

import logging

logger = logging.getLogger(__name__)


async def recovery_router(node: DeviceNode, manifest: Manifest) -> bool:
    """Inspect failure state and reset node for automatic retry where possible.

    Returns True if node was reset and should be retried, False if it needs manual review.
    """
    if node.failure_attempts >= 3:
        return False

    category = node.failure_category

    if category == FailureCategory.RAGDB_COLLISION.value:
        node.failure_stage = None
        node.failure_category = None
        node.failure_message = None
        node.failure_retryable = True
        node.failure_at = None
        manifest.persist(node)
        logger.info(f"Device {node.device_id}: recovered from RAGDB_COLLISION")
        return True

    if category in (
        FailureCategory.RAGDB_INDEXING_FAILED.value,
        FailureCategory.RAGDB_SUBMISSION_ERROR.value,
    ):
        node.failure_stage = None
        node.failure_category = None
        node.failure_message = None
        node.failure_retryable = True
        node.failure_attempts = 0
        node.failure_at = None
        node.stage_convert_marker = 0
        node.stage_index_rag = 0
        node.ragscallion_job_id = None
        node.ragscallion_submitted_at = None
        node.queue = QUEUE_0_INITIAL
        manifest.persist(node)
        logger.info(f"Device {node.device_id}: recovered from {category}")
        return True

    if category in (
        FailureCategory.PDF_DOWNLOAD_FAILED.value,
        FailureCategory.PDF_INVALID.value,
    ):
        node.failure_stage = None
        node.failure_category = None
        node.failure_message = None
        node.failure_retryable = True
        node.failure_at = None
        manifest.persist(node)
        logger.info(f"Device {node.device_id}: recovered from {category}")
        return True

    return False
