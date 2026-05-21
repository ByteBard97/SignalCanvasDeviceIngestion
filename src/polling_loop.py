"""Async polling loop for Ragscallion job status monitoring with failure tracking."""

import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from .harness.manifest import (
    Manifest,
    QUEUE_2_POLLING_RAGSCALLION,
    DOC_TYPE_SPEC_SHEET,
)
from .ragscallion_client import RagscallionClient
from .harness.manifest import _trash_local_file

# Constants (no magic numbers per ClaudeCodeRules)
POLL_INTERVAL_SECONDS = 3
CONSECUTIVE_FAILURE_WARN_THRESHOLD = 3
CONSECUTIVE_FAILURE_ERROR_THRESHOLD = 10
STALE_NODE_WARNING_MINUTES = 15

logger = logging.getLogger(__name__)


async def run_polling_loop(
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
    logger_instance: Optional[logging.Logger] = None,
) -> None:
    """Poll Ragscallion /jobs endpoint to monitor job completion.

    Polls every 3 seconds to check for ready or failed jobs.
    Updates manifest nodes based on Ragscallion responses.

    Implements race condition fix:
    - Captures last_check BEFORE request
    - Uses server_now from response for next poll (avoids clock skew)

    Implements failure tracking:
    - Logs warnings at 3 consecutive failures
    - Logs errors at 10 consecutive failures
    - Marks nodes suspicious if > 15 min in queue_2 without update

    Args:
        ragscallion_client: RagscallionClient instance for polling
        manifest: Manifest instance for node tracking
        logger_instance: Optional logger (uses module logger if not provided)

    Note:
        Runs in infinite loop; cancel via asyncio task cancellation.
        Does not raise on polling failures; logs and continues.
    """
    global logger
    if logger_instance:
        logger = logger_instance

    # Capture initial timestamp before first poll. If queue_2 has nodes
    # waiting on jobs submitted earlier, rewind `since` to the earliest
    # submission so the first poll catches anything that completed during
    # any pipeline-down gap. Without this, jobs that finished while we were
    # offline are silently lost — the node sits in queue_2 forever unless
    # someone re-submits.
    last_check = _initial_since(manifest)
    consecutive_failures = 0

    logger.info(f"Starting Ragscallion polling loop (initial since={last_check.isoformat()})")

    while True:
        try:
            # Request server time before poll starts (for timing diagnostics)
            request_start = datetime.now(timezone.utc)

            # Poll for completed or failed jobs
            response = await ragscallion_client.poll_jobs(
                since=last_check.isoformat(),
                status="ready,failed",
                limit=100,
            )

            # Extract server time from response (avoids clock skew)
            if response.get("server_now"):
                server_now_str = response["server_now"]
                # Handle both formats: "2026-04-29T10:30:45.123Z" and ISO with TZ
                if server_now_str.endswith('Z'):
                    server_now_str = server_now_str[:-1] + '+00:00'
                # Ragscallion may already append +00:00 to a Z-normalized string
                if server_now_str.endswith('+00:00+00:00'):
                    server_now_str = server_now_str[:-6]
                last_check = datetime.fromisoformat(server_now_str)
            else:
                # Fallback: use current time if server_now missing
                last_check = datetime.now(timezone.utc)

            # Reset failure counter on successful poll
            consecutive_failures = 0

            jobs = response.get("jobs", [])
            if jobs:
                logger.debug(f"Poll returned {len(jobs)} jobs")

            # Process each job result
            for job in jobs:
                corpus_id = job.get("corpus_id")
                if not corpus_id:
                    logger.warning(f"Job missing corpus_id: {job}")
                    continue

                # Retrieve node from manifest (corpus_id is source of truth)
                node = manifest.get_node_by_corpus_id(corpus_id)
                if not node:
                    logger.warning(f"Node not found for corpus_id={corpus_id}")
                    continue

                _process_job_result(job, node, manifest)

            # Check for stale nodes (> 15 min in queue_2 without status update)
            await _check_stale_nodes(manifest)

        except Exception as e:
            # Log polling error and continue (forgiving)
            consecutive_failures += 1
            logger.warning(
                f"Polling failed (attempt {consecutive_failures}): {type(e).__name__}: {e}"
            )

            # Log at threshold warnings
            if consecutive_failures == CONSECUTIVE_FAILURE_WARN_THRESHOLD:
                logger.warning(
                    f"Polling failed {CONSECUTIVE_FAILURE_WARN_THRESHOLD} times. "
                    f"Continuing to retry..."
                )

            if consecutive_failures == CONSECUTIVE_FAILURE_ERROR_THRESHOLD:
                logger.error(
                    f"Polling failed {CONSECUTIVE_FAILURE_ERROR_THRESHOLD} times. "
                    f"Ragscallion may be unavailable."
                )

            # Continue polling despite failures
            # Nodes in queue_2 will be processed on next successful poll

        # Sleep between polls
        await asyncio.sleep(POLL_INTERVAL_SECONDS)


def _initial_since(manifest: Manifest) -> datetime:
    """Earliest submitted_at among queue_2 nodes, or now() if queue_2 is empty.

    The polling endpoint filters jobs by `since` — events older than that are
    not returned. If a job completed during a pipeline-down gap, the next run
    must rewind `since` past the submission time to catch it.
    """
    queue_2 = manifest.list_by_queue(QUEUE_2_POLLING_RAGSCALLION)
    submissions: list[datetime] = []
    for node in queue_2:
        ts_str = getattr(node, "ragscallion_submitted_at", None)
        if not ts_str:
            continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            submissions.append(ts)
        except (ValueError, AttributeError):
            continue
    if not submissions:
        return datetime.now(timezone.utc)
    return min(submissions)


def _process_job_result(job: dict, node, manifest: Manifest) -> None:
    """Apply one ready/failed Ragscallion job to its document and (if spec_sheet)
    advance the node.

    Multi-doc semantics: every job result updates the matching device_documents
    row. Only the spec_sheet job advances the node's queue. Secondary docs
    continue indexing without gating extraction.
    """
    corpus_id = job.get("corpus_id")
    job_id = job.get("job_id")
    status = job.get("status")

    docs = manifest.list_documents(node.device_id)
    matching_doc = next(
        (d for d in docs if d.ragscallion_job_id == job_id), None
    )
    doc_type = matching_doc.doc_type if matching_doc else None
    is_spec_sheet = (
        doc_type == DOC_TYPE_SPEC_SHEET
        or (matching_doc is None and job_id == node.ragscallion_job_id)
    )

    if status == "ready":
        if matching_doc:
            _trash_local_file(
                matching_doc.local_path,
                f"Device {node.device_id} {matching_doc.doc_type} (ready)",
            )
            manifest.clear_document_local_path(matching_doc.id)
            if is_spec_sheet:
                manifest.clear_node_pdf_path(node.device_id)
                node.pdf_path = None
            manifest.mark_document_indexed(
                matching_doc.id, ragscallion_job_id=job_id
            )

        chunks_indexed = job.get("chunks_indexed", "?")
        logger.info(
            f"Job {job_id} ready "
            f"(corpus_id={corpus_id}, doc_type={doc_type or 'unknown'}): "
            f"{chunks_indexed} chunks indexed"
        )
        try:
            if isinstance(chunks_indexed, int) and chunks_indexed < 10:
                logger.warning(
                    f"Device {corpus_id} ({doc_type or 'unknown'}): "
                    f"only {chunks_indexed} chunks indexed — likely a "
                    f"marketing brochure, not a technical manual."
                )
        except Exception:
            pass

        if is_spec_sheet and node.queue == QUEUE_2_POLLING_RAGSCALLION:
            node.queue = 3
            node.ragscallion_completed_at = job.get("completed_at")
            node.stage_convert_marker = 2  # COMPLETED
            node.stage_index_rag = 2  # COMPLETED
            manifest.persist(node)

    elif status == "failed":
        error_msg = job.get("error", "Unknown error")
        if matching_doc:
            _trash_local_file(
                matching_doc.local_path,
                f"Device {node.device_id} {matching_doc.doc_type} (failed)",
            )
            manifest.clear_document_local_path(matching_doc.id)
            if is_spec_sheet:
                manifest.clear_node_pdf_path(node.device_id)
                node.pdf_path = None
        if is_spec_sheet:
            node.queue = 4
            node.stage_index_rag = 3  # STAGE_FAILED — must be set so retry filters can find it
            manifest.persist(node)
            manifest.add_failure(
                device_id=corpus_id,
                failure_category="RAGDB_INDEXING_FAILED",
                failure_message=error_msg,
                stage=3,  # STAGE_INDEX_RAG
                retryable=True,
            )
            logger.error(
                f"Job {job_id} (spec_sheet) failed "
                f"(corpus_id={corpus_id}): {error_msg}"
            )
        else:
            # Secondary doc failure: log and leave indexed_at NULL. Device is
            # not failed; spec_sheet status is what gates extraction.
            logger.warning(
                f"Job {job_id} ({doc_type or 'unknown'}) failed "
                f"(corpus_id={corpus_id}): {error_msg}"
            )


async def _check_stale_nodes(manifest: Manifest) -> None:
    """Check for nodes in queue_2 that have been there > 15 minutes.

    Marks suspicious nodes with marked_suspicious = True for operator review.

    Args:
        manifest: Manifest instance for node querying
    """
    nodes_in_queue_2 = manifest.list_by_queue(QUEUE_2_POLLING_RAGSCALLION)
    now = datetime.now(timezone.utc)
    stale_threshold = now - timedelta(minutes=STALE_NODE_WARNING_MINUTES)

    for node in nodes_in_queue_2:
        # Parse updated_at timestamp
        try:
            updated_at = datetime.fromisoformat(node.updated_at.replace('Z', '+00:00'))
            # Ensure aware datetime for comparison
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
        except (ValueError, AttributeError):
            logger.warning(f"Node {node.device_id} has invalid updated_at: {node.updated_at}")
            continue

        # Check if node is stale
        if updated_at < stale_threshold and not node.marked_suspicious:
            node.marked_suspicious = True
            manifest.persist(node)
            logger.warning(
                f"Node {node.device_id} (corpus_id={node.corpus_id}) "
                f"has been in queue_2 for > {STALE_NODE_WARNING_MINUTES} minutes. "
                f"Marked suspicious. Job: {node.ragscallion_job_id}"
            )
