"""Stage implementations for device ingestion pipeline.

Each stage is a pure async function that processes a device node and persists results.
Stages use constants for queue IDs and stage codes (no magic numbers).
"""

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .harness.manifest import (
    DeviceNode,
    Manifest,
    FailureCategory,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_3_READY_FOR_EXTRACTION,
    QUEUE_4_MANUAL_REVIEW,
    STAGE_INDEX_RAG,
    STAGE_EXTRACT_SPECS,
)
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

# Concurrency control for spec extraction
EXTRACTION_SEMAPHORE = asyncio.Semaphore(5)  # Max 5 concurrent Haiku calls

# Concurrency constants (no magic numbers)
MAX_CONCURRENT_EXTRACTIONS = 5
EXTRACTION_TIMEOUT_SECONDS = 180


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
        node.failure_category = FailureCategory.RAGDB_COLLISION.value
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
        node.failure_category = FailureCategory.RAGSCALLION_UNAVAILABLE.value
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
        node.failure_category = FailureCategory.RAGDB_SUBMISSION_ERROR.value
        node.failure_message = str(e)
        node.failure_retryable = False
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission error: {e}")
        return False


async def stage_5_extract_specs(
    node: DeviceNode,
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
) -> bool:
    """Extract device specs via Haiku agent with RAG search context.

    Uses semaphore to limit concurrent Haiku calls to 5 (I/O-bound, doesn't
    compete with GPU). Reduces extraction time from 30+ hours to ~10 hours
    for 7,000 devices.

    Input:
        node: Device node with corpus_id from stage 3-4
        ragscallion_client: RagscallionClient for RAG corpus search
        manifest: Manifest instance for persistence

    Output:
        True if specs successfully extracted and stored
        False if extraction failed and moved to queue_4 (retryable)

    Processing:
        - Acquire semaphore (max 5 concurrent)
        - Search Ragscallion corpus for device context
        - Call Haiku agent to extract specs from corpus hits
        - Store specs_json, mark stage_extract_specs=COMPLETED
        - On failure: move to queue_4 with EXTRACTION_FAILED

    Side Effects:
        - Updates node: specs_json, stage_extract_specs, queue (on failure)
        - Persists node to manifest
        - Logs extraction result

    Note:
        Anthropic rate limits (4000 RPM) are safe at 5 concurrent.
        If hitting 429s, reduce MAX_CONCURRENT_EXTRACTIONS.
    """
    async with EXTRACTION_SEMAPHORE:
        try:
            # Perform RAG search for device context
            # TODO: Implement RAG search and Haiku agent call
            # For now, placeholder for extraction logic
            spec_json = await _extract_specs_via_agent(
                manufacturer=node.manufacturer,
                model=node.model,
                corpus_id=node.corpus_id,
                rag_search=lambda q: ragscallion_client.search(q, corpus=node.corpus_id),
            )

            if not spec_json:
                # Extraction failed: mark for retry
                node.failure_stage = STAGE_EXTRACT_SPECS
                node.failure_category = FailureCategory.EXTRACTION_FAILED.value
                node.failure_message = "Agent couldn't extract specs from corpus"
                node.failure_retryable = True
                node.failure_attempts += 1
                node.failure_at = datetime.now(timezone.utc).isoformat()
                node.queue = QUEUE_4_MANUAL_REVIEW
                node.stage_extract_specs = STAGE_FAILED
                manifest.persist(node)
                logger.warning(f"Device {node.device_id} extraction failed")
                return False

            # Success: store specs and mark complete
            node.specs_json = spec_json
            node.stage_extract_specs = STAGE_COMPLETED
            manifest.persist(node)
            logger.info(f"Device {node.device_id} specs extracted successfully")
            return True

        except asyncio.TimeoutError:
            # Extraction timeout (per-device EXTRACTION_TIMEOUT_SECONDS)
            node.failure_stage = STAGE_EXTRACT_SPECS
            node.failure_category = FailureCategory.EXTRACTION_TIMEOUT.value
            node.failure_message = f"Extraction timeout after {EXTRACTION_TIMEOUT_SECONDS}s"
            node.failure_retryable = True
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_extract_specs = STAGE_FAILED
            manifest.persist(node)
            logger.error(f"Device {node.device_id} extraction timeout")
            return False

        except Exception as e:
            # Unexpected error: log and move to manual review
            node.failure_stage = STAGE_EXTRACT_SPECS
            node.failure_category = FailureCategory.EXTRACTION_FAILED.value
            node.failure_message = f"Unexpected error: {str(e)}"
            node.failure_retryable = False
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_extract_specs = STAGE_FAILED
            manifest.persist(node)
            logger.error(f"Device {node.device_id} extraction error: {e}")
            return False


async def _extract_specs_via_agent(
    manufacturer: str,
    model: str,
    corpus_id: str,
    rag_search,
) -> Optional[str]:
    """Extract device specs via Kimi CLI using RAG context.

    Builds a prompt with device metadata and Ragscallion coordinates, shells out
    to the Kimi CLI (with the device-extraction skill), and parses the resulting
    JSON from stdout.

    Args:
        manufacturer: Device manufacturer (e.g., "YAMAHA")
        model: Device model (e.g., "R08D")
        corpus_id: Ragscallion corpus ID for this device
        rag_search: Callable that searches Ragscallion corpus for context

    Returns:
        JSON string of extracted specs, or None if extraction failed.
    """
    from .kimi_runner import run_kimi, extract_json_block

    repo_root = Path(__file__).resolve().parent.parent
    skills_dir = repo_root / ".claude" / "skills"

    ragscallion_base_url = "http://192.168.0.200:8086"

    prompt = (
        f"You are the SignalCanvas device-extraction agent.\n"
        f"Use the device-extraction skill.\n"
        f"\n"
        f"Inputs:\n"
        f"- manufacturer: {manufacturer}\n"
        f"- model: {model}\n"
        f"- corpus_id: {corpus_id}\n"
        f"- ragscallion_base_url: {ragscallion_base_url}\n"
        f"\n"
        f"Task:\n"
        f"1. Query the Ragscallion corpus with targeted curl searches.\n"
        f"2. Extract a structured device template.\n"
        f"3. Emit ONLY valid JSON on stdout — no markdown, no explanations.\n"
    )

    stdout = await run_kimi(
        prompt,
        skills_dir=skills_dir,
        work_dir=repo_root,
        timeout=EXTRACTION_TIMEOUT_SECONDS,
    )
    if stdout is None:
        return None

    json_block = extract_json_block(stdout)
    return json_block


async def process_stage_5_batch(
    manifest: Manifest,
    ragscallion_client: RagscallionClient,
) -> dict[str, int]:
    """Process all nodes in queue_3 (ready for extraction) with semaphore limit.

    Batch processes all devices ready for stage 5 extraction in parallel,
    enforcing max 5 concurrent Haiku calls via semaphore.

    Input:
        manifest: Manifest instance with device nodes
        ragscallion_client: RagscallionClient for RAG searches

    Output:
        Dictionary with counts: {successful, failed, exceptions}

    Processing:
        - Get all nodes in queue_3 (ready to extract specs)
        - Create async tasks for all nodes
        - Run with asyncio.gather(..., return_exceptions=True)
        - Semaphore enforces max 5 concurrent extraction calls
        - Log final counts

    Side Effects:
        - Updates all processed nodes in manifest
        - Moves failed nodes to queue_4 (manual review)
        - Logs batch processing summary

    Note:
        At 5 concurrent, with ~20s per extraction:
        - 7,000 devices / 5 concurrent = 1,400 batches
        - 1,400 * 20s = 28,000 seconds = ~7.8 hours
        vs. 7,000 * 20s = 140,000 seconds = ~39 hours serial
    """
    # Get all nodes ready for extraction (queue_3)
    # TODO: Implement manifest.list_by_queue(3) or equivalent
    nodes = []  # Placeholder; will be populated from manifest

    if not nodes:
        logger.info("No nodes in queue_3 (ready for extraction)")
        return {"successful": 0, "failed": 0, "exceptions": 0}

    logger.info(
        f"Processing {len(nodes)} nodes for spec extraction "
        f"(semaphore limit: {MAX_CONCURRENT_EXTRACTIONS})"
    )

    # Run all extractions in parallel with semaphore enforcing max 5 concurrent
    tasks = [
        stage_5_extract_specs(node, ragscallion_client, manifest)
        for node in nodes
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Count results
    successful = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if r is False)
    exceptions = sum(1 for r in results if isinstance(r, Exception))

    logger.info(
        f"Stage 5 batch complete: {successful} successful, "
        f"{failed} failed, {exceptions} exceptions"
    )

    return {
        "successful": successful,
        "failed": failed,
        "exceptions": exceptions,
    }
