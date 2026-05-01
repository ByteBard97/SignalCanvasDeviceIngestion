"""LangGraph-based pipeline orchestrator for device ingestion."""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from langgraph.graph import StateGraph
from pydantic import BaseModel

from .config import settings
from .harness import (
    IngestionManifest,
    IngestionNode,
    IngestionState,
    FailureCategory,
    QUEUE_4_MANUAL_REVIEW,
    STAGE_FIND_PDF,
    STAGE_DOWNLOAD_PDF,
    STAGE_GENERATE_PATCH,
    STAGE_VALIDATE_PATCH,
)
from .pipeline_stages import (
    stage_3_4_submit_to_ragscallion,
    stage_5_extract_specs,
    process_stage_5_batch,
    STAGE_FAILED,
)
from .ragscallion_client import RagscallionClient

logger = logging.getLogger(__name__)


class PipelineState(BaseModel):
    """State graph for LangGraph pipeline."""

    manifest: IngestionManifest
    execution_state: IngestionState
    phase: int
    device_list: list[dict]  # [{manufacturer, model, category}, ...]
    errors: list[str] = []

    class Config:
        arbitrary_types_allowed = True


class DeviceIngestionPipeline:
    """Main pipeline orchestrator using LangGraph for checkpointing and state management."""

    def __init__(self, phase: int = 0):
        self.phase = phase
        self.manifest = IngestionManifest(settings.manifests_db)
        self.execution_state = IngestionState(current_phase=phase)
        self.ragscallion_client = RagscallionClient()
        self.graph = self._build_graph()

    def _build_graph(self) -> StateGraph:
        """Build the LangGraph state graph."""
        graph = StateGraph(PipelineState)

        # Define nodes
        graph.add_node("pick_next_device", self._pick_next_device)
        graph.add_node("stage_find_pdf", self._stage_find_pdf)
        graph.add_node("stage_download_pdf", self._stage_download_pdf)
        graph.add_node("stage_convert_marker", self._stage_convert_marker)
        graph.add_node("stage_index_rag", self._stage_index_rag)
        graph.add_node("stage_extract_specs", self._stage_extract_specs)
        graph.add_node("stage_generate_patch", self._stage_generate_patch)
        graph.add_node("stage_validate_patch", self._stage_validate_patch)
        graph.add_node("complete_device", self._complete_device)
        graph.add_node("report_metrics", self._report_metrics)

        # Set entry point
        graph.set_entry_point("pick_next_device")

        # Define edges (routing logic)
        graph.add_edge("pick_next_device", "stage_find_pdf")
        graph.add_edge("stage_find_pdf", "stage_download_pdf")
        graph.add_edge("stage_download_pdf", "stage_convert_marker")
        graph.add_edge("stage_convert_marker", "stage_index_rag")
        graph.add_edge("stage_index_rag", "stage_extract_specs")
        graph.add_edge("stage_extract_specs", "stage_generate_patch")
        graph.add_edge("stage_generate_patch", "stage_validate_patch")
        graph.add_edge("stage_validate_patch", "complete_device")

        # Conditional routing from complete_device
        def should_continue(state: PipelineState) -> str:
            if state.execution_state.current_device_id is None:
                return "report_metrics"  # No more devices
            return "pick_next_device"

        graph.add_conditional_edges("complete_device", should_continue)

        return graph.compile()

    def _pick_next_device(self, state: PipelineState) -> dict[str, Any]:
        """Pick the next device to process based on manifest state."""
        logger.info("Picking next device...")
        # Get incomplete devices from manifest
        all_nodes = self.manifest.get_all_nodes()
        incomplete = [n for n in all_nodes if not n.is_complete]

        if not incomplete:
            logger.info("No incomplete devices remaining")
            state.execution_state.current_device_id = None
            return {"execution_state": state.execution_state}

        # Pick first incomplete device
        next_node = incomplete[0]
        state.execution_state.start_device(next_node)
        logger.info(f"Processing {next_node.device_id}")
        return {"execution_state": state.execution_state}

    def _stage_find_pdf(self, state: PipelineState) -> dict[str, Any]:
        """Stage 1: Find PDF manual for device via web search.

        On failure: Record failure metadata and move to queue_4 (manual review).
        The user can then provide a different PDF URL for retry.

        Failure Categories:
        - PDF_NOT_FOUND: Web search returned no results (retryable: user provides URL)
        """
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 1] Finding PDF for {device_id}")

        # TODO: Implement web search + Haiku validation
        pdf_url = None
        try:
            # Placeholder: web search + validation would go here
            # pdf_url = await search_and_validate_pdf(node.manufacturer, node.model)
            pass
        except Exception as e:
            logger.error(f"Stage 1 search error for {device_id}: {e}")

        if not pdf_url:
            # No PDF found: record failure and move to manual review
            node.failure_stage = STAGE_FIND_PDF
            node.failure_category = FailureCategory.PDF_NOT_FOUND.value
            node.failure_message = f"No PDF found for {node.manufacturer} {node.model}"
            node.failure_retryable = True  # User can provide URL
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_find_pdf = STAGE_FAILED
            self.manifest.persist(node)
            logger.warning(f"Device {device_id} PDF not found, moved to manual review")
            return {"manifest": self.manifest, "execution_state": state.execution_state}

        # Success: store PDF URL and advance
        node.pdf_url = pdf_url
        node.stage_find_pdf = 2  # COMPLETED
        self.manifest.persist(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_download_pdf(self, state: PipelineState) -> dict[str, Any]:
        """Stage 2: Download and validate PDF file.

        On failure: Record failure metadata and move to queue_4 (manual review).
        User can retry download or provide alternative URL.

        Failure Categories:
        - PDF_DOWNLOAD_FAILED: Network error, timeout, or invalid HTTP response (retryable)
        - PDF_INVALID: Downloaded file is not a valid PDF (not retryable)
        """
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 2] Downloading PDF for {device_id}")

        pdf_path = None
        error_detail = None
        try:
            # TODO: Implement HTTP download + file validation
            # pdf_path = await download_pdf(node.pdf_url, timeout=30)
            pass
        except Exception as e:
            error_detail = str(e)
            logger.error(f"Stage 2 download error for {device_id}: {e}")

        if not pdf_path:
            # Download failed or file invalid
            if error_detail and "invalid" in error_detail.lower():
                # File is corrupt/invalid
                node.failure_category = FailureCategory.PDF_INVALID.value
                node.failure_message = f"Downloaded PDF is invalid: {error_detail}"
                node.failure_retryable = False
            else:
                # Network/timeout error
                node.failure_category = FailureCategory.PDF_DOWNLOAD_FAILED.value
                node.failure_message = f"Download failed: {error_detail or 'unknown error'}"
                node.failure_retryable = True

            node.failure_stage = STAGE_DOWNLOAD_PDF
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_download_pdf = STAGE_FAILED
            self.manifest.persist(node)
            logger.warning(f"Device {device_id} download failed, moved to manual review")
            return {"manifest": self.manifest, "execution_state": state.execution_state}

        # Success: store PDF path and advance
        node.pdf_path = pdf_path
        node.stage_download_pdf = 2  # COMPLETED
        self.manifest.persist(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_convert_marker(self, state: PipelineState) -> dict[str, Any]:
        """Stage 3: Convert PDF to Markdown via Marker."""
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 3] Converting PDF to Markdown for {device_id}")
        # TODO: Implement Marker subprocess integration

        self.manifest.add_node(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_index_rag(self, state: PipelineState) -> dict[str, Any]:
        """Stage 3-4: Submit PDF to Ragscallion for indexing (combined stages).

        This stage combines the Convert Marker (stage 3) and Index RAG (stage 4) operations:
        - Assumes PDF has been converted to Markdown (stage 2 output)
        - Submits PDF to Ragscallion with retry logic
        - Moves device to queue_2 for polling on success
        - Moves device to queue_4 on collision or transient failure
        """
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stages 3-4] Submitting to Ragscallion for {device_id}")

        # Run async stage function synchronously
        success = asyncio.run(
            stage_3_4_submit_to_ragscallion(
                node,
                self.ragscallion_client,
                self.manifest,
            )
        )

        if success:
            # Advance stage on success
            state.execution_state.advance_stage()
        else:
            # On failure (collision, unavailable), device goes to queue_4 for manual review
            logger.warning(f"Device {device_id} submission failed, moved to manual review queue")

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_extract_specs(self, state: PipelineState) -> dict[str, Any]:
        """Stage 5: Extract signal specs via Haiku agent + RAG search.

        Uses asyncio.Semaphore(5) to limit concurrent Haiku calls. Processes the
        current device via the async stage_5_extract_specs function.

        Note: This single-device wrapper maintains compatibility with the
        sequential pipeline. For batch processing of queue_3, use process_stage_5_batch.
        """
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 5] Extracting specs for {device_id}")

        # Run async extraction synchronously
        success = asyncio.run(
            stage_5_extract_specs(
                node,
                self.ragscallion_client,
                self.manifest,
            )
        )

        if success:
            # Advance stage on success
            state.execution_state.advance_stage()
        else:
            # On failure, device goes to queue_4 for manual review
            logger.warning(f"Device {device_id} extraction failed, moved to manual review queue")

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_generate_patch(self, state: PipelineState) -> dict[str, Any]:
        """Stage 6: Generate PatchLang template from extracted specs.

        On failure: Record failure metadata and move to queue_4 (manual review).
        User can retry generation with different parameters.

        Failure Categories:
        - PATCH_GENERATION_FAILED: Agent couldn't generate valid PatchLang (retryable)
        """
        import json

        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 6] Generating .patch for {device_id}")

        patch_source = None
        try:
            from stages.generate_patch import generate_patch

            specs = node.specs_json
            if isinstance(specs, str):
                specs = json.loads(specs)
            patch_source = generate_patch(specs)
        except Exception as e:
            logger.error(f"Stage 6 generation error for {device_id}: {e}")

        if not patch_source:
            # Generation failed: record failure and move to manual review
            node.failure_stage = STAGE_GENERATE_PATCH
            node.failure_category = FailureCategory.PATCH_GENERATION_FAILED.value
            node.failure_message = "Patch generation returned empty or failed"
            node.failure_retryable = True  # Can adjust generation params
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_generate_patch = STAGE_FAILED
            self.manifest.persist(node)
            logger.warning(f"Device {device_id} patch generation failed, moved to manual review")
            return {"manifest": self.manifest, "execution_state": state.execution_state}

        # Success: store patch source and advance
        node.patch_source = patch_source
        node.stage_generate_patch = 2  # COMPLETED
        self.manifest.persist(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_validate_patch(self, state: PipelineState) -> dict[str, Any]:
        """Stage 7: Validate generated .patch against PatchLang compiler.

        On failure: Record failure metadata and move to queue_4 (manual review).
        Generation logic needs fixing; not retryable without code changes.

        Failure Categories:
        - PATCH_VALIDATION_FAILED: PatchLang compiler error (not retryable)
        """
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 7] Validating .patch for {device_id}")

        is_valid = False
        validation_errors: list[str] = []
        try:
            from stages.validate_patch import validate_patch

            is_valid, validation_errors = validate_patch(node.patch_source)
        except Exception as e:
            validation_errors = [str(e)]
            logger.error(f"Stage 7 validation error for {device_id}: {e}")

        if not is_valid:
            # Validation failed: record failure and move to manual review
            error_msg = "; ".join(validation_errors) if validation_errors else "Patch validation failed"
            node.failure_stage = STAGE_VALIDATE_PATCH
            node.failure_category = FailureCategory.PATCH_VALIDATION_FAILED.value
            node.failure_message = f"Patch validation error: {error_msg[:200]}"
            node.failure_retryable = False  # Generation logic needs fixing
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_validate_patch = STAGE_FAILED
            self.manifest.persist(node)
            logger.warning(f"Device {device_id} patch validation failed, moved to manual review")
            return {"manifest": self.manifest, "execution_state": state.execution_state}

        # Success: mark complete
        node.stage_validate_patch = 2  # COMPLETED
        self.manifest.persist(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _complete_device(self, state: PipelineState) -> dict[str, Any]:
        """Mark device as complete and checkpoint."""
        if state.execution_state.current_node:
            state.execution_state.complete_device(state.execution_state.current_node)
            self.manifest.checkpoint()
            state.execution_state.checkpoint()
            logger.info(f"Completed {state.execution_state.current_device_id}")

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _report_metrics(self, state: PipelineState) -> dict[str, Any]:
        """Report final pipeline metrics."""
        stats = self.manifest.stats()
        logger.info(f"Pipeline metrics: {stats}")
        logger.info(f"Total devices processed: {state.execution_state.devices_processed}")
        logger.info(f"Devices completed: {state.execution_state.devices_completed}")
        logger.info(f"Devices failed: {state.execution_state.devices_failed}")

        return {"execution_state": state.execution_state}

    def run(self, device_list: list[dict]):
        """Execute the pipeline on a device list."""
        settings.ensure_output_dirs()

        # Initialize manifest with devices
        for device in device_list:
            node = IngestionNode(
                device_id=device["id"],
                manufacturer=device["manufacturer"],
                model=device["model"],
            )
            self.manifest.add_node(node)

        logger.info(f"Starting pipeline with {len(device_list)} devices")

        # Run the graph
        initial_state = PipelineState(
            manifest=self.manifest,
            execution_state=self.execution_state,
            phase=self.phase,
            device_list=device_list,
        )

        # Execute with checkpointing (LangGraph handles resumability)
        final_state = self.graph.invoke(
            initial_state,
            config={"thread_id": f"phase_{self.phase}"},
        )

        return final_state
