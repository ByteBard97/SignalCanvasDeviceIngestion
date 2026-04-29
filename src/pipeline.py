"""LangGraph-based pipeline orchestrator for device ingestion."""

import asyncio
import logging
from datetime import datetime
from typing import Any

from langgraph.graph import StateGraph
from pydantic import BaseModel

from .config import settings
from .harness import IngestionManifest, IngestionNode, IngestionState
from .pipeline_stages import (
    stage_3_4_submit_to_ragscallion,
    stage_5_extract_specs,
    process_stage_5_batch,
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
        """Stage 1: Find PDF manual for device via web search."""
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 1] Finding PDF for {device_id}")
        # TODO: Implement web search + Haiku validation

        node.stage_find_pdf = state.execution_state.current_stage
        self.manifest.add_node(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_download_pdf(self, state: PipelineState) -> dict[str, Any]:
        """Stage 2: Download and validate PDF file."""
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 2] Downloading PDF for {device_id}")
        # TODO: Implement HTTP download + file validation

        self.manifest.add_node(node)
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
        """Stage 6: Generate PatchLang template from extracted specs."""
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 6] Generating .patch for {device_id}")
        # TODO: Implement PatchBuilder integration

        self.manifest.add_node(node)
        state.execution_state.advance_stage()

        return {"manifest": self.manifest, "execution_state": state.execution_state}

    def _stage_validate_patch(self, state: PipelineState) -> dict[str, Any]:
        """Stage 7: Validate generated .patch against PatchLang compiler."""
        device_id = state.execution_state.current_device_id
        node = self.manifest.get_node(device_id)

        logger.info(f"[Stage 7] Validating .patch for {device_id}")
        # TODO: Implement patchlang_python.check()

        self.manifest.add_node(node)
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
