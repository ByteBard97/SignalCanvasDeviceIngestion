"""Ingestion execution state - tracks current device, phase, and progress."""

from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from .manifest import IngestionNode


class IngestionState(BaseModel):
    """Runtime state of the ingestion pipeline."""

    current_phase: int  # 0 (3 devices), 1 (50), 2 (1500), 3 (remaining)
    current_device_id: Optional[str] = None
    current_stage: int = 1  # 1-7 (NOT_STARTED → VALIDATE_PATCH)
    current_node: Optional[IngestionNode] = None

    devices_processed: int = 0
    devices_completed: int = 0
    devices_failed: int = 0

    started_at: Optional[datetime] = None
    last_checkpoint: Optional[datetime] = None

    class Config:
        arbitrary_types_allowed = True

    def start_device(self, node: IngestionNode):
        """Mark a device as being processed."""
        self.current_device_id = node.device_id
        self.current_node = node
        self.current_stage = node.get_current_stage()
        self.started_at = datetime.now()
        if self.last_checkpoint is None:
            self.last_checkpoint = datetime.now()

    def advance_stage(self):
        """Move to the next stage for the current device."""
        self.current_stage = min(self.current_stage + 1, 8)

    def complete_device(self, node: IngestionNode):
        """Mark a device as completed."""
        self.current_device_id = None
        self.current_node = None
        self.devices_processed += 1
        if node.is_complete:
            self.devices_completed += 1
        if node.last_failure is not None:
            self.devices_failed += 1
        self.last_checkpoint = datetime.now()

    def checkpoint(self):
        """Record a checkpoint for resumability."""
        self.last_checkpoint = datetime.now()
