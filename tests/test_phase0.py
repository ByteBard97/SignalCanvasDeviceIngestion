"""Phase 0 test harness - validate pipeline with 3 ground truth devices."""

import json
import logging
from pathlib import Path

import pytest

from src.config import settings
from src.harness import IngestionManifest, IngestionNode
from src.pipeline import DeviceIngestionPipeline

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def phase0_devices():
    """Load Phase 0 ground truth devices."""
    fixture_path = Path(__file__).parent / "fixtures" / "phase0_ground_truth.json"
    with open(fixture_path) as f:
        data = json.load(f)

    devices = []
    for device_data in data["devices"]:
        devices.append({
            "id": device_data["id"],
            "manufacturer": device_data["manufacturer"],
            "model": device_data["model"],
            "category": device_data.get("category", "converter"),
        })
    return devices


@pytest.fixture
def test_manifest(tmp_path):
    """Create a test manifest database."""
    db_path = tmp_path / "test_ingestion.db"
    return IngestionManifest(db_path)


def test_manifest_persistence(test_manifest):
    """Test that manifest can persist and retrieve nodes."""
    node = IngestionNode(
        device_id="test-device",
        manufacturer="TEST",
        model="Model123",
    )

    # Add node
    test_manifest.add_node(node)

    # Retrieve node
    retrieved = test_manifest.get_node("test-device")
    assert retrieved is not None
    assert retrieved.manufacturer == "TEST"
    assert retrieved.model == "Model123"


def test_manifest_stats(test_manifest, phase0_devices):
    """Test manifest statistics."""
    for device in phase0_devices:
        node = IngestionNode(
            device_id=device["id"],
            manufacturer=device["manufacturer"],
            model=device["model"],
        )
        test_manifest.add_node(node)

    stats = test_manifest.stats()
    assert stats["total_devices"] == 3
    assert stats["completed"] == 0
    assert stats["failed"] == 0


def test_pipeline_initialization(phase0_devices):
    """Test that pipeline initializes correctly."""
    pipeline = DeviceIngestionPipeline(phase=0)

    assert pipeline.phase == 0
    assert pipeline.manifest is not None
    assert pipeline.graph is not None
    assert pipeline.execution_state.current_phase == 0


def test_device_node_stage_tracking():
    """Test device node stage tracking."""
    node = IngestionNode(
        device_id="test-device",
        manufacturer="Yamaha",
        model="R08D",
    )

    # Check initial state
    assert node.get_current_stage() == 1
    assert node.stage_find_pdf.value == "NOT_STARTED"

    # Mark stages complete
    from src.harness.manifest import StageStatus
    node.stage_find_pdf = StageStatus.COMPLETED
    assert node.get_current_stage() == 2

    node.stage_download_pdf = StageStatus.COMPLETED
    assert node.get_current_stage() == 3


def test_phase0_fixture_structure(phase0_devices):
    """Test that Phase 0 devices are properly structured."""
    assert len(phase0_devices) == 3

    # Check expected devices
    ids = {d["id"] for d in phase0_devices}
    assert "yamaha-r08d" in ids
    assert "audinate-avio-ai2" in ids
    assert "yamaha-cl5" in ids


def test_ragscallion_connectivity():
    """Test that Ragscallion server is reachable."""
    import requests

    try:
        response = requests.get(
            f"http://{settings.ragscallion_host}:{settings.ragscallion_port}/health",
            timeout=5,
        )
        assert response.status_code == 200
        assert response.json().get("status") == "ok"
        logger.info("✓ Ragscallion server is reachable and healthy")
    except requests.ConnectionError as e:
        pytest.skip(f"Ragscallion server not reachable: {e}")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
