from src.runner import _query_stage_0_nodes
from src.harness.manifest import Manifest, DeviceNode, QUEUE_0_INITIAL
from src.pipeline_stages import STAGE_FAILED


def _node(device_id, attempts):
    n = DeviceNode(device_id=device_id, manufacturer="m", model="x")
    n.queue = QUEUE_0_INITIAL
    n.stage_resolve_sku = STAGE_FAILED
    n.failure_attempts = attempts
    return n


def test_stage_0_query_excludes_devices_over_attempt_cap(tmp_path):
    manifest = Manifest(tmp_path / "m.db")
    manifest.persist(_node("under", 4))
    manifest.persist(_node("over", 5))
    ids = {n.device_id for n in _query_stage_0_nodes(manifest)}
    assert "under" in ids
    assert "over" not in ids
