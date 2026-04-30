"""Integration test for Stage 5 Kimi-based spec extraction.

This test shells out to the real Kimi CLI and queries the live Ragscallion
server. It is marked as slow and skipped when Kimi is unavailable.
"""

import json
import shutil

import pytest

from src.pipeline_stages import _extract_specs_via_agent


pytestmark = pytest.mark.slow


@pytest.fixture
def has_kimi():
    return shutil.which("kimi") is not None


@pytest.mark.asyncio
@pytest.mark.skipif(shutil.which("kimi") is None, reason="Kimi CLI not on PATH")
async def test_extract_specs_via_agent_r08d():
    """End-to-end extraction for Yamaha R08D from the yamaha-phase0 corpus."""
    result = await _extract_specs_via_agent(
        manufacturer="YAMAHA",
        model="R08D",
        corpus_id="yamaha-phase0",
        rag_search=None,  # Kimi queries Ragscallion directly via curl
    )

    assert result is not None, "_extract_specs_via_agent returned None"
    assert isinstance(result, str), "Result should be a JSON string"
    assert len(result) > 0, "Result string is empty"

    parsed = json.loads(result)

    required_top_keys = {
        "device_metadata",
        "signal_flow",
        "power_specs",
        "physical_specs",
    }
    assert required_top_keys.issubset(set(parsed.keys())), (
        f"Missing top-level keys; got {set(parsed.keys())}"
    )

    # Minimal sanity checks on metadata
    meta = parsed["device_metadata"]
    assert meta.get("manufacturer", "").upper() == "YAMAHA"
    assert meta.get("model_number", "").upper() == "R08D"

    # Signal flow should be present
    assert isinstance(parsed["signal_flow"], dict)
    assert "ports" in parsed["signal_flow"]
    assert isinstance(parsed["signal_flow"]["ports"], list)

    # Confidence should be one of the allowed values
    assert parsed.get("extraction_confidence") in {"high", "medium", "low"}
