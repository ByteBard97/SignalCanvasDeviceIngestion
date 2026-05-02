"""Regression test: compare normalized Stage-5 extractions against manually curated ground truth.

Scores precision/recall per device for ports, connectors, channels, and bridges.
Fails if any device falls below the configured threshold.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest

from stages.normalize_specs import normalize_extraction, _canonicalize_name
from stages.classify_device import classify, _classify_by_rule

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_REPO_ROOT = Path(__file__).resolve().parent.parent
_GROUND_TRUTH_PATH = _REPO_ROOT / "tests" / "fixtures" / "ground_truth_specs.json"
_STAGE5_DIR = _REPO_ROOT / "output" / "stage_5_agentic"

# ---------------------------------------------------------------------------
# Thresholds
# ---------------------------------------------------------------------------

MIN_PORT_PRECISION = 0.70
MIN_PORT_RECALL = 0.70

# ---------------------------------------------------------------------------
# Scoring dataclass
# ---------------------------------------------------------------------------


@dataclass
class DeviceScore:
    device_id: str
    precision: float = 0.0
    recall: float = 0.0
    f1: float = 0.0
    connector_acc: float = 0.0
    channel_acc: float = 0.0
    bridge_precision: float = 0.0
    bridge_recall: float = 0.0
    matched: int = 0
    extra: int = 0
    missing: int = 0
    errors: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_ground_truth() -> dict[str, dict]:
    with open(_GROUND_TRUTH_PATH, encoding="utf-8") as fh:
        data = json.load(fh)
    return data.get("devices", {})


def _load_fixture(device_id: str) -> dict:
    path = _STAGE5_DIR / f"{device_id}.moonshot.json"
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _normalize_connector(conn: str | None) -> str | None:
    """Treat connector variants as equivalent."""
    if not conn:
        return None
    c = conn.strip().lower()
    # XLR variants
    if c.startswith("xlr"):
        return "xlr"
    # 1/4" / 6.35mm variants
    if c in ("1/4\" trs", '1/4" trs', "1/4 trs", "1/4\"", '1/4"', "6.35mm"):
        return "1/4 trs"
    # 3.5mm / MiniJack variants
    if c in ("mini jack", "minijack", "3.5mm"):
        return "3.5mm"
    # Euroblock / Phoenix
    if c in ("euroblock", "phoenix", "detachable terminal block"):
        return "euroblock"
    return c


def _normalize_direction(direction: str | None) -> str:
    """Canonicalize direction for matching."""
    if not direction:
        return ""
    d = direction.strip().lower()
    if d in ("input", "in"):
        return "input"
    if d in ("output", "out"):
        return "output"
    if d in ("input_output", "input/output", "i/o", "bidirectional", "io"):
        return "io"
    return d


def _canonicalize_port(port: dict) -> tuple[str, str, str | None]:
    """Return a canonical key for port matching."""
    name = _canonicalize_name(port.get("name") or "").strip().lower()
    direction = _normalize_direction(port.get("direction"))
    connector = _normalize_connector(port.get("connector"))
    return (name, direction, connector)


def _match_ports(
    extracted_ports: list[dict], truth_ports: list[dict]
) -> tuple[set[tuple], set[tuple], set[tuple]]:
    """Return (matched, extra, missing) sets of canonical port keys."""
    extracted_set = {_canonicalize_port(p) for p in extracted_ports}
    truth_set = {_canonicalize_port(p) for p in truth_ports}
    matched = extracted_set & truth_set
    extra = extracted_set - truth_set
    missing = truth_set - extracted_set
    return matched, extra, missing


def _score_ports(
    extracted_ports: list[dict], truth_ports: list[dict], matched: set[tuple]
) -> tuple[float, float, float]:
    """Score connector accuracy and channel accuracy on matched ports."""
    extracted_by_key = {_canonicalize_port(p): p for p in extracted_ports}
    truth_by_key = {_canonicalize_port(p): p for p in truth_ports}

    conn_ok = 0
    chan_ok = 0
    for key in matched:
        ep = extracted_by_key[key]
        tp = truth_by_key[key]

        # Connector match (truth may be None = "don't care")
        if tp.get("connector") is None or ep.get("connector") == tp.get("connector"):
            conn_ok += 1

        # Channel match (truth may be None = "don't care")
        if tp.get("channels") is None or ep.get("channels") == tp.get("channels"):
            chan_ok += 1

    total = len(matched) or 1
    return conn_ok / total, chan_ok / total


def _score_bridges(
    extracted_bridges: list[str], truth_bridges: list[str]
) -> tuple[float, float]:
    """Return (precision, recall) for bridges."""
    extracted_set = set(extracted_bridges)
    truth_set = set(truth_bridges)
    matched = extracted_set & truth_set
    precision = len(matched) / len(extracted_set) if extracted_set else 1.0
    recall = len(matched) / len(truth_set) if truth_set else 1.0
    return precision, recall


def _score_device(device_id: str, truth: dict, extracted: dict) -> DeviceScore:
    """Compare a normalized extraction against ground truth."""
    score = DeviceScore(device_id=device_id)

    truth_ports = truth.get("signal_flow", {}).get("ports", [])
    extracted_ports = extracted.get("signal_flow", {}).get("ports", [])

    matched, extra, missing = _match_ports(extracted_ports, truth_ports)
    score.matched = len(matched)
    score.extra = len(extra)
    score.missing = len(missing)

    total_extracted = len(extracted_ports) or 1
    total_truth = len(truth_ports) or 1

    score.precision = len(matched) / total_extracted
    score.recall = len(matched) / total_truth
    if score.precision + score.recall > 0:
        score.f1 = 2 * score.precision * score.recall / (score.precision + score.recall)

    if matched:
        score.connector_acc, score.channel_acc = _score_ports(
            extracted_ports, truth_ports, matched
        )

    truth_bridges = truth.get("signal_flow", {}).get("bridges", [])
    extracted_bridges = extracted.get("signal_flow", {}).get("bridges", [])
    score.bridge_precision, score.bridge_recall = _score_bridges(
        extracted_bridges, truth_bridges
    )

    if extra:
        score.errors.append(f"Extra ports: {sorted(extra)}")
    if missing:
        score.errors.append(f"Missing ports: {sorted(missing)}")

    return score


# ---------------------------------------------------------------------------
# Parametrized test
# ---------------------------------------------------------------------------

_GROUND_TRUTH = _load_ground_truth()
_DEVICE_IDS = sorted(_GROUND_TRUTH.keys())


@pytest.mark.parametrize("device_id", _DEVICE_IDS)
def test_device_against_ground_truth(device_id: str) -> None:
    """Load fixture, normalize, and score against ground truth."""
    truth = _GROUND_TRUTH[device_id]
    raw = _load_fixture(device_id)

    # We need the device class for normalization; use the classify rule table
    # synchronously since the rule table is pure Python.
    mfr = raw.get("device_metadata", {}).get("manufacturer", "")
    mdl = raw.get("device_metadata", {}).get("model_number", "")

    # Use rule-based classification synchronously when possible to avoid LLM call
    classification = _classify_by_rule(mfr, mdl)
    if classification is None:
        import asyncio
        import os
        from dotenv import load_dotenv
        load_dotenv()
        from moonshot_client import MoonshotClient
        client = MoonshotClient(api_key=os.environ.get("MOONSHOT_API_KEY"))
        classification = asyncio.run(classify(mfr, mdl, moonshot=client))
    normalized = normalize_extraction(raw, classification.class_)

    score = _score_device(device_id, truth, normalized)

    # Build a detailed assertion message
    lines = [
        f"\n{'=' * 60}",
        f"Device: {device_id}",
        f"  Port precision: {score.precision:.2f}  recall: {score.recall:.2f}  F1: {score.f1:.2f}",
        f"  Connector accuracy (matched): {score.connector_acc:.2f}",
        f"  Channel accuracy (matched):   {score.channel_acc:.2f}",
        f"  Bridge precision: {score.bridge_precision:.2f}  recall: {score.bridge_recall:.2f}",
        f"  Matched: {score.matched}  Extra: {score.extra}  Missing: {score.missing}",
    ]
    for err in score.errors:
        lines.append(f"  ERROR: {err}")
    lines.append("=" * 60)
    info = "\n".join(lines)

    # Print for visibility even when test passes
    print(info)

    assert score.precision >= MIN_PORT_PRECISION, (
        f"{device_id}: port precision {score.precision:.2f} < {MIN_PORT_PRECISION}\n{info}"
    )
    assert score.recall >= MIN_PORT_RECALL, (
        f"{device_id}: port recall {score.recall:.2f} < {MIN_PORT_RECALL}\n{info}"
    )


def test_ground_truth_coverage() -> None:
    """Ensure every calibration device has a ground-truth entry."""
    fixture_ids = {
        p.stem.replace(".moonshot", "") for p in _STAGE5_DIR.glob("*.moonshot.json") if "trace" not in p.name
    }
    truth_ids = set(_GROUND_TRUTH.keys())
    missing = fixture_ids - truth_ids
    assert not missing, f"Missing ground truth for: {sorted(missing)}"
