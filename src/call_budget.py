"""Per-device LLM call budget.

A hard ceiling on the number of LLM subprocess spawns (Kimi or Haiku) any
single device may trigger within one pipeline run. This is the authoritative
defense against runaway retry loops: it is enforced at the subprocess
chokepoints (run_kimi / _run_claude) and so survives bugs in any individual
stage's retry gating.

In-memory and per-run by design — see
docs/superpowers/plans/2026-05-26-per-device-llm-call-budget.md for why a
persisted counter is intentionally not used.
"""

from __future__ import annotations

import logging
import os
import threading

logger = logging.getLogger(__name__)

# Legitimate worst-case device path is ~6 LLM spawns; 8 gives headroom while
# turning a runaway loop into a bounded 8-call loss. Override for tests / tuning.
MAX_LLM_CALLS_PER_DEVICE = int(os.environ.get("MAX_LLM_CALLS_PER_DEVICE", "8"))

_lock = threading.Lock()
_counts: dict[str, int] = {}


def try_consume(device_id: str | None) -> bool:
    """Record one LLM spawn for *device_id* and report whether it is allowed.

    Returns True and increments the count if the device is under budget.
    Returns False (without incrementing) if the device has hit its ceiling.
    A None device_id is uncounted and always allowed (ad-hoc / untracked calls).
    """
    if device_id is None:
        return True
    with _lock:
        used = _counts.get(device_id, 0)
        if used >= MAX_LLM_CALLS_PER_DEVICE:
            logger.error(
                "LLM call budget exceeded for device %s (%d/%d) — refusing spawn",
                device_id, used, MAX_LLM_CALLS_PER_DEVICE,
            )
            return False
        _counts[device_id] = used + 1
        return True


def get_count(device_id: str) -> int:
    """Return the number of LLM spawns recorded for *device_id* this run."""
    with _lock:
        return _counts.get(device_id, 0)


def reset_all() -> None:
    """Clear all per-device counts (start-of-run / test isolation)."""
    with _lock:
        _counts.clear()
