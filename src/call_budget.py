"""Per-device LLM call budget.

A hard ceiling on the number of LLM invocations (Kimi CLI, Haiku CLI, or
Moonshot HTTP) any single device may trigger within one pipeline run. This is
the authoritative defense against runaway retry loops: it is enforced at the
three LLM chokepoints (run_kimi / _run_claude / MoonshotClient.chat_completion)
and so survives bugs in any individual stage's retry gating.

In-memory and per-run by design — see
docs/superpowers/plans/2026-05-26-per-device-llm-call-budget.md for why a
persisted counter is intentionally not used.
"""

from __future__ import annotations

import logging
import os
import sys as _sys
import threading

# This module is reached under two import names because the codebase is loaded
# both as a package (`src.call_budget`, e.g. under pytest / `python -m
# src.runner`) and top-level (`call_budget`, via the stages' sys.path shim that
# loads moonshot_client bare). Without aliasing those would be two distinct
# module objects with two separate _counts maps — a budget split-brain where
# reset_all() clears only one. Self-alias both names to this single object so
# every chokepoint shares one count map. importlib.reload re-executes in place,
# so the test fixtures' reload-cap pattern keeps pointing at the same object.
_sys.modules.setdefault("call_budget", _sys.modules[__name__])
_sys.modules.setdefault("src.call_budget", _sys.modules[__name__])

logger = logging.getLogger(__name__)


class CallBudgetExceeded(RuntimeError):
    """Raised by chokepoints that cannot return a sentinel (e.g. chat_completion)."""


# Worst-case legit path is ~12-15 LLM calls (classify + SKU + find-PDF/HTML
# retries + extract + re-extract, with Haiku->Kimi fallbacks each doubling a
# step). 12 covers the common path while turning a runaway loop into a bounded
# loss. Override via env for tests / tuning.
MAX_LLM_CALLS_PER_DEVICE = int(os.environ.get("MAX_LLM_CALLS_PER_DEVICE", "12"))

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
