"""Async runner using Claude Code CLI (Haiku) with Kimi CLI fallback.

Routing strategy:
- Simple structured tasks (name correction, SKU lookup): Haiku first
- Agentic web search (PDF finding): Kimi first — Kimi's web search is better
- Spec extraction: routed by device complexity class
    simple classes (generic, speaker, camera, wireless_rx, ...): Haiku first
    complex classes (dante_stagebox, dsp_processor, mixer, ...): Kimi first
    re-extraction with critique (attempt 2+): always Kimi
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path
from typing import Optional

from . import call_budget
from .kimi_runner import extract_json_block, run_kimi, _kill_proc_group

logger = logging.getLogger(__name__)

CLAUDE_BINARY = "claude"
HAIKU_MODEL = "haiku"

# Classes where extraction is well-scoped: few ports, clear schema, no
# complex network topology. Haiku handles these reliably.
_HAIKU_CLASSES: frozenset[str] = frozenset({
    "generic",
    "speaker",
    "microphone",
    "headset",
    "camera",
    "display",
    "projector",
    "monitor",
    "recorder",
    "wireless_rx",
    "it_networking",
})

# Classes where extraction is genuinely hard: multi-network-port topology,
# many I/O channels, AoIP routing. Kimi's larger context wins here.
_KIMI_CLASSES: frozenset[str] = frozenset({
    "dante_stagebox",
    "dante_adapter_input",
    "dante_adapter_output",
    "dsp_processor",
    "mixing_console",
    "matrix",
    "intercom",
    "codec",
})


async def run_extraction(
    prompt: str,
    *,
    skills_dir: Path,
    work_dir: Path,
    timeout: float = 120.0,
    max_steps: int = 30,
    device_id: str | None = None,
) -> Optional[str]:
    """Haiku first, Kimi fallback. Use for simple structured tasks."""
    result = await _run_claude(
        prompt, skills_dir=skills_dir, work_dir=work_dir, timeout=timeout,
        device_id=device_id,
    )
    if result is not None:
        return result
    logger.warning("Claude Haiku failed — falling back to Kimi")
    return await run_kimi(
        prompt, skills_dir=skills_dir, work_dir=work_dir,
        timeout=timeout, max_steps=max_steps, device_id=device_id,
    )


async def run_extraction_routed(
    prompt: str,
    *,
    skills_dir: Path,
    work_dir: Path,
    device_class: str,
    has_critique: bool = False,
    timeout: float = 120.0,
    max_steps: int = 30,
    device_id: str | None = None,
) -> Optional[str]:
    """Route to Haiku or Kimi based on device class and whether this is a retry.

    - has_critique=True means attempt 2+: Haiku already failed, go straight to Kimi.
    - Complex device classes go straight to Kimi.
    - Simple classes try Haiku first.
    """
    use_kimi_direct = has_critique or device_class in _KIMI_CLASSES

    if use_kimi_direct:
        reason = "critique retry" if has_critique else f"complex class '{device_class}'"
        logger.info("Routing to Kimi directly (%s)", reason)
        return await run_kimi(
            prompt, skills_dir=skills_dir, work_dir=work_dir,
            timeout=timeout, max_steps=max_steps, device_id=device_id,
        )

    logger.info("Routing to Haiku first (class '%s')", device_class)
    result = await _run_claude(
        prompt, skills_dir=skills_dir, work_dir=work_dir, timeout=timeout,
        device_id=device_id,
    )
    if result is not None:
        return result
    logger.warning("Haiku failed for class '%s' — falling back to Kimi", device_class)
    return await run_kimi(
        prompt, skills_dir=skills_dir, work_dir=work_dir,
        timeout=timeout, max_steps=max_steps, device_id=device_id,
    )


async def _run_claude(
    prompt: str,
    *,
    skills_dir: Path,
    work_dir: Path,
    timeout: float,
    device_id: str | None = None,
) -> Optional[str]:
    """Invoke the Claude Code CLI non-interactively with Haiku."""
    if not call_budget.try_consume(device_id):
        return None
    if not shutil.which(CLAUDE_BINARY):
        logger.debug("Claude CLI not found on PATH")
        return None

    cmd = [
        CLAUDE_BINARY,
        "-p", prompt,
        "--model", HAIKU_MODEL,
        "--add-dir", str(work_dir),
        "--dangerously-skip-permissions",
        "--output-format", "text",
    ]

    logger.debug("Running Claude Haiku (--model %s)", HAIKU_MODEL)

    proc: Optional[asyncio.subprocess.Process] = None
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.error("Claude subprocess timed out after %.0fs", timeout)
        if proc is not None:
            await _kill_proc_group(proc)
        return None
    except Exception as exc:
        logger.error("Claude subprocess failed: %s", exc)
        if proc is not None:
            await _kill_proc_group(proc)
        return None

    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")

    if stderr:
        logger.debug("Claude stderr: %s", stderr[:200])

    if proc.returncode != 0:
        logger.error("Claude exited with code %d. stderr: %s", proc.returncode, stderr[:200])
        return None

    return stdout or None


__all__ = ["run_extraction", "run_extraction_routed", "extract_json_block"]
