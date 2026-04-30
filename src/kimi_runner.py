"""Async runner for the Kimi CLI."""

import asyncio
import json
import logging
import shutil
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Kimi CLI binary name
KIMI_BINARY = "kimi"


async def run_kimi(
    prompt: str,
    *,
    skills_dir: Path,
    work_dir: Path,
    timeout: float = 180.0,
) -> Optional[str]:
    """Invoke the Kimi CLI non-interactively and return its stdout.

    Args:
        prompt: The system/user prompt to pass via -p.
        skills_dir: Absolute path to the .claude/skills directory.
        work_dir: Absolute path to the repository root (for --add-dir).
        timeout: Maximum seconds to wait for Kimi to finish.

    Returns:
        Kimi's stdout as a string, or None on failure / timeout.
    """
    if not shutil.which(KIMI_BINARY):
        logger.error("Kimi CLI not found on PATH")
        return None

    cmd = [
        KIMI_BINARY,
        "-p",
        prompt,
        "--quiet",
        "--skills-dir",
        str(skills_dir),
        "--add-dir",
        str(work_dir),
        "--afk",
        "--max-steps-per-turn",
        "30",
    ]

    logger.debug("Running Kimi command: %s", " ".join(cmd))

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout_bytes, stderr_bytes = await asyncio.wait_for(
            proc.communicate(), timeout=timeout
        )
    except asyncio.TimeoutError:
        logger.error("Kimi subprocess timed out after %.0fs", timeout)
        try:
            proc.kill()
            await proc.wait()
        except ProcessLookupError:
            pass
        return None
    except Exception as exc:
        logger.error("Kimi subprocess failed: %s", exc)
        return None

    stdout = stdout_bytes.decode("utf-8", errors="replace")
    stderr = stderr_bytes.decode("utf-8", errors="replace")

    if stderr:
        logger.debug("Kimi stderr: %s", stderr)

    if proc.returncode != 0:
        logger.error(
            "Kimi exited with code %d. stderr: %s", proc.returncode, stderr
        )
        return None

    return stdout


def extract_json_block(text: str) -> Optional[str]:
    """Extract the largest balanced-brace JSON object from text.

    Tries a direct parse first, then falls back to a brace-balance scan.
    """
    text = text.strip()
    if not text:
        return None

    # Fast path: the whole text is JSON
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    # Scan for the largest balanced { ... } block
    best: Optional[str] = None
    i = 0
    while i < len(text):
        if text[i] == "{":
            depth = 0
            start = i
            for j in range(i, len(text)):
                if text[j] == "{":
                    depth += 1
                elif text[j] == "}":
                    depth -= 1
                    if depth == 0:
                        candidate = text[start : j + 1]
                        try:
                            json.loads(candidate)
                            if best is None or len(candidate) > len(best):
                                best = candidate
                        except json.JSONDecodeError:
                            pass
                        i = j
                        break
        i += 1

    return best
