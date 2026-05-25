"""Async runner for the Kimi CLI."""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

# Kimi CLI binary name
KIMI_BINARY = "kimi"

# Admission control: prevent OOM from too many concurrent Kimi subprocesses.
# Each kimi CLI invocation can use 500MB-1GB resident; on a 24GB Mac running
# Ragscallion polling, Python pipeline, plus other tools, ~5-6 concurrent is
# the safe ceiling. Past sessions have seen SIGSEGV (-11) and OOM-kill (-9)
# when this is uncapped.
_MAX_CONCURRENT_KIMI = int(os.environ.get("MAX_CONCURRENT_KIMI", "5"))
_MIN_FREE_MEM_MB = int(os.environ.get("KIMI_MIN_FREE_MEM_MB", "2048"))
_MEM_WAIT_TIMEOUT_SECS = float(os.environ.get("KIMI_MEM_WAIT_TIMEOUT", "120"))
_kimi_semaphore: Optional[asyncio.Semaphore] = None


def _get_kimi_semaphore() -> asyncio.Semaphore:
    """Lazy-init the semaphore so it binds to the current event loop."""
    global _kimi_semaphore
    if _kimi_semaphore is None:
        _kimi_semaphore = asyncio.Semaphore(_MAX_CONCURRENT_KIMI)
    return _kimi_semaphore


def _free_memory_mb() -> int:
    """Return approximate usable free memory in MB (macOS vm_stat).

    Returns a huge value on parse failure so callers don't block when the
    check itself is broken.
    """
    try:
        result = subprocess.run(
            ["vm_stat"], capture_output=True, text=True, timeout=2
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return 999_999

    page_size = 16384
    free_pages = inactive_pages = spec_pages = 0
    for line in result.stdout.splitlines():
        if "page size of" in line:
            try:
                page_size = int(line.split("page size of")[1].split()[0])
            except (IndexError, ValueError):
                pass
        elif line.startswith("Pages free:"):
            free_pages = int(line.split()[-1].rstrip("."))
        elif line.startswith("Pages inactive:"):
            inactive_pages = int(line.split()[-1].rstrip("."))
        elif line.startswith("Pages speculative:"):
            spec_pages = int(line.split()[-1].rstrip("."))
    usable_bytes = (free_pages + inactive_pages + spec_pages) * page_size
    return usable_bytes // (1024 * 1024)


def _count_kimi_processes() -> tuple[int, int]:
    """Return (process_count, total_rss_mb) for currently running kimi CLI
    invocations on this machine. Used as a defensive cross-check on top of
    our own semaphore — other users of the Mac may also be running kimi."""
    try:
        result = subprocess.run(
            ["ps", "ax", "-o", "rss=,command="],
            capture_output=True, text=True, timeout=2,
        )
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        return 0, 0
    count = 0
    rss_kb = 0
    for line in result.stdout.splitlines():
        if "kimi-cli" in line or "/kimi " in line or line.rstrip().endswith("/kimi"):
            parts = line.strip().split(None, 1)
            if len(parts) == 2:
                try:
                    rss_kb += int(parts[0])
                    count += 1
                except ValueError:
                    pass
    return count, rss_kb // 1024


async def _wait_for_memory_headroom() -> bool:
    """Block until system has enough free RAM to safely spawn another Kimi.

    Returns False if we time out waiting (caller should treat as failure
    rather than spawning under memory pressure).
    """
    loop = asyncio.get_event_loop()
    deadline = loop.time() + _MEM_WAIT_TIMEOUT_SECS
    logged_wait = False
    while True:
        free_mb = _free_memory_mb()
        proc_count, proc_rss = _count_kimi_processes()
        # OK if either: plenty of free memory, OR only a few kimi procs running
        if free_mb >= _MIN_FREE_MEM_MB and proc_count <= _MAX_CONCURRENT_KIMI:
            return True
        if loop.time() >= deadline:
            logger.warning(
                "Kimi admission timeout: free=%dMB (need %d), "
                "kimi_procs=%d using %dMB",
                free_mb, _MIN_FREE_MEM_MB, proc_count, proc_rss,
            )
            return False
        if not logged_wait:
            logger.info(
                "Kimi admission: waiting for memory headroom "
                "(free=%dMB, procs=%d using %dMB)",
                free_mb, proc_count, proc_rss,
            )
            logged_wait = True
        await asyncio.sleep(3.0)


async def _kill_proc_group(proc: asyncio.subprocess.Process) -> None:
    """Kill the entire process group of a subprocess."""
    try:
        pgid = os.getpgid(proc.pid)
        os.killpg(pgid, signal.SIGKILL)
    except (ProcessLookupError, OSError):
        pass
    # Fallback: kill the process directly
    try:
        proc.kill()
    except (ProcessLookupError, OSError):
        pass
    # Wait with a short timeout
    try:
        await asyncio.wait_for(proc.wait(), timeout=5.0)
    except asyncio.TimeoutError:
        pass


async def run_kimi(
    prompt: str,
    *,
    skills_dir: Path,
    work_dir: Path,
    timeout: float = 180.0,
    max_steps: int = 30,
) -> Optional[str]:
    """Invoke the Kimi CLI non-interactively and return its stdout.

    Args:
        prompt: The system/user prompt to pass via -p.
        skills_dir: Absolute path to the .claude/skills directory.
        work_dir: Absolute path to the repository root (for --add-dir).
        timeout: Maximum seconds to wait for Kimi to finish.
        max_steps: Cap on agent loop iterations (--max-steps-per-turn).
            Use a small value (3-5) for narrow look-up tasks like Stage 1
            so Kimi fail-fasts instead of burning budget on exploration.

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
        str(max_steps),
    ]

    logger.debug("Running Kimi command: %s", " ".join(cmd))

    proc: Optional[asyncio.subprocess.Process] = None
    semaphore = _get_kimi_semaphore()
    try:
        async with semaphore:
            if not await _wait_for_memory_headroom():
                logger.error(
                    "Refusing to spawn Kimi: insufficient memory headroom"
                )
                return None
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
        logger.error("Kimi subprocess timed out after %.0fs", timeout)
        if proc is not None:
            await _kill_proc_group(proc)
        return None
    except Exception as exc:
        logger.error("Kimi subprocess failed: %s", exc)
        if proc is not None:
            await _kill_proc_group(proc)
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
