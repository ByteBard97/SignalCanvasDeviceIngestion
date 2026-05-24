"""Ensure the Ragscallion RAG server is running before pipeline work begins."""

import logging
import subprocess
import time

import httpx

from config import settings

logger = logging.getLogger(__name__)

RAG_STARTUP_TIMEOUT_SECS = 90
RAG_POLL_INTERVAL_SECS = 3
RAG_LOG_TAIL_AFTER_SECS = 30
RAG_HEALTH_TIMEOUT = 2.0

_RAG_PROJECT_DIR = "/home/geoff/projects/device-library-rag"


def _health_check(base_url: str) -> bool:
    try:
        resp = httpx.get(f"{base_url}/health", timeout=RAG_HEALTH_TIMEOUT)
        return resp.status_code == 200
    except Exception:
        return False


def _ssh_run(cmd: str) -> subprocess.CompletedProcess:
    target = f"{settings.ragscallion_ssh_user}@{settings.ragscallion_ssh_host}"
    return subprocess.run(
        ["ssh", "-o", "ConnectTimeout=5", "-o", "BatchMode=yes", target, cmd],
        capture_output=True,
        text=True,
    )


def _tail_log(lines: int) -> str:
    result = _ssh_run(f"cd {_RAG_PROJECT_DIR} && tail -n {lines} server.log")
    return result.stdout if result.returncode == 0 else ""


def ensure_ragscallion_running() -> None:
    """Raise RuntimeError if Ragscallion cannot be reached or started."""
    # Resolve discovery once — avoids re-scanning /24 subnet on every poll iteration
    base_url = settings.ragscallion_base_url()

    if _health_check(base_url):
        return

    logger.info("Ragscallion not responding; starting via SSH...")

    start_cmd = (
        f"cd {_RAG_PROJECT_DIR} && bash run-server.sh 8086 </dev/null >>server.log 2>&1 &"
    )
    ssh_result = _ssh_run(start_cmd)
    if ssh_result.returncode != 0:
        raise RuntimeError(
            f"Cannot reach {settings.ragscallion_ssh_host} via SSH — "
            "is the Linux box powered on and on the LAN?"
        )

    deadline = time.monotonic() + RAG_STARTUP_TIMEOUT_SECS
    log_warned = False

    while time.monotonic() < deadline:
        if _health_check(base_url):
            logger.info("Ragscallion is healthy.")
            return

        elapsed = RAG_STARTUP_TIMEOUT_SECS - (deadline - time.monotonic())
        if elapsed >= RAG_LOG_TAIL_AFTER_SECS and not log_warned:
            tail = _tail_log(20)
            if tail:
                logger.warning("Ragscallion slow to start. Last 20 log lines:\n%s", tail)
            log_warned = True

        time.sleep(RAG_POLL_INTERVAL_SECS)

    final_log = _tail_log(50)
    raise RuntimeError(
        f"Ragscallion did not start within {RAG_STARTUP_TIMEOUT_SECS}s. "
        f"Last log:\n{final_log}"
    )
