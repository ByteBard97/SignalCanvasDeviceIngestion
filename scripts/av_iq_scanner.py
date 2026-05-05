#!/usr/bin/env python3
"""Scan patchify devices against AV-iQ predictable datasheet URLs.

AV-iQ hosts datasheet PDFs at predictable URLs:
  https://cdn-docs.av-iq.com/dataSheet/<MODEL>.pdf
  https://cdn-docs.av-iq.com/dataSheet/<MODEL>_Datasheet.pdf

This script tests each device in the patchify dataset against these
patterns and records hits. Devices with confirmed AV-iQ URLs can skip
Stage 1 (PDF search) in the ingestion pipeline.

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/av_iq_scanner.py
"""

import asyncio
import json
import sys
from pathlib import Path

import httpx
import urllib3

urllib3.disable_warnings()

PATCHIFY_PATH = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
OUTPUT_PATH = Path("output/av_iq_matches.json")

BATCH_SIZE = 500
CONCURRENCY = 50
TIMEOUT = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.av-iq.com/",
}


def _url_patterns(name: str) -> list[str]:
    """Generate candidate URL path components for a device name."""
    patterns = [name]
    if " " in name:
        patterns.append(name.replace(" ", "-"))
        patterns.append(name.replace(" ", ""))
    return [f"{p}.pdf" for p in patterns] + [f"{p}_Datasheet.pdf" for p in patterns]


async def _check_device(client: httpx.AsyncClient, device: dict) -> dict | None:
    name = device.get("name", "").strip()
    if not name:
        return None

    for pattern in _url_patterns(name):
        url = f"https://cdn-docs.av-iq.com/dataSheet/{pattern}"
        try:
            resp = await client.head(url, follow_redirects=True, timeout=TIMEOUT)
            if resp.status_code == 200:
                return {
                    "manufacturer": device.get("manufacturer", ""),
                    "name": name,
                    "av_iq_url": url,
                    "pattern": pattern,
                    "size_bytes": resp.headers.get("content-length"),
                }
        except Exception:
            pass
    return None


async def _scan_batch(
    client: httpx.AsyncClient,
    devices: list[dict],
    batch_num: int,
    total_batches: int,
) -> list[dict]:
    tasks = [_check_device(client, d) for d in devices]
    results = []
    for coro in asyncio.as_completed(tasks):
        result = await coro
        if result:
            results.append(result)
    print(
        f"  Batch {batch_num}/{total_batches}: {len(results)}/{len(devices)} hits"
    )
    return results


async def main() -> int:
    if not PATCHIFY_PATH.exists():
        print(f"Error: patchify dataset not found at {PATCHIFY_PATH}")
        return 1

    with open(PATCHIFY_PATH) as f:
        devices = json.load(f)

    print(f"Scanning {len(devices)} devices against AV-iQ...")

    limits = httpx.Limits(max_connections=CONCURRENCY)
    all_results: list[dict] = []

    async with httpx.AsyncClient(limits=limits, verify=False) as client:
        total_batches = (len(devices) + BATCH_SIZE - 1) // BATCH_SIZE
        for i in range(total_batches):
            start = i * BATCH_SIZE
            end = min(start + BATCH_SIZE, len(devices))
            batch = devices[start:end]
            results = await _scan_batch(client, batch, i + 1, total_batches)
            all_results.extend(results)

    pct = len(all_results) / len(devices) * 100
    print(f"\nTotal: {len(all_results)}/{len(devices)} devices found = {pct:.1f}%")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    from datetime import datetime, timezone
    with open(OUTPUT_PATH, "w") as f:
        json.dump(
            {
                "scanned_at": datetime.now(timezone.utc).isoformat(),
                "total_devices": len(devices),
                "matches_found": len(all_results),
                "coverage_pct": round(pct, 2),
                "matches": all_results,
            },
            f,
            indent=2,
        )
    print(f"Results saved to {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
