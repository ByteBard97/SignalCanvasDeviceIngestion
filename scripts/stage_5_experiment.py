#!/usr/bin/env python3
"""
Stage 5 Non-Agentic Extraction Experiment — Measurement & A/B Comparison

This script is a ONE-OFF EXPERIMENT, NOT the production Stage 5.

Goal: Replace the agentic Kimi-CLI Stage 5 (which shells out to an agent that
performs its own RAG queries in a loop) with a single-shot Moonshot API call
where we fetch RAG chunks ourselves, stuff them into one prompt, and capture
real token usage via response.usage.

Why this matters:
- The agentic loop hides intermediate token consumption inside the subprocess.
- We need hard numbers to project cost for the full 6,619-device dataset.
- This script runs on the 6 already-completed devices as a controlled A/B
  comparison against the existing specs_json (Kimi-CLI extraction).

Outputs:
- JSON files under output/stage_5_experiment/{device_id}.moonshot.json
- Usage rows in manifest.usage_log with stage="stage_5_experiment"
- Printed report: per-device tokens, latency, cost, validity, projection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from statistics import mean, median
from typing import Optional

import httpx

# Load .env before any imports that read env vars
from dotenv import load_dotenv

load_dotenv()

# Ensure src/ is on path so we can import project modules
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

from harness.manifest import Manifest  # noqa: E402
from moonshot_client import MoonshotClient, UsageRecord  # noqa: E402

# ---------------------------------------------------------------------------
# Pricing constants (USD per million tokens) — Moonshot official pricing
# ---------------------------------------------------------------------------
PRICE_8K_INPUT_PER_MTOK = 0.20
PRICE_8K_OUTPUT_PER_MTOK = 2.00
PRICE_32K_INPUT_PER_MTOK = 1.00
PRICE_32K_OUTPUT_PER_MTOK = 3.00

# Threshold for choosing 8K vs 32K pricing tier (tokens)
CONTEXT_TIER_THRESHOLD = 7000

# Full dataset size for projection
FULL_DATASET_SIZE = 6619

# Concurrency & timeout
MAX_CONCURRENT_CALLS = 3
PER_DEVICE_TIMEOUT_SECONDS = 120

# Ragscallion search endpoint
RAGSCALLION_SEARCH_URL = "http://localhost:8086/search"

# Fixed search queries (deterministic so runs are comparable)
SEARCH_QUERIES = [
    "power consumption voltage PoE",
    "physical dimensions weight rack",
    "input output ports connectors XLR Dante Ethernet",
    "signal flow internal routing bridge bus",
    "sample rate latency channels",
]
SEARCH_LIMIT = 3

logger = logging.getLogger(__name__)


@dataclass
class DeviceResult:
    device_id: str
    manufacturer: str
    model: str
    corpus_id: str
    text: str
    usage: UsageRecord
    estimate: int
    json_valid: bool
    error: Optional[str] = None


def _build_prompt(manufacturer: str, model: str, chunks: list[dict]) -> str:
    """Build the single-shot extraction prompt."""
    chunks_text = ""
    for chunk in chunks:
        source = chunk.get("source", "unknown")
        section = chunk.get("section", "unknown")
        text = chunk.get("text", "")
        chunks_text += f"[{source}, {section}]\n{text}\n\n"

    prompt = (
        f"You are the SignalCanvas device-extraction agent. Extract a structured "
        f"device template for {manufacturer} {model} from the chunks below.\n\n"
        f"Required output: JSON only, matching this schema (no commentary, no markdown):\n"
        f"{{\n"
        f'  "device_metadata": {{ "manufacturer", "model_number", "label", "device_type", "category" }},\n'
        f'  "signal_flow": {{ "ports": [{{"name","direction","connector","channels","attributes":[]}}], "bridges": ["from->to", ...] }},\n'
        f'  "power_specs": {{ "power_draw_w", "voltage", "thermal_btuh", "poe_budget_w", "poe_draw_w" }},\n'
        f'  "physical_specs": {{ "height_mm", "width_mm", "depth_mm", "weight_kg" }},\n'
        f'  "extraction_confidence": "high|medium|low",\n'
        f'  "notes": "..."\n'
        f"}}\n\n"
        f"Use null for unknown fields. Do not invent specs.\n\n"
        f"=== Chunks ===\n"
        f"{chunks_text}"
    )
    return prompt


async def _search_ragscallion(
    http: httpx.AsyncClient,
    corpus_id: str,
    query: str,
    limit: int = SEARCH_LIMIT,
) -> list[dict]:
    """Search Ragscallion and return a list of result dicts."""
    params = {
        "q": query,
        "corpus": corpus_id,
        "limit": limit,
    }
    resp = await http.get(RAGSCALLION_SEARCH_URL, params=params, timeout=30.0)
    resp.raise_for_status()
    body = resp.json()
    results = body.get("results", {}).get(corpus_id, [])
    return results


async def _fetch_chunks_for_device(
    http: httpx.AsyncClient,
    corpus_id: str,
) -> list[dict]:
    """Run all fixed queries, dedupe chunks, return ordered list."""
    seen: set[tuple[str, str, str]] = set()
    chunks: list[dict] = []

    for query in SEARCH_QUERIES:
        try:
            results = await _search_ragscallion(http, corpus_id, query)
        except Exception as e:
            logger.warning(f"RAG search failed for {corpus_id} query '{query}': {e}")
            continue

        for r in results:
            source = r.get("source", "")
            section = r.get("section", "")
            text = r.get("text", "")
            key = (source, section, text[:200])
            if key in seen:
                continue
            seen.add(key)
            chunks.append(r)

    return chunks


def _usd_cost(prompt_tokens: int, completion_tokens: int) -> float:
    """Compute USD cost using 8K or 32K pricing based on prompt size."""
    if prompt_tokens < CONTEXT_TIER_THRESHOLD:
        in_rate = PRICE_8K_INPUT_PER_MTOK
        out_rate = PRICE_8K_OUTPUT_PER_MTOK
    else:
        in_rate = PRICE_32K_INPUT_PER_MTOK
        out_rate = PRICE_32K_OUTPUT_PER_MTOK
    return (
        prompt_tokens * in_rate / 1_000_000
        + completion_tokens * out_rate / 1_000_000
    )


def _fmt_usd(cost: float) -> str:
    if cost < 0.01:
        return f"${cost:.6f}"
    return f"${cost:.4f}"


async def _process_one_device(
    sem: asyncio.Semaphore,
    http: httpx.AsyncClient,
    client: MoonshotClient,
    manifest: Manifest,
    node,
) -> DeviceResult:
    """Fetch chunks, build prompt, call Moonshot, log usage, save JSON."""
    device_id = node.device_id
    manufacturer = node.manufacturer
    model = node.model
    corpus_id = node.corpus_id or device_id

    async with sem:
        # 1. Fetch RAG chunks
        try:
            chunks = await asyncio.wait_for(
                _fetch_chunks_for_device(http, corpus_id),
                timeout=PER_DEVICE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                usage=UsageRecord(model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0),
                estimate=0,
                json_valid=False,
                error="RAG chunk fetch timeout",
            )
        except Exception as e:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                usage=UsageRecord(model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0),
                estimate=0,
                json_valid=False,
                error=f"RAG chunk fetch error: {e}",
            )

        # 2. Build prompt
        prompt = _build_prompt(manufacturer, model, chunks)

        # 3. Get free token estimate
        try:
            estimate = await client.estimate_tokens(prompt)
        except Exception as e:
            logger.warning(f"Token estimate failed for {device_id}: {e}")
            estimate = 0

        # 4. Call Moonshot chat completion
        try:
            text, usage = await asyncio.wait_for(
                client.chat_completion(prompt, response_format_json=True, temperature=0.0),
                timeout=PER_DEVICE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                usage=UsageRecord(model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0),
                estimate=estimate,
                json_valid=False,
                error="Moonshot chat completion timeout",
            )
        except Exception as e:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                usage=UsageRecord(model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0),
                estimate=estimate,
                json_valid=False,
                error=f"Moonshot chat completion error: {e}",
            )

        # 5. Log usage to manifest
        manifest.log_usage(
            device_id=device_id,
            stage="stage_5_experiment",
            provider="moonshot",
            model=usage.model,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
            elapsed_ms=usage.elapsed_ms,
        )

        # 6. Validate JSON
        json_valid = False
        try:
            parsed = json.loads(text)
            json_valid = isinstance(parsed, dict)
        except json.JSONDecodeError:
            json_valid = False

        # 7. Save JSON file
        out_dir = REPO_ROOT / "output" / "stage_5_experiment"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{device_id}.moonshot.json"
        out_path.write_text(text, encoding="utf-8")

        return DeviceResult(
            device_id=device_id,
            manufacturer=manufacturer,
            model=model,
            corpus_id=corpus_id,
            text=text,
            usage=usage,
            estimate=estimate,
            json_valid=json_valid,
            error=None,
        )


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    db_path = REPO_ROOT / "output" / "ingestion.db"
    manifest = Manifest(db_path)

    # Load the 6 completed devices
    # Manifest already opened the DB; proceed to query
    import sqlite3

    conn = sqlite3.connect(str(manifest.db_path))
    rows = conn.execute(
        "SELECT device_id, manufacturer, model, corpus_id, stage_extract_specs "
        "FROM device_nodes WHERE stage_extract_specs = 2"
    ).fetchall()
    conn.close()

    # Build lightweight node objects (just enough attrs for our use)
    class _Node:
        pass

    nodes = []
    for row in rows:
        n = _Node()
        n.device_id, n.manufacturer, n.model, n.corpus_id, n.stage_extract_specs = row
        nodes.append(n)

    print(f"=== Stage 5 Non-Agentic Experiment ===")
    print(f"Devices to process: {len(nodes)}")
    for n in nodes:
        print(f"  - {n.device_id} | {n.manufacturer} {n.model} (corpus={n.corpus_id})")
    print()

    client = MoonshotClient()
    http = httpx.AsyncClient(timeout=30.0)

    # Balance before
    try:
        balance_before = await client.get_balance()
    except Exception as e:
        logger.warning(f"Could not fetch balance before: {e}")
        balance_before = {}

    sem = asyncio.Semaphore(MAX_CONCURRENT_CALLS)
    tasks = [
        _process_one_device(sem, http, client, manifest, node)
        for node in nodes
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Balance after
    try:
        balance_after = await client.get_balance()
    except Exception as e:
        logger.warning(f"Could not fetch balance after: {e}")
        balance_after = {}

    await client.close()
    await http.aclose()

    # -----------------------------------------------------------------------
    # Build report
    # -----------------------------------------------------------------------
    report_lines: list[str] = []
    report_lines.append("=" * 90)
    report_lines.append("PER-DEVICE RESULTS")
    report_lines.append(
        f"{'device_id':<25} {'model':<20} {'in_tok':>8} {'out_tok':>8} {'elapsed_ms':>10} {'USD':>12} {'valid':>5} {'est_match':>9}"
    )
    report_lines.append("-" * 90)

    total_in = 0
    total_out = 0
    total_usd = 0.0
    in_tokens_list: list[int] = []
    out_tokens_list: list[int] = []
    usd_list: list[float] = []
    valid_count = 0
    estimate_match_count = 0

    for r in results:
        if isinstance(r, Exception):
            report_lines.append(f"EXCEPTION: {r}")
            continue

        if r.error:
            report_lines.append(
                f"{r.device_id:<25} {r.model:<20} ERROR: {r.error}"
            )
            continue

        usd = _usd_cost(r.usage.prompt_tokens, r.usage.completion_tokens)
        total_in += r.usage.prompt_tokens
        total_out += r.usage.completion_tokens
        total_usd += usd
        in_tokens_list.append(r.usage.prompt_tokens)
        out_tokens_list.append(r.usage.completion_tokens)
        usd_list.append(usd)

        if r.json_valid:
            valid_count += 1

        # estimate match: within 5% of actual prompt tokens
        if r.estimate > 0 and abs(r.estimate - r.usage.prompt_tokens) / max(r.usage.prompt_tokens, 1) <= 0.05:
            est_match = "yes"
            estimate_match_count += 1
        else:
            est_match = "no"

        report_lines.append(
            f"{r.device_id:<25} {r.model:<20} {r.usage.prompt_tokens:>8} {r.usage.completion_tokens:>8} "
            f"{r.usage.elapsed_ms:>10} {_fmt_usd(usd):>12} {'yes' if r.json_valid else 'no':>5} {est_match:>9}"
        )

    report_lines.append("-" * 90)

    # Aggregates
    if in_tokens_list:
        mean_in = mean(in_tokens_list)
        median_in = median(in_tokens_list)
        mean_out = mean(out_tokens_list)
        median_out = median(out_tokens_list)
        mean_usd = mean(usd_list)
        median_usd = median(usd_list)
    else:
        mean_in = median_in = mean_out = median_out = mean_usd = median_usd = 0.0

    report_lines.append("")
    report_lines.append("=" * 90)
    report_lines.append("AGGREGATES (6 devices)")
    report_lines.append(f"  Total input tokens:    {total_in:>10,}")
    report_lines.append(f"  Total output tokens:   {total_out:>10,}")
    report_lines.append(f"  Total USD spent:       {_fmt_usd(total_usd):>12}")
    report_lines.append(f"  Mean input / device:   {mean_in:>10.1f}")
    report_lines.append(f"  Median input / device: {median_in:>10.1f}")
    report_lines.append(f"  Mean output / device:  {mean_out:>10.1f}")
    report_lines.append(f"  Median output / device:{median_out:>10.1f}")
    report_lines.append(f"  Mean USD / device:     {_fmt_usd(mean_usd):>12}")
    report_lines.append(f"  Median USD / device:   {_fmt_usd(median_usd):>12}")
    report_lines.append(f"  JSON valid count:      {valid_count}/{len(nodes)}")
    report_lines.append(f"  Estimate match count:  {estimate_match_count}/{len(nodes)}")

    # Projection
    proj_in = int(median_in * FULL_DATASET_SIZE)
    proj_out = int(median_out * FULL_DATASET_SIZE)
    if median_in < CONTEXT_TIER_THRESHOLD:
        proj_usd = (
            proj_in * PRICE_8K_INPUT_PER_MTOK / 1_000_000
            + proj_out * PRICE_8K_OUTPUT_PER_MTOK / 1_000_000
        )
        tier = "8K"
    else:
        proj_usd = (
            proj_in * PRICE_32K_INPUT_PER_MTOK / 1_000_000
            + proj_out * PRICE_32K_OUTPUT_PER_MTOK / 1_000_000
        )
        tier = "32K"

    report_lines.append("")
    report_lines.append("=" * 90)
    report_lines.append(f"PROJECTION ({FULL_DATASET_SIZE:,} devices, {tier} pricing)")
    report_lines.append(f"  Median input × {FULL_DATASET_SIZE}:  {proj_in:>12,} tokens")
    report_lines.append(f"  Median output × {FULL_DATASET_SIZE}: {proj_out:>12,} tokens")
    report_lines.append(f"  Projected USD:         {_fmt_usd(proj_usd):>12}")

    # Balance
    report_lines.append("")
    report_lines.append("=" * 90)
    report_lines.append("BALANCE")
    report_lines.append(f"  Before: {balance_before}")
    report_lines.append(f"  After:  {balance_after}")
    report_lines.append("=" * 90)

    full_report = "\n".join(report_lines)
    print(full_report)

    # Also write report to file
    report_path = REPO_ROOT / "output" / "stage_5_experiment_report.txt"
    report_path.write_text(full_report, encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
