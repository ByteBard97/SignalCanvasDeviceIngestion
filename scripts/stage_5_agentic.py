#!/usr/bin/env python3
"""Stage 5 Agentic Extraction Orchestrator — hybrid two-pass retrieval + validation.

Runs extract_specs_agentic.extract() over the calibration device set.
Outputs per-device JSON + trace files; prints aggregate table.
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
from stages.extract_specs_agentic import extract, ExtractionTrace  # noqa: E402

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

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Calibration device set
# ---------------------------------------------------------------------------
DEVICES: list[tuple[str, str, str, str]] = [
    ("yamaha-rio1608-d2", "Yamaha", "Rio1608-D2", "yamaha-rio1608-d2"),
    ("audinate-avio-ao2", "Audinate", "AVIO-AO2", "audinate-avio-ao2"),
    ("audinate-avio-ai2", "Audinate", "AVIO-AI2", "audinate-avio-ai2"),
    ("shure-ulxd4", "Shure", "ULXD4", "shure-ulxd4"),
    ("allen-heath-sq5", "Allen & Heath", "SQ-5", "allen-heath-sq5"),
    ("qsc-core-110f", "QSC", "Core 110f", "qsc-core-110f"),
]


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class DeviceResult:
    device_id: str
    manufacturer: str
    model: str
    corpus_id: str
    text: str
    trace: ExtractionTrace
    usage: UsageRecord
    estimate: int
    json_valid: bool
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Per-device processing
# ---------------------------------------------------------------------------


async def _process_one_device(
    sem: asyncio.Semaphore,
    http: httpx.AsyncClient,
    client: MoonshotClient,
    manifest: Manifest,
    device_id: str,
    manufacturer: str,
    model: str,
    corpus_id: str,
) -> DeviceResult:
    """Run hybrid extraction for one device."""
    async with sem:
        # Token estimate (prompt not built yet, skip for now; estimate after chunks)
        estimate = 0

        try:
            extracted, trace = await asyncio.wait_for(
                extract(manufacturer, model, corpus_id, http, client),
                timeout=PER_DEVICE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                trace=ExtractionTrace(
                    classification=trace.classification if "trace" in dir() else None,  # type: ignore[arg-type]
                ),
                usage=UsageRecord(
                    model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0
                ),
                estimate=estimate,
                json_valid=False,
                error="Extraction timeout",
            )
        except Exception as e:
            return DeviceResult(
                device_id=device_id,
                manufacturer=manufacturer,
                model=model,
                corpus_id=corpus_id,
                text="",
                trace=ExtractionTrace(
                    classification=trace.classification if "trace" in dir() else None,  # type: ignore[arg-type]
                ),
                usage=UsageRecord(
                    model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0
                ),
                estimate=estimate,
                json_valid=False,
                error=f"Extraction error: {e}",
            )

        # Serialize extracted dict
        text = json.dumps(extracted, indent=2, ensure_ascii=False)

        # Log usage
        usage = trace.usage or UsageRecord(
            model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0
        )
        manifest.log_usage(
            device_id=device_id,
            stage="stage_5_agentic",
            provider="moonshot",
            model=usage.model,
            prompt_tokens=usage.prompt_tokens,
            completion_tokens=usage.completion_tokens,
            total_tokens=usage.total_tokens,
            elapsed_ms=usage.elapsed_ms,
        )

        # Validate JSON shape
        json_valid = isinstance(extracted, dict) and "_parse_error" not in extracted

        # Save JSON
        out_dir = REPO_ROOT / "output" / "stage_5_agentic"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{device_id}.moonshot.json"
        out_path.write_text(text, encoding="utf-8")

        # Save trace
        trace_path = out_dir / f"{device_id}.trace.json"
        trace_dict = {
            "classification": {
                "class": trace.classification.class_,
                "confidence": trace.classification.confidence,
                "source": trace.classification.source,
            },
            "pass1_queries": trace.pass1_queries,
            "pass2_queries": trace.pass2_queries,
            "pass1_chunk_count": trace.pass1_chunk_count,
            "pass2_chunk_count": trace.pass2_chunk_count,
            "deduped_chunk_count": trace.deduped_chunk_count,
            "model_filtered_chunk_count": trace.model_filtered_chunk_count,
            "retries": trace.retries,
            "validator_misses": trace.validator_misses,
            "aliases_used": trace.aliases_used,
            "disambiguation_applied": trace.disambiguation_applied,
            "estimated_input_tokens": trace.estimated_input_tokens,
            "model_tier": trace.model_tier,
            "usage": {
                "model": usage.model,
                "prompt_tokens": usage.prompt_tokens,
                "completion_tokens": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
                "elapsed_ms": usage.elapsed_ms,
            },
        }
        trace_path.write_text(json.dumps(trace_dict, indent=2, ensure_ascii=False), encoding="utf-8")

        return DeviceResult(
            device_id=device_id,
            manufacturer=manufacturer,
            model=model,
            corpus_id=corpus_id,
            text=text,
            trace=trace,
            usage=usage,
            estimate=estimate,
            json_valid=json_valid,
            error=None,
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    db_path = REPO_ROOT / "output" / "ingestion.db"
    manifest = Manifest(db_path)

    print("=== Stage 5 Agentic Extraction ===")
    print(f"Devices to process: {len(DEVICES)}")
    for did, mfr, mdl, cid in DEVICES:
        print(f"  - {did} | {mfr} {mdl} (corpus={cid})")
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
        _process_one_device(sem, http, client, manifest, did, mfr, mdl, cid)
        for did, mfr, mdl, cid in DEVICES
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
    report_lines.append("=" * 110)
    report_lines.append("PER-DEVICE RESULTS")
    report_lines.append(
        f"{'device_id':<25} {'model':<20} {'class':<20} {'in_tok':>8} {'out_tok':>8} "
        f"{'elapsed_ms':>10} {'USD':>12} {'valid':>5} {'retries':>7} {'val_miss':>8}"
    )
    report_lines.append("-" * 110)

    total_in = 0
    total_out = 0
    total_usd = 0.0
    in_tokens_list: list[int] = []
    out_tokens_list: list[int] = []
    usd_list: list[float] = []
    valid_count = 0
    retries_total = 0
    val_miss_total = 0

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

        retries_total += r.trace.retries
        val_miss_total += len(r.trace.validator_misses)

        report_lines.append(
            f"{r.device_id:<25} {r.model:<20} {r.trace.classification.class_:<20} "
            f"{r.usage.prompt_tokens:>8} {r.usage.completion_tokens:>8} "
            f"{r.usage.elapsed_ms:>10} {_fmt_usd(usd):>12} "
            f"{'yes' if r.json_valid else 'no':>5} {r.trace.retries:>7} {len(r.trace.validator_misses):>8}"
        )

    report_lines.append("-" * 110)

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
    report_lines.append("=" * 110)
    report_lines.append(f"AGGREGATES ({len(DEVICES)} devices)")
    report_lines.append(f"  Total input tokens:    {total_in:>10,}")
    report_lines.append(f"  Total output tokens:   {total_out:>10,}")
    report_lines.append(f"  Total USD spent:       {_fmt_usd(total_usd):>12}")
    report_lines.append(f"  Mean input / device:   {mean_in:>10.1f}")
    report_lines.append(f"  Median input / device: {median_in:>10.1f}")
    report_lines.append(f"  Mean output / device:  {mean_out:>10.1f}")
    report_lines.append(f"  Median output / device:{median_out:>10.1f}")
    report_lines.append(f"  Mean USD / device:     {_fmt_usd(mean_usd):>12}")
    report_lines.append(f"  Median USD / device:   {_fmt_usd(median_usd):>12}")
    report_lines.append(f"  JSON valid count:      {valid_count}/{len(DEVICES)}")
    report_lines.append(f"  Total retries:         {retries_total}")
    report_lines.append(f"  Total validator misses:{val_miss_total}")

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
    report_lines.append("=" * 110)
    report_lines.append(f"PROJECTION ({FULL_DATASET_SIZE:,} devices, {tier} pricing)")
    report_lines.append(f"  Median input × {FULL_DATASET_SIZE}:  {proj_in:>12,} tokens")
    report_lines.append(f"  Median output × {FULL_DATASET_SIZE}: {proj_out:>12,} tokens")
    report_lines.append(f"  Projected USD:         {_fmt_usd(proj_usd):>12}")

    # Balance
    report_lines.append("")
    report_lines.append("=" * 110)
    report_lines.append("BALANCE")
    report_lines.append(f"  Before: {balance_before}")
    report_lines.append(f"  After:  {balance_after}")
    report_lines.append("=" * 110)

    full_report = "\n".join(report_lines)
    print(full_report)

    # Also write report to file
    report_path = REPO_ROOT / "output" / "stage_5_agentic_report.txt"
    report_path.write_text(full_report, encoding="utf-8")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
