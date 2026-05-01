#!/usr/bin/env python3
"""Full-pipeline runner: Stage 5 → 6 → 7 end-to-end for calibration devices.

Runs extract_specs_agentic.extract() over the calibration device set,
normalizes each extraction, generates a PatchLang .patch file, validates
it, and writes a JSON report.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
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
from stages.generate_patch import generate_patch  # noqa: E402
from stages.normalize_specs import normalize_extraction  # noqa: E402
from stages.validate_patch import validate_patch  # noqa: E402

# ---------------------------------------------------------------------------
# Pricing constants (USD per million tokens) — Moonshot official pricing
# ---------------------------------------------------------------------------
PRICE_8K_INPUT_PER_MTOK = 0.20
PRICE_8K_OUTPUT_PER_MTOK = 2.00
PRICE_32K_INPUT_PER_MTOK = 1.00
PRICE_32K_OUTPUT_PER_MTOK = 3.00

# Threshold for choosing 8K vs 32K pricing tier (tokens)
CONTEXT_TIER_THRESHOLD = 7000

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
# Per-device pipeline result
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    device_id: str
    manufacturer: str
    model: str
    corpus_id: str

    # Stage 5
    stage5_ok: bool = False
    stage5_error: Optional[str] = None
    extraction_json_path: Optional[str] = None
    usage: UsageRecord = field(
        default_factory=lambda: UsageRecord(
            model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0
        )
    )

    # Stage 5→6 transition
    normalized_ok: bool = False
    normalized_error: Optional[str] = None

    # Stage 6
    stage6_ok: bool = False
    stage6_error: Optional[str] = None
    patch_source: str = ""

    # Stage 7
    stage7_ok: bool = False
    stage7_errors: list[str] = field(default_factory=list)

    # Output
    output_path: Optional[str] = None
    invalid_path: Optional[str] = None

    @property
    def usd_cost(self) -> float:
        return _usd_cost(self.usage.prompt_tokens, self.usage.completion_tokens)


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
) -> PipelineResult:
    """Run Stage 5 → 6 → 7 for one device."""
    result = PipelineResult(
        device_id=device_id,
        manufacturer=manufacturer,
        model=model,
        corpus_id=corpus_id,
    )

    async with sem:
        # ------------------------------------------------------------------
        # Stage 5: Extraction
        # ------------------------------------------------------------------
        try:
            extracted, trace = await asyncio.wait_for(
                extract(manufacturer, model, corpus_id, http, client),
                timeout=PER_DEVICE_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            result.stage5_error = "Extraction timeout"
            return result
        except Exception as e:
            result.stage5_error = f"Extraction error: {e}"
            return result

        # Validate JSON shape
        if not isinstance(extracted, dict) or "_parse_error" in extracted:
            result.stage5_error = "Invalid or unparseable extraction JSON"
            if isinstance(extracted, dict) and "_parse_error" in extracted:
                result.stage5_error += f": {extracted['_parse_error']}"
            return result

        result.stage5_ok = True
        result.usage = trace.usage or UsageRecord(
            model="", prompt_tokens=0, completion_tokens=0, total_tokens=0, elapsed_ms=0
        )

        # Log usage
        manifest.log_usage(
            device_id=device_id,
            stage="stage_5_agentic",
            provider="moonshot",
            model=result.usage.model,
            prompt_tokens=result.usage.prompt_tokens,
            completion_tokens=result.usage.completion_tokens,
            total_tokens=result.usage.total_tokens,
            elapsed_ms=result.usage.elapsed_ms,
        )

        # Save extraction JSON
        out_dir = REPO_ROOT / "output" / "stage_5_agentic"
        out_dir.mkdir(parents=True, exist_ok=True)
        extraction_path = out_dir / f"{device_id}.moonshot.json"
        extraction_path.write_text(
            json.dumps(extracted, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        result.extraction_json_path = str(extraction_path.relative_to(REPO_ROOT))

        # ------------------------------------------------------------------
        # Normalization
        # ------------------------------------------------------------------
        classification = trace.classification
        if classification is None or not getattr(classification, "class_", None):
            result.normalized_error = "Missing device classification"
            return result

        try:
            normalized = normalize_extraction(extracted, classification.class_)
        except Exception as e:
            result.normalized_error = f"Normalization error: {e}"
            return result

        result.normalized_ok = True

        # ------------------------------------------------------------------
        # Stage 6: Generate Patch
        # ------------------------------------------------------------------
        try:
            patch_source = generate_patch(normalized)
        except Exception as e:
            result.stage6_error = f"Patch generation error: {e}"
            return result

        result.stage6_ok = True
        result.patch_source = patch_source

        # ------------------------------------------------------------------
        # Stage 7: Validate Patch
        # ------------------------------------------------------------------
        try:
            is_valid, errors = validate_patch(patch_source)
        except Exception as e:
            result.stage7_errors = [f"Validation exception: {e}"]
            is_valid = False

        result.stage7_ok = is_valid
        result.stage7_errors = errors

        # ------------------------------------------------------------------
        # Write output
        # ------------------------------------------------------------------
        patches_dir = REPO_ROOT / "output" / "patches"
        patches_dir.mkdir(parents=True, exist_ok=True)

        if is_valid:
            patch_path = patches_dir / f"{device_id}.patch"
            patch_path.write_text(patch_source, encoding="utf-8")
            result.output_path = str(patch_path.relative_to(REPO_ROOT))
        else:
            invalid_path = patches_dir / f"{device_id}.patch.invalid"
            invalid_path.write_text(patch_source, encoding="utf-8")
            result.invalid_path = str(invalid_path.relative_to(REPO_ROOT))

        return result


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

    print("=== Pipeline: Stage 5 → 6 → 7 ===")
    print(f"Devices to process: {len(DEVICES)}")
    for did, mfr, mdl, cid in DEVICES:
        print(f"  - {did} | {mfr} {mdl} (corpus={cid})")
    print()

    client = MoonshotClient()
    http = httpx.AsyncClient(timeout=30.0)

    sem = asyncio.Semaphore(MAX_CONCURRENT_CALLS)
    tasks = [
        _process_one_device(sem, http, client, manifest, did, mfr, mdl, cid)
        for did, mfr, mdl, cid in DEVICES
    ]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    await client.close()
    await http.aclose()

    # -----------------------------------------------------------------------
    # Build summary table
    # -----------------------------------------------------------------------
    report_lines: list[str] = []
    report_lines.append("=" * 110)
    report_lines.append("PIPELINE RESULTS")
    report_lines.append(
        f"{'Device':<25} {'Stage5':>8} {'Normalized':>12} {'Stage6':>8} "
        f"{'Stage7':>8} {'Output'}"
    )
    report_lines.append("-" * 110)

    total_usd = 0.0
    valid_count = 0
    invalid_count = 0

    per_device_report: list[dict] = []

    for r in results:
        if isinstance(r, Exception):
            report_lines.append(f"EXCEPTION: {r}")
            continue

        total_usd += r.usd_cost

        stage5_str = "OK" if r.stage5_ok else (r.stage5_error or "FAILED")
        norm_str = "OK" if r.normalized_ok else (r.normalized_error or "FAILED")
        stage6_str = "OK" if r.stage6_ok else (r.stage6_error or "FAILED")
        stage7_str = "OK" if r.stage7_ok else "FAILED"
        output_str = r.output_path or r.invalid_path or "—"

        report_lines.append(
            f"{r.device_id:<25} {stage5_str:>8} {norm_str:>12} {stage6_str:>8} "
            f"{stage7_str:>8} {output_str}"
        )

        if r.stage7_ok:
            valid_count += 1
        elif r.stage6_ok:
            invalid_count += 1

        per_device_report.append({
            "device_id": r.device_id,
            "manufacturer": r.manufacturer,
            "model": r.model,
            "stage5_ok": r.stage5_ok,
            "stage5_error": r.stage5_error,
            "extraction_json_path": r.extraction_json_path,
            "normalized_ok": r.normalized_ok,
            "normalized_error": r.normalized_error,
            "stage6_ok": r.stage6_ok,
            "stage6_error": r.stage6_error,
            "stage7_ok": r.stage7_ok,
            "stage7_errors": r.stage7_errors,
            "output_path": r.output_path,
            "invalid_path": r.invalid_path,
            "usd_cost": r.usd_cost,
        })

    report_lines.append("-" * 110)
    report_lines.append("")
    report_lines.append(f"Valid patches:   {valid_count}")
    report_lines.append(f"Invalid patches: {invalid_count}")
    report_lines.append(f"Failed Stage 5:  {len(DEVICES) - valid_count - invalid_count}")
    report_lines.append(f"Total USD:       {_fmt_usd(total_usd)}")
    report_lines.append("=" * 110)

    full_report = "\n".join(report_lines)
    print(full_report)

    # -----------------------------------------------------------------------
    # Write JSON report
    # -----------------------------------------------------------------------
    report_json = {
        "devices": per_device_report,
        "aggregates": {
            "total_devices": len(DEVICES),
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "failed_stage5_count": len(DEVICES) - valid_count - invalid_count,
            "total_usd": total_usd,
        },
    }

    report_path = REPO_ROOT / "output" / "pipeline_report.json"
    report_path.write_text(
        json.dumps(report_json, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\nJSON report written to: {report_path.relative_to(REPO_ROOT)}")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
