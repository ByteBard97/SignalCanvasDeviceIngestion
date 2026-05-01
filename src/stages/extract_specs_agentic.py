"""Hybrid agentic Stage 5 extractor: classify → template queries → generic safety net → dedupe → extract → validate."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
import yaml

# Ensure src/ is on path when running standalone
import sys

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from moonshot_client import MoonshotClient, UsageRecord  # noqa: E402
from stages._ragscallion import search_ragscallion  # noqa: E402
from stages.classify_device import classify, Classification  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
EXTRACTION_MODEL = "moonshot-v1-8k"
RAGSCALLION_SEARCH_URL = "http://192.168.0.200:8086/search"
DEFAULT_PASS_LIMIT = 5
DEFAULT_GENERIC_LIMIT = 3
MAX_RETRIES = 1
PROMPT_CHAR_BUDGET = 6000
PER_DEVICE_TIMEOUT_SECONDS = 120
MODEL_FILTER_MIN_CHUNKS = 8
MAX_CHUNKS_PER_PROMPT = 20

# Required port categories per class (for validator)
REQUIRED_CATEGORIES: dict[str, list[str]] = {
    "dante_stagebox": ["dante_port"],
    "dante_adapter_input": ["dante_port", "analog_port"],
    "dante_adapter_output": ["dante_port", "analog_port"],
    "wireless_rx": ["analog_port", "antenna_port"],
    "mixing_console": ["analog_port"],
    "dsp_processor": ["analog_port", "network_port"],
    "generic": [],
}

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class ExtractionTrace:
    """Trace of the extraction process for observability."""

    classification: Classification
    pass1_queries: list[str] = field(default_factory=list)
    pass2_queries: list[str] = field(default_factory=list)
    pass1_chunk_count: int = 0
    pass2_chunk_count: int = 0
    deduped_chunk_count: int = 0
    model_filtered_chunk_count: int = 0
    effective_chunk_count: int = 0
    model_filter_floor_triggered: bool = False
    retries: int = 0
    validator_misses: list[str] = field(default_factory=list)
    usage: Optional[UsageRecord] = None


# ---------------------------------------------------------------------------
# Prompt builder (mirrors stage_5_experiment.py)
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Template loading
# ---------------------------------------------------------------------------


def _template_path(class_name: str) -> Path:
    here = Path(__file__).resolve().parent
    return here / "query_templates" / f"{class_name}.yaml"


def _load_template(class_name: str) -> list[dict]:
    """Load query template for a class. Falls back to generic if missing."""
    path = _template_path(class_name)
    if not path.exists():
        logger.warning(f"Template missing for {class_name}, falling back to generic")
        path = _template_path("generic")
    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh)
    return data.get("queries", [])


# ---------------------------------------------------------------------------
# Chunk utilities
# ---------------------------------------------------------------------------


def _chunk_key(chunk: dict) -> str:
    """Deterministic hash key for deduplication."""
    text = chunk.get("text", "")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:32]


def _dedupe_chunks(chunks: list[dict]) -> list[dict]:
    """Remove duplicate chunks by text hash, preserving order."""
    seen: set[str] = set()
    out: list[dict] = []
    for c in chunks:
        key = _chunk_key(c)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out


def _filter_by_model_token(chunks: list[dict], model: str) -> tuple[list[dict], bool]:
    """Prefer chunks containing the exact model token (case-insensitive).

    Returns (selected_chunks, floor_triggered).  If the filtered set drops
    below MODEL_FILTER_MIN_CHUNKS we fall back to the full deduped set so
    the LLM prompt isn't starved of context.
    """
    token = model.lower()
    filtered = [c for c in chunks if token in c.get("text", "").lower()]
    if not filtered:
        return chunks, False
    if len(filtered) < MODEL_FILTER_MIN_CHUNKS:
        logger.warning(
            f"Model filter for '{model}' produced {len(filtered)} chunks "
            f"(floor={MODEL_FILTER_MIN_CHUNKS}); falling back to full "
            f"deduped set of {len(chunks)} chunks."
        )
        return chunks, True
    return filtered, False


# ---------------------------------------------------------------------------
# RAG query runners
# ---------------------------------------------------------------------------


async def _run_queries(
    http: httpx.AsyncClient,
    corpus_id: str,
    queries: list[dict],
) -> list[dict]:
    """Run a list of query dicts concurrently and flatten results."""
    sem = asyncio.Semaphore(len(queries))  # all at once

    async def _one(q: dict) -> list[dict]:
        async with sem:
            try:
                return await search_ragscallion(
                    http,
                    corpus_id,
                    q["q"],
                    limit=q.get("limit", DEFAULT_PASS_LIMIT),
                )
            except Exception as exc:
                logger.warning(f"RAG search failed for query '{q['q']}': {exc}")
                return []

    tasks = [_one(q) for q in queries]
    nested = await asyncio.gather(*tasks)
    flat: list[dict] = []
    for results in nested:
        flat.extend(results)
    return flat


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def _has_port_category(extracted: dict, category: str) -> bool:
    """Check if extracted JSON has at least one port matching the category."""
    ports = extracted.get("signal_flow", {}).get("ports", [])
    if not isinstance(ports, list):
        return False
    for port in ports:
        if not isinstance(port, dict):
            continue
        name = (port.get("name") or "").lower()
        connector = (port.get("connector") or "").lower()
        attrs = port.get("attributes", [])
        if not isinstance(attrs, list):
            attrs = []
        attr_str = " ".join(str(a) for a in attrs).lower()
        combined = f"{name} {connector} {attr_str}"
        if category == "dante_port" and "dante" in combined:
            return True
        if category == "analog_port" and any(
            x in combined for x in ("xlr", "trs", "rca", "analog", "mic", "line")
        ):
            return True
        if category == "digital_port" and any(
            x in combined for x in ("aes3", "aes", "digital", "madi", "optical", "coaxial")
        ):
            return True
        if category == "antenna_port" and any(
            x in combined for x in ("antenna", "bnc", "sma", "diversity")
        ):
            return True
        if category == "network_port" and any(
            x in combined for x in ("ethernet", "rj45", "network", "tcp", "ip")
        ):
            return True
        if category == "usb_port" and "usb" in combined:
            return True
        if category == "control_port" and any(
            x in combined for x in ("gpio", "contact", "rs-232", "control")
        ):
            return True
    return False


def _validate_extraction(extracted: dict, class_name: str) -> list[str]:
    """Return list of missing required categories."""
    required = REQUIRED_CATEGORIES.get(class_name, [])
    misses: list[str] = []
    for cat in required:
        if not _has_port_category(extracted, cat):
            misses.append(cat)
    return misses


# ---------------------------------------------------------------------------
# Targeted re-query for a missing category
# ---------------------------------------------------------------------------


async def _requery_for_category(
    http: httpx.AsyncClient,
    corpus_id: str,
    category: str,
    manufacturer: str,
    model: str,
) -> list[dict]:
    """Run a targeted query for a missing port category."""
    query_map: dict[str, str] = {
        "dante_port": f"{manufacturer} {model} Dante network port RJ45",
        "analog_port": f"{manufacturer} {model} analog input output XLR connector",
        "digital_port": f"{manufacturer} {model} AES3 MADI digital input output",
        "antenna_port": f"{manufacturer} {model} antenna connector BNC diversity",
        "network_port": f"{manufacturer} {model} Ethernet network control port",
        "usb_port": f"{manufacturer} {model} USB port audio interface",
        "control_port": f"{manufacturer} {model} GPIO control port RS-232",
    }
    q = query_map.get(category, f"{manufacturer} {model} {category}")
    try:
        return await search_ragscallion(http, corpus_id, q, limit=DEFAULT_PASS_LIMIT)
    except Exception as exc:
        logger.warning(f"Re-query failed for category '{category}': {exc}")
        return []


# ---------------------------------------------------------------------------
# Main extraction entrypoint
# ---------------------------------------------------------------------------


async def extract(
    manufacturer: str,
    model: str,
    corpus_id: str,
    http: httpx.AsyncClient,
    moonshot: MoonshotClient,
) -> tuple[dict, ExtractionTrace]:
    """Hybrid two-pass extraction with validation and retry."""
    trace = ExtractionTrace(
        classification=Classification(class_="generic", confidence=0.0, source="unknown"),
    )

    # 1. Classify
    classification = await classify(manufacturer, model)
    trace.classification = classification

    # 2. Load templates
    pass1_queries = _load_template(classification.class_)
    pass2_queries = _load_template("generic")
    trace.pass1_queries = [q["q"] for q in pass1_queries]
    trace.pass2_queries = [q["q"] for q in pass2_queries]

    # 3. Pass 1: type-specific queries
    pass1_chunks = await _run_queries(http, corpus_id, pass1_queries)
    trace.pass1_chunk_count = len(pass1_chunks)

    # 4. Pass 2: generic safety-net queries
    pass2_chunks = await _run_queries(http, corpus_id, pass2_queries)
    trace.pass2_chunk_count = len(pass2_chunks)

    # 5. Dedupe union
    all_chunks = _dedupe_chunks(pass1_chunks + pass2_chunks)
    trace.deduped_chunk_count = len(all_chunks)

    # 6. Model-token filter with chunk-count floor
    filtered, floor_triggered = _filter_by_model_token(all_chunks, model)
    trace.model_filtered_chunk_count = len(filtered)
    trace.model_filter_floor_triggered = floor_triggered
    chunks_to_use = filtered if filtered and not floor_triggered else all_chunks

    # 6b. Hard chunk cap to stay inside the 8K model token limit
    if len(chunks_to_use) > MAX_CHUNKS_PER_PROMPT:
        logger.warning(
            f"Chunk count {len(chunks_to_use)} exceeds MAX_CHUNKS_PER_PROMPT "
            f"({MAX_CHUNKS_PER_PROMPT}) for {model}; truncating to first "
            f"{MAX_CHUNKS_PER_PROMPT} chunks."
        )
        chunks_to_use = chunks_to_use[:MAX_CHUNKS_PER_PROMPT]
    trace.effective_chunk_count = len(chunks_to_use)

    # 7. Build prompt & call Moonshot
    prompt = _build_prompt(manufacturer, model, chunks_to_use)
    text, usage = await moonshot.chat_completion(
        prompt,
        model=EXTRACTION_MODEL,
        response_format_json=True,
        temperature=0.0,
    )
    trace.usage = usage

    # 8. Parse JSON
    try:
        extracted: dict = json.loads(text)
    except json.JSONDecodeError as exc:
        logger.warning(f"JSON decode failed for {manufacturer} {model}: {exc}")
        extracted = {"_raw_text": text, "_parse_error": str(exc)}

    # 9. Validator
    misses = _validate_extraction(extracted, classification.class_)
    trace.validator_misses = misses

    # 10. Retry (max 1)
    if misses and trace.retries < MAX_RETRIES:
        trace.retries += 1
        # Re-query for the first missing category
        target = misses[0]
        extra_chunks = await _requery_for_category(
            http, corpus_id, target, manufacturer, model
        )
        if extra_chunks:
            all_chunks = _dedupe_chunks(all_chunks + extra_chunks)
            filtered, floor_triggered = _filter_by_model_token(all_chunks, model)
            trace.model_filter_floor_triggered = floor_triggered
            chunks_to_use = filtered if filtered and not floor_triggered else all_chunks
            if len(chunks_to_use) > MAX_CHUNKS_PER_PROMPT:
                chunks_to_use = chunks_to_use[:MAX_CHUNKS_PER_PROMPT]
            trace.model_filtered_chunk_count = len(filtered)
            trace.effective_chunk_count = len(chunks_to_use)

            prompt = _build_prompt(manufacturer, model, chunks_to_use)
            text, usage = await moonshot.chat_completion(
                prompt,
                model=EXTRACTION_MODEL,
                response_format_json=True,
                temperature=0.0,
            )
            trace.usage = usage
            try:
                extracted = json.loads(text)
            except json.JSONDecodeError as exc:
                logger.warning(f"JSON decode failed on retry for {manufacturer} {model}: {exc}")
                extracted = {"_raw_text": text, "_parse_error": str(exc)}

            misses = _validate_extraction(extracted, classification.class_)
            trace.validator_misses = misses

    return extracted, trace
