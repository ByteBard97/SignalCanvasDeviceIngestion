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
from stages.normalize_specs import normalize_extraction  # noqa: E402
from stages.sku_aliases import get_registry, AliasEntry  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RAGSCALLION_SEARCH_URL = "http://192.168.0.200:8086/search"
DEFAULT_PASS_LIMIT = 5
DEFAULT_GENERIC_LIMIT = 3
MAX_RETRIES = 1
PROMPT_CHAR_BUDGET = 6000
PER_DEVICE_TIMEOUT_SECONDS = 120
MODEL_FILTER_MIN_CHUNKS = 8
MAX_CHUNKS_PER_PROMPT = 40
EXTRACTION_TEMPERATURE = 0.0
EXTRACTION_SEED = 42

# Auto-routing tiers, ordered cheapest → largest. Each entry maps a Moonshot
# model to the maximum input-token count we'll send under the OUTPUT_BUDGET.
TIER_INPUT_LIMITS = [
    ("moonshot-v1-8k",   7500),
    ("moonshot-v1-32k",  30000),
    ("moonshot-v1-128k", 120000),
]
OUTPUT_BUDGET_TOKENS = 700


def pick_tier(estimated_input_tokens: int) -> str:
    """Return the smallest Moonshot tier that fits this prompt with headroom."""
    needed = estimated_input_tokens + OUTPUT_BUDGET_TOKENS
    for model, limit in TIER_INPUT_LIMITS:
        if needed <= limit:
            return model
    raise RuntimeError(
        f"Prompt too large even for largest tier: {needed} tokens "
        f"(limits: {TIER_INPUT_LIMITS})"
    )

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
    aliases_used: list[str] = field(default_factory=list)
    disambiguation_applied: bool = False
    estimated_input_tokens: int = 0
    model_tier: str = ""


# ---------------------------------------------------------------------------
# Prompt builder (mirrors stage_5_experiment.py)
# ---------------------------------------------------------------------------


def _infer_connectors(extracted: dict) -> dict:
    """Fill null connectors based on port name keywords."""
    ports = extracted.get("signal_flow", {}).get("ports", [])
    if not isinstance(ports, list):
        return extracted

    mapping = [
        (["xlr"], "XLR"),
        (["trs", "1/4", '1/4"', "6.35mm", "quarter-inch", "phone jack"], "TRS"),
        (["rj45", "ethercon", "ethernet", "dante", "network", "lan", "voip", "gigabit"], "RJ45"),
        (["usb"], "USB"),
        (["sma"], "SMA"),
        (["bnc"], "BNC"),
        (["lemo"], "LEMO"),
        (["hdmi"], "HDMI"),
        (["rca", "phono"], "RCA"),
        (["euroblock", "phoenix", "terminal block", "combicon", "pluggable terminal"], "Euroblock"),
        (["rj11", "rj-11", "pots"], "RJ11"),
        (["3.5mm", "1/8", "minijack", "mini jack"], "3.5mm"),
        (["aes3", "aes"], "XLR"),
        (["madi"], "BNC"),
        (["gpi", "gpo", "gpio"], "Euroblock"),
    ]

    for port in ports:
        if not isinstance(port, dict):
            continue
        connector = port.get("connector")
        if connector is not None and connector != "":
            continue
        name = (port.get("name") or "").lower()
        for keywords, ctype in mapping:
            if any(kw in name for kw in keywords):
                port["connector"] = ctype
                break

    return extracted


def _build_peripheral_prompt(
    manufacturer: str,
    model: str,
    chunks: list[dict],
    existing_ports: list[dict],
) -> str:
    """Build a tiny focused prompt for extracting peripheral ports only."""
    chunks_text = ""
    for chunk in chunks:
        source = chunk.get("source", "unknown")
        section = chunk.get("section", "unknown")
        text = chunk.get("text", "")
        chunks_text += f"[{source}, {section}]\n{text}\n\n"

    existing_names = [p.get("name", "") for p in existing_ports if isinstance(p, dict)]
    existing_names_str = ", ".join(f'"{n}"' for n in existing_names) if existing_names else "none"

    prompt = (
        f"You are the SignalCanvas device-extraction agent. Extract ONLY peripheral "
        f"ports for {manufacturer} {model} from the chunks below.\n\n"
        f"Peripheral ports are small utility connectors such as: LAN/Ethernet, USB-B, "
        f"USB-A/SQ-Drive, footswitch/foot-pedal, GPIO, or similar.\n\n"
        f"Ports ALREADY extracted (do NOT duplicate these): {existing_names_str}\n\n"
        f"Return JSON ONLY, no commentary, no markdown:\n"
        f'{{ "ports": [{{"name","direction","connector","channels","attributes":[]}}] }}\n'
        f"Use null for unknown fields. Do not invent specs. If no new peripheral ports "
        f"are found, return {{\"ports\":[]}}.\n\n"
        f"=== Chunks ===\n"
        f"{chunks_text}"
    )
    return prompt


def _build_prompt(
    manufacturer: str,
    model: str,
    chunks: list[dict],
    disambiguation: Optional[str] = None,
) -> str:
    """Build the single-shot extraction prompt."""
    chunks_text = ""
    for chunk in chunks:
        source = chunk.get("source", "unknown")
        section = chunk.get("section", "unknown")
        text = chunk.get("text", "")
        chunks_text += f"[{source}, {section}]\n{text}\n\n"

    disambiguation_section = ""
    if disambiguation:
        disambiguation_section = (
            f"DEVICE DISAMBIGUATION (READ CAREFULLY):\n"
            f"{disambiguation}\n\n"
        )

    prompt = (
        f"You are the SignalCanvas device-extraction agent. Extract a structured "
        f"device template for {manufacturer} {model} from the chunks below.\n\n"
        f"{disambiguation_section}"
        f"Required output: JSON only, matching this schema (no commentary, no markdown):\n"
        f"{{\n"
        f'  "device_metadata": {{ "manufacturer", "model_number", "label", "device_type", "category" }},\n'
        f'  "signal_flow": {{ "ports": [{{"name","direction","connector","channels","attributes":[]}}], "bridges": ["from->to", ...] }},\n'
        f'  "power_specs": {{ "power_draw_w", "voltage", "thermal_btuh", "poe_budget_w", "poe_draw_w" }},\n'
        f'  "physical_specs": {{ "height_mm", "width_mm", "depth_mm", "weight_kg" }},\n'
        f'  "extraction_confidence": "high|medium|low",\n'
        f'  "notes": "..."\n'
        f"}}\n\n"
        f"Connector canonicalization rules: If a port uses XLR, TRS, RJ45, USB, SMA, BNC, LEMO, HDMI, RCA, or Euroblock connectors, you MUST specify the connector type. Do not leave connector as null when the connector type is obvious from the port name or surrounding context.\n\n"
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


def _filter_by_model_token(
    chunks: list[dict],
    model: str,
    aliases: Optional[list[str]] = None,
) -> tuple[list[dict], bool]:
    """Prefer chunks containing the exact model token or any alias (case-insensitive).

    Returns (selected_chunks, floor_triggered).  If the filtered set drops
    below MODEL_FILTER_MIN_CHUNKS we fall back to the full deduped set so
    the LLM prompt isn't starved of context.
    """
    tokens = [model.lower()]
    if aliases:
        tokens.extend(a.lower() for a in aliases)

    def _matches(chunk: dict) -> bool:
        text = chunk.get("text", "").lower()
        return any(token in text for token in tokens)

    filtered = [c for c in chunks if _matches(c)]
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


async def _extract_peripheral_ports(
    http: httpx.AsyncClient,
    corpus_id: str,
    existing_ports: list[dict],
    manufacturer: str,
    model: str,
    moonshot: MoonshotClient,
) -> list[dict]:
    """Second-pass extraction focused on peripheral ports only.

    Queries Ragscallion for a small set of generic peripheral-port queries,
    builds a tiny focused prompt, and asks the LLM to extract only peripheral
    ports not already present in *existing_ports*.
    """
    peripheral_queries = [
        f"{manufacturer} {model} LAN Ethernet network RJ45 port connector",
        f"{manufacturer} {model} USB type B port connector computer audio interface",
        f"{manufacturer} {model} USB type A port connector storage recording playback",
        f"{manufacturer} {model} footswitch foot pedal jack connector",
    ]

    # Gather chunks (limit 2 per query to keep total small)
    all_chunks: list[dict] = []
    for q in peripheral_queries:
        try:
            results = await search_ragscallion(http, corpus_id, q, limit=2)
            all_chunks.extend(results)
        except Exception as exc:
            logger.warning(f"Peripheral RAG search failed for query '{q}': {exc}")

    if not all_chunks:
        return []

    # Deduplicate and cap at 5 chunks
    chunks = _dedupe_chunks(all_chunks)[:5]

    prompt = _build_peripheral_prompt(manufacturer, model, chunks, existing_ports)
    estimated = await moonshot.estimate_tokens(prompt)
    tier = pick_tier(estimated)
    logger.info(
        f"{manufacturer} {model} (peripheral pass): estimated {estimated} tokens → tier {tier}"
    )

    try:
        text, _usage = await moonshot.chat_completion(
            prompt,
            model=tier,
            response_format_json=True,
            temperature=EXTRACTION_TEMPERATURE,
            seed=EXTRACTION_SEED,
        )
    except Exception as exc:
        logger.warning(f"Peripheral pass LLM call failed for {manufacturer} {model}: {exc}")
        return []

    try:
        parsed: dict = json.loads(text)
        new_ports = parsed.get("ports", [])
        if not isinstance(new_ports, list):
            return []
    except json.JSONDecodeError as exc:
        logger.warning(f"Peripheral pass JSON decode failed for {manufacturer} {model}: {exc}")
        return []

    # Canonicalize messy connector strings from the LLM
    _CONNECTOR_CANON: list[tuple[list[str], str]] = [
        (["lan ethernet", "ethernet", "network", "rj45"], "RJ45"),
        (["usb-b", "usb type b", "usb_b"], "USB-B"),
        (["usb-a", "usb type a", "usb_a", "sq-drive"], "USB-A"),
        (["foot", "pedal", "footswitch"], "TRS"),
        (["xlr"], "XLR"),
        (["trs", "1/4"], "TRS"),
        (["usb"], "USB"),
        (["gpio"], "Euroblock"),
    ]

    def _canonicalize_connector(conn: str | None, pname: str) -> str | None:
        if not conn:
            return None
        c = conn.lower()
        for keywords, canonical in _CONNECTOR_CANON:
            if any(kw in c for kw in keywords):
                return canonical
        # Fallback: infer from port name
        pl = pname.lower()
        for keywords, canonical in _CONNECTOR_CANON:
            if any(kw in pl for kw in keywords):
                return canonical
        return conn

    # Filter out duplicates and known false positives
    existing_names = {p.get("name", "").lower() for p in existing_ports if isinstance(p, dict)}
    FALSE_POSITIVES = {"midi", "midi over usb", "midi i/o", "bluetooth", "wifi", "wi-fi"}
    merged: list[dict] = []
    for port in new_ports:
        if not isinstance(port, dict):
            continue
        name = port.get("name", "")
        if name.lower() in existing_names:
            continue
        if name.lower() in FALSE_POSITIVES:
            continue
        # Canonicalize connector regardless of whether it's null or messy
        port["connector"] = _canonicalize_connector(port.get("connector"), name)
        merged.append(port)

    logger.info(
        f"{manufacturer} {model} peripheral pass found {len(merged)} new port(s)"
    )
    return merged


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

    # 0. Alias lookup
    registry = get_registry()
    alias_entry = registry.lookup(manufacturer, model)
    aliases = alias_entry.aliases if alias_entry else None
    disambiguation = alias_entry.disambiguation if alias_entry else None
    if alias_entry:
        trace.aliases_used = list(alias_entry.aliases)
        trace.disambiguation_applied = True

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

    # 6. Model-token filter with chunk-count floor (alias-aware)
    filtered, floor_triggered = _filter_by_model_token(all_chunks, model, aliases)
    trace.model_filtered_chunk_count = len(filtered)
    trace.model_filter_floor_triggered = floor_triggered
    chunks_to_use = filtered if filtered and not floor_triggered else all_chunks

    # 6b. Priority-based chunk cap to stay inside the 8K model token limit.
    # Order: model-matching chunks > pass-1 (type-specific) > pass-2 (generic)
    # so peripheral pages (often surfaced by pass-2) aren't silently dropped.
    if len(chunks_to_use) > MAX_CHUNKS_PER_PROMPT:
        model_token = model.lower()
        alias_tokens = [a.lower() for a in aliases] if aliases else []
        pass1_keys = {_chunk_key(c) for c in pass1_chunks}

        def _priority(chunk: dict) -> int:
            key = _chunk_key(chunk)
            text = chunk.get("text", "").lower()
            if model_token in text or any(a in text for a in alias_tokens):
                return 0  # highest — keep all model-matching chunks
            if key in pass1_keys:
                return 1  # medium — pass-1 type-specific
            return 2  # lowest — pass-2 generic safety net

        chunks_to_use = sorted(chunks_to_use, key=_priority)
        dropped = len(chunks_to_use) - MAX_CHUNKS_PER_PROMPT
        logger.warning(
            f"Chunk count {len(chunks_to_use)} exceeds MAX_CHUNKS_PER_PROMPT "
            f"({MAX_CHUNKS_PER_PROMPT}) for {model}; dropping {dropped} "
            f"lowest-priority chunks."
        )
        chunks_to_use = chunks_to_use[:MAX_CHUNKS_PER_PROMPT]
    trace.effective_chunk_count = len(chunks_to_use)

    # 7. Stable chunk ordering before prompt build (eliminates retrieval-order noise)
    chunks_to_use = sorted(
        chunks_to_use,
        key=lambda c: (
            c.get("source", ""),
            c.get("page", ""),
            c.get("section", ""),
            c.get("chunk_id", ""),
        ),
    )

    # 8. Build prompt, estimate tokens, pick tier, call Moonshot
    prompt = _build_prompt(manufacturer, model, chunks_to_use, disambiguation)
    estimated = await moonshot.estimate_tokens(prompt)
    trace.estimated_input_tokens = estimated
    tier = pick_tier(estimated)
    trace.model_tier = tier
    logger.info(
        f"{manufacturer} {model}: estimated {estimated} tokens → tier {tier}"
    )
    text, usage = await moonshot.chat_completion(
        prompt,
        model=tier,
        response_format_json=True,
        temperature=EXTRACTION_TEMPERATURE,
        seed=EXTRACTION_SEED,
    )
    trace.usage = usage

    # 8. Parse JSON
    try:
        extracted: dict = json.loads(text)
        extracted = _infer_connectors(extracted)
        extracted = normalize_extraction(extracted, classification.class_)
    except json.JSONDecodeError as exc:
        logger.warning(f"JSON decode failed for {manufacturer} {model}: {exc}")
        extracted = {"_raw_text": text, "_parse_error": str(exc)}

    # 8b. Peripheral-port second pass (after normalization, before validation)
    if isinstance(extracted, dict) and "_parse_error" not in extracted:
        existing_ports = extracted.get("signal_flow", {}).get("ports", [])
        if not isinstance(existing_ports, list):
            existing_ports = []
        try:
            new_ports = await _extract_peripheral_ports(
                http, corpus_id, existing_ports, manufacturer, model, moonshot
            )
            if new_ports:
                extracted.setdefault("signal_flow", {}).setdefault("ports", []).extend(new_ports)
                # Re-infer connectors on the merged result before re-normalizing
                extracted = _infer_connectors(extracted)
                extracted = normalize_extraction(extracted, classification.class_)
        except Exception as exc:
            logger.warning(f"Peripheral pass failed for {manufacturer} {model}: {exc}")

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
            filtered, floor_triggered = _filter_by_model_token(all_chunks, model, aliases)
            trace.model_filter_floor_triggered = floor_triggered
            chunks_to_use = filtered if filtered and not floor_triggered else all_chunks
            if len(chunks_to_use) > MAX_CHUNKS_PER_PROMPT:
                model_token = model.lower()
                alias_tokens = [a.lower() for a in aliases] if aliases else []
                pass1_keys = {_chunk_key(c) for c in pass1_chunks}

                def _retry_priority(chunk: dict) -> int:
                    key = _chunk_key(chunk)
                    text = chunk.get("text", "").lower()
                    if model_token in text or any(a in text for a in alias_tokens):
                        return 0
                    if key in pass1_keys:
                        return 1
                    return 2

                chunks_to_use = sorted(chunks_to_use, key=_retry_priority)
                chunks_to_use = chunks_to_use[:MAX_CHUNKS_PER_PROMPT]
            trace.model_filtered_chunk_count = len(filtered)
            trace.effective_chunk_count = len(chunks_to_use)

            prompt = _build_prompt(manufacturer, model, chunks_to_use, disambiguation)
            estimated_retry = await moonshot.estimate_tokens(prompt)
            trace.estimated_input_tokens = estimated_retry
            tier_retry = pick_tier(estimated_retry)
            trace.model_tier = tier_retry
            logger.info(
                f"{manufacturer} {model} (retry): estimated {estimated_retry} tokens → tier {tier_retry}"
            )
            text, usage = await moonshot.chat_completion(
                prompt,
                model=tier_retry,
                response_format_json=True,
                temperature=EXTRACTION_TEMPERATURE,
                seed=EXTRACTION_SEED,
            )
            trace.usage = usage
            try:
                extracted = json.loads(text)
                extracted = _infer_connectors(extracted)
                extracted = normalize_extraction(extracted, classification.class_)
            except json.JSONDecodeError as exc:
                logger.warning(f"JSON decode failed on retry for {manufacturer} {model}: {exc}")
                extracted = {"_raw_text": text, "_parse_error": str(exc)}

            misses = _validate_extraction(extracted, classification.class_)
            trace.validator_misses = misses

    return extracted, trace
