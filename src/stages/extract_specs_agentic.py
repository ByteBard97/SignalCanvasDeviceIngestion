"""Hybrid agentic Stage 5 extractor: classify → template queries → generic safety net → dedupe → extract → validate."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
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
from stages.normalize_specs import (
    normalize_extraction,
    _canonicalize_name,
    _normalize_bridge,
)  # noqa: E402
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
    ("moonshot-v1-8k", 7500),
    ("moonshot-v1-32k", 30000),
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
        f"Prompt too large even for largest tier: {needed} tokens " f"(limits: {TIER_INPUT_LIMITS})"
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
    pass_a_tokens: int = 0
    pass_b_tokens: int = 0
    pass_a_tier: str = ""
    pass_b_tier: str = ""


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
        f'are found, return {{"ports":[]}}.\n\n'
        f"=== Chunks ===\n"
        f"{chunks_text}"
    )
    return prompt


def _format_chunks(chunks: list[dict]) -> str:
    """Format chunk list into prompt text."""
    parts: list[str] = []
    for chunk in chunks:
        source = chunk.get("source", "unknown")
        section = chunk.get("section", "unknown")
        text = chunk.get("text", "")
        parts.append(f"[{source}, {section}]\n{text}")
    return "\n\n".join(parts)


def _detect_protocol_hints(chunks: list[dict]) -> str:
    """Scan chunks for network-audio protocol keywords and return a hint string.

    This helps the LLM avoid defaulting to 'Dante' for every RJ45/etherCON port.
    """
    text = "\n".join(c.get("text", "") for c in chunks).lower()
    protocols_found: list[str] = []
    protocol_keywords = {
        "Dante": ["dante"],
        "AES50": ["aes50"],
        "MADI": ["madi"],
        "AVB": ["avb", "ieee 802.1"],
        "Ultranet": ["ultranet"],
        "Cobranet": ["cobranet"],
        "Milan": ["milan"],
    }
    for protocol, keywords in protocol_keywords.items():
        if any(kw in text for kw in keywords):
            protocols_found.append(protocol)

    if not protocols_found:
        return (
            "\nProtocol guidance: No explicit network-audio protocol (Dante, AES50, MADI, AVB, "
            "Ultranet, Milan) was detected in the source chunks. If you see RJ45 or etherCON "
            "ports used for audio, do NOT assume Dante. Use a generic attribute like 'Network' "
            "or the specific protocol name ONLY if the document explicitly states it.\n"
        )
    return (
        f"\nProtocol guidance: The document explicitly mentions these network-audio protocols: "
        f"{', '.join(protocols_found)}. Use ONLY these protocol names for RJ45/etherCON audio "
        f"ports. Do NOT add Dante unless it is explicitly listed above.\n"
    )


def _build_ports_prompt(
    manufacturer: str,
    model: str,
    chunks: list[dict],
    disambiguation: Optional[str] = None,
) -> str:
    """Build Pass A prompt: ports and bridges only."""
    chunks_text = _format_chunks(chunks)
    disambiguation_section = ""
    if disambiguation:
        disambiguation_section = f"DEVICE DISAMBIGUATION (READ CAREFULLY):\n{disambiguation}\n\n"
    protocol_hints = _detect_protocol_hints(chunks)
    return (
        f"You are the SignalCanvas device-extraction agent. Extract ONLY the physical "
        f"connectors and signal-flow bridges for {manufacturer} {model} from the chunks below.\n\n"
        f"{disambiguation_section}"
        f"List EVERY physical connector on this device. Do not omit any port.\n\n"
        f"Return JSON ONLY, no commentary, no markdown:\n"
        f'{{ "signal_flow": {{ "ports": [{{"name","direction","connector","channels","attributes":[]}}], '
        f'"bridges": ["from->to"] }} }}\n\n'
        f"Connector canonicalization rules: If a port uses XLR, TRS, RJ45, USB, SMA, "
        f"BNC, LEMO, HDMI, RCA, or Euroblock connectors, you MUST specify the connector type. "
        f"Do not leave connector as null when the connector type is obvious from the port name "
        f"or surrounding context.\n\n"
        f"Network-audio protocol rules: RJ45 and etherCON connectors are used for MANY "
        f"different protocols (Dante, AES50, MADI, AVB, Ultranet, Milan, plain Ethernet). "
        f"Do NOT default to 'Dante'. Only label a port with a specific protocol if the "
        f"document explicitly mentions it. When in doubt, use a generic attribute or omit it.\n"
        f"{protocol_hints}\n"
        f"CRITICAL channel-vs-connector rule: A single physical connector that carries "
        f"multiple audio channels (e.g., '64 Dante channels over RJ45', '512 DMX channels over XLR', "
        f"'64 MADI channels over BNC', '128 GigaACE channels over etherCON') is ONE physical port. "
        f"Set channels to 1 (or omit it) for that connector. ONLY use channels > 1 when there are "
        f"multiple IDENTICAL physical connectors in a row (e.g., '16 XLR mic inputs' = channels: 16). "
        f"NEVER expand a channel count into multiple ports.\n\n"
        f"Use null for unknown fields. Do not invent specs.\n\n"
        f"=== Chunks ===\n"
        f"{chunks_text}"
    )


def _build_metadata_prompt(
    manufacturer: str,
    model: str,
    chunks: list[dict],
    disambiguation: Optional[str] = None,
) -> str:
    """Build Pass B prompt: metadata, physical specs, power, confidence, notes."""
    chunks_text = _format_chunks(chunks)
    disambiguation_section = ""
    if disambiguation:
        disambiguation_section = f"DEVICE DISAMBIGUATION (READ CAREFULLY):\n{disambiguation}\n\n"
    return (
        f"You are the SignalCanvas device-extraction agent. Extract metadata, physical specs, "
        f"power specs, and confidence for {manufacturer} {model} from the chunks below.\n\n"
        f"{disambiguation_section}"
        f"Return JSON ONLY, no commentary, no markdown:\n"
        f"{{\n"
        f'  "device_metadata": {{ "manufacturer", "model_number", "label", "device_type", "category" }},\n'
        f'  "physical_specs": {{ "height_mm", "width_mm", "depth_mm", "weight_kg" }},\n'
        f'  "power_specs": {{ "power_draw_w", "voltage", "thermal_btuh", "poe_budget_w", "poe_draw_w" }},\n'
        f'  "extraction_confidence": "high|medium|low",\n'
        f'  "notes": "..."\n'
        f"}}\n\n"
        f"Use null for unknown fields. Do not invent specs.\n\n"
        f"=== Chunks ===\n"
        f"{chunks_text}"
    )


def _merge_passes(ports_result: dict, metadata_result: dict) -> dict:
    """Merge Pass A and Pass B results. Ports dict wins on signal_flow."""
    merged = dict(metadata_result)
    if "signal_flow" in ports_result:
        merged["signal_flow"] = ports_result["signal_flow"]
    return merged


def _aggregate_usage(a: UsageRecord, b: UsageRecord) -> UsageRecord:
    """Sum two UsageRecords."""
    return UsageRecord(
        model=f"{a.model}+{b.model}",
        prompt_tokens=a.prompt_tokens + b.prompt_tokens,
        completion_tokens=a.completion_tokens + b.completion_tokens,
        total_tokens=a.total_tokens + b.total_tokens,
        elapsed_ms=a.elapsed_ms + b.elapsed_ms,
    )


async def _run_llm_pass(
    prompt: str,
    moonshot: MoonshotClient,
    max_tokens: Optional[int] = None,
) -> tuple[dict, str, int, str, UsageRecord]:
    """Run a single LLM pass and return (parsed, raw_text, estimated_tokens, tier, usage)."""
    estimated = await moonshot.estimate_tokens(prompt)
    tier = pick_tier(estimated)
    text, usage = await moonshot.chat_completion(
        prompt,
        model=tier,
        response_format_json=True,
        temperature=EXTRACTION_TEMPERATURE,
        seed=EXTRACTION_SEED,
        max_tokens=max_tokens,
    )
    try:
        parsed: dict = json.loads(text)
    except json.JSONDecodeError:
        parsed = {}
    return parsed, text, estimated, tier, usage


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

    logger.info(f"{manufacturer} {model} peripheral pass found {len(merged)} new port(s)")
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

    # 8. Run Pass A (ports) and Pass B (metadata)
    prompt_a = _build_ports_prompt(manufacturer, model, chunks_to_use, disambiguation)
    extracted_ports, text_a, est_a, tier_a, usage_a = await _run_llm_pass(
        prompt_a, moonshot, max_tokens=1200
    )
    trace.pass_a_tokens = est_a
    trace.pass_a_tier = tier_a
    logger.info(f"{manufacturer} {model} (ports pass): estimated {est_a} tokens → tier {tier_a}")

    prompt_b = _build_metadata_prompt(manufacturer, model, chunks_to_use, disambiguation)
    extracted_metadata, text_b, est_b, tier_b, usage_b = await _run_llm_pass(prompt_b, moonshot)
    trace.pass_b_tokens = est_b
    trace.pass_b_tier = tier_b
    trace.estimated_input_tokens = est_a + est_b
    trace.model_tier = f"{tier_a}+{tier_b}"
    trace.usage = _aggregate_usage(usage_a, usage_b)
    logger.info(f"{manufacturer} {model} (metadata pass): estimated {est_b} tokens → tier {tier_b}")

    # Parse and merge
    if not extracted_ports and not extracted_metadata:
        extracted: dict = {
            "_raw_text_a": text_a,
            "_raw_text_b": text_b,
            "_parse_error": "Both passes failed JSON decode",
        }
    else:
        extracted = _merge_passes(extracted_ports, extracted_metadata)
        extracted = _infer_connectors(extracted)
        extracted = normalize_extraction(extracted, classification.class_)

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
        target = misses[0]
        extra_chunks = await _requery_for_category(http, corpus_id, target, manufacturer, model)
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

            prompt_a = _build_ports_prompt(manufacturer, model, chunks_to_use, disambiguation)
            extracted_ports, text_a, est_a, tier_a, usage_a = await _run_llm_pass(
                prompt_a, moonshot, max_tokens=1200
            )
            trace.pass_a_tokens = est_a
            trace.pass_a_tier = tier_a

            prompt_b = _build_metadata_prompt(manufacturer, model, chunks_to_use, disambiguation)
            extracted_metadata, text_b, est_b, tier_b, usage_b = await _run_llm_pass(
                prompt_b, moonshot
            )
            trace.pass_b_tokens = est_b
            trace.pass_b_tier = tier_b
            trace.estimated_input_tokens = est_a + est_b
            trace.model_tier = f"{tier_a}+{tier_b}"
            trace.usage = _aggregate_usage(usage_a, usage_b)
            logger.info(f"{manufacturer} {model} (retry): ports={tier_a} meta={tier_b}")

            if not extracted_ports and not extracted_metadata:
                extracted = {
                    "_raw_text_a": text_a,
                    "_raw_text_b": text_b,
                    "_parse_error": "Both passes failed JSON decode on retry",
                }
            else:
                extracted = _merge_passes(extracted_ports, extracted_metadata)
                extracted = _infer_connectors(extracted)
                extracted = normalize_extraction(extracted, classification.class_)

            misses = _validate_extraction(extracted, classification.class_)
            trace.validator_misses = misses

    return extracted, trace


# ---------------------------------------------------------------------------
# N-shot majority-voting wrapper
# ---------------------------------------------------------------------------


def _port_richness(port: dict) -> int:
    """Score a port by how many key fields are filled."""
    return sum(
        1
        for k in ("direction", "connector", "channels")
        if port.get(k) is not None and port.get(k) != ""
    )


async def extract_n_shot(
    http: httpx.AsyncClient,
    corpus_id: str,
    manufacturer: str,
    model: str,
    moonshot: MoonshotClient,
    n: int = 3,
) -> tuple[dict, ExtractionTrace]:
    """Run extraction N times and merge results via majority voting.

    Port merge strategy:
      - Union of all ports across runs, deduplicated by canonical name.
      - If the same canonical name appears in multiple runs, keep the
        "richest" version (most fields filled: direction, connector, channels).

    Bridge merge strategy:
      - Normalize each bridge string.
      - Keep bridges that appear in >= ceil(N/2) runs (majority vote).

    Metadata / physical_specs / power_specs:
      - Taken from the first successful run.

    Trace aggregation:
      - estimated_input_tokens = sum across runs
      - usage = sum across runs (new UsageRecord)
      - retries = max across runs
    """
    if n <= 1:
        return await extract(manufacturer, model, corpus_id, http, moonshot)

    sem = asyncio.Semaphore(2)

    async def _one() -> tuple[dict, ExtractionTrace]:
        async with sem:
            return await extract(manufacturer, model, corpus_id, http, moonshot)

    results: list[tuple[dict, ExtractionTrace]] = []
    for _ in range(n):
        extracted, trace = await _one()
        results.append((extracted, trace))

    # Filter to successful runs (dict without parse error)
    successful = [
        (ext, tr) for ext, tr in results if isinstance(ext, dict) and "_parse_error" not in ext
    ]

    if not successful:
        # All failed — return first failure
        return results[0]

    first_ext, first_trace = successful[0]

    # ------------------------------------------------------------------
    # Merge ports: union by canonical name, keep richest
    # ------------------------------------------------------------------
    port_map: dict[str, dict] = {}
    for ext, _tr in successful:
        ports = ext.get("signal_flow", {}).get("ports", [])
        if not isinstance(ports, list):
            continue
        for port in ports:
            if not isinstance(port, dict):
                continue
            name = port.get("name")
            if not name:
                continue
            canonical = _canonicalize_name(name)
            if canonical not in port_map:
                port_map[canonical] = dict(port)
            else:
                existing = port_map[canonical]
                if _port_richness(port) > _port_richness(existing):
                    port_map[canonical] = dict(port)
                elif _port_richness(port) == _port_richness(existing):
                    # Merge attributes if richness is equal
                    attrs = set(existing.get("attributes") or [])
                    attrs.update(port.get("attributes") or [])
                    merged = dict(existing)
                    merged["attributes"] = sorted(attrs) if attrs else None
                    # Take any non-null field from port if existing is null
                    for k in ("direction", "connector", "channels"):
                        if merged.get(k) is None and port.get(k) is not None:
                            merged[k] = port[k]
                    port_map[canonical] = merged

    merged_ports = list(port_map.values())

    # ------------------------------------------------------------------
    # Merge bridges: majority vote
    # ------------------------------------------------------------------
    bridge_counts: dict[str, int] = {}
    for ext, _tr in successful:
        bridges = ext.get("signal_flow", {}).get("bridges", [])
        if not isinstance(bridges, list):
            continue
        seen_in_run: set[str] = set()
        for bridge in bridges:
            if not isinstance(bridge, str):
                continue
            norm = _normalize_bridge(bridge)
            if norm is None:
                continue
            if norm not in seen_in_run:
                seen_in_run.add(norm)
                bridge_counts[norm] = bridge_counts.get(norm, 0) + 1

    threshold = math.ceil(n / 2)
    merged_bridges = [b for b, cnt in bridge_counts.items() if cnt >= threshold]

    # ------------------------------------------------------------------
    # Build merged dict from first successful run, swapping ports/bridges
    # ------------------------------------------------------------------
    merged = json.loads(json.dumps(first_ext))  # deep copy
    signal_flow = merged.setdefault("signal_flow", {})
    signal_flow["ports"] = merged_ports
    signal_flow["bridges"] = merged_bridges

    # ------------------------------------------------------------------
    # Aggregate traces
    # ------------------------------------------------------------------
    total_estimated_input_tokens = sum(tr.estimated_input_tokens for _ext, tr in results)

    total_prompt_tokens = sum((tr.usage.prompt_tokens if tr.usage else 0) for _ext, tr in results)
    total_completion_tokens = sum(
        (tr.usage.completion_tokens if tr.usage else 0) for _ext, tr in results
    )
    total_tokens = sum((tr.usage.total_tokens if tr.usage else 0) for _ext, tr in results)
    total_elapsed_ms = sum((tr.usage.elapsed_ms if tr.usage else 0) for _ext, tr in results)

    # Pick the most common model tier among successful runs
    tier_votes: dict[str, int] = {}
    pass_a_tier_votes: dict[str, int] = {}
    pass_b_tier_votes: dict[str, int] = {}
    for _ext, tr in successful:
        if tr.model_tier:
            tier_votes[tr.model_tier] = tier_votes.get(tr.model_tier, 0) + 1
        if tr.pass_a_tier:
            pass_a_tier_votes[tr.pass_a_tier] = pass_a_tier_votes.get(tr.pass_a_tier, 0) + 1
        if tr.pass_b_tier:
            pass_b_tier_votes[tr.pass_b_tier] = pass_b_tier_votes.get(tr.pass_b_tier, 0) + 1
    best_tier = max(tier_votes, key=tier_votes.get) if tier_votes else ""
    best_pass_a_tier = max(pass_a_tier_votes, key=pass_a_tier_votes.get) if pass_a_tier_votes else ""
    best_pass_b_tier = max(pass_b_tier_votes, key=pass_b_tier_votes.get) if pass_b_tier_votes else ""

    total_pass_a_tokens = sum(tr.pass_a_tokens for _ext, tr in results)
    total_pass_b_tokens = sum(tr.pass_b_tokens for _ext, tr in results)

    aggregated_usage = UsageRecord(
        model=best_tier,
        prompt_tokens=total_prompt_tokens,
        completion_tokens=total_completion_tokens,
        total_tokens=total_tokens,
        elapsed_ms=total_elapsed_ms,
    )

    # Union queries and validator misses across all runs
    pass1_queries_union: list[str] = []
    pass2_queries_union: list[str] = []
    validator_misses_union: list[str] = []
    seen_p1: set[str] = set()
    seen_p2: set[str] = set()
    seen_vm: set[str] = set()
    max_retries = 0
    floor_triggered = False

    for _ext, tr in results:
        for q in tr.pass1_queries:
            if q not in seen_p1:
                seen_p1.add(q)
                pass1_queries_union.append(q)
        for q in tr.pass2_queries:
            if q not in seen_p2:
                seen_p2.add(q)
                pass2_queries_union.append(q)
        for m in tr.validator_misses:
            if m not in seen_vm:
                seen_vm.add(m)
                validator_misses_union.append(m)
        if tr.retries > max_retries:
            max_retries = tr.retries
        if tr.model_filter_floor_triggered:
            floor_triggered = True

    composite_trace = ExtractionTrace(
        classification=first_trace.classification,
        pass1_queries=pass1_queries_union,
        pass2_queries=pass2_queries_union,
        pass1_chunk_count=max(tr.pass1_chunk_count for _ext, tr in results),
        pass2_chunk_count=max(tr.pass2_chunk_count for _ext, tr in results),
        deduped_chunk_count=max(tr.deduped_chunk_count for _ext, tr in results),
        model_filtered_chunk_count=max(tr.model_filtered_chunk_count for _ext, tr in results),
        effective_chunk_count=max(tr.effective_chunk_count for _ext, tr in results),
        model_filter_floor_triggered=floor_triggered,
        retries=max_retries,
        validator_misses=validator_misses_union,
        usage=aggregated_usage,
        aliases_used=first_trace.aliases_used,
        disambiguation_applied=first_trace.disambiguation_applied,
        estimated_input_tokens=total_estimated_input_tokens,
        model_tier=best_tier,
        pass_a_tokens=total_pass_a_tokens,
        pass_b_tokens=total_pass_b_tokens,
        pass_a_tier=best_pass_a_tier,
        pass_b_tier=best_pass_b_tier,
    )

    return merged, composite_trace
