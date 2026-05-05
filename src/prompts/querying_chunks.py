"""Prompt strings for Stage 5 (extract_specs_agentic) and RAG-search agents.

Helps the agent translate a structured spec target ("how many Dante channels?")
into queries that match the manufacturer's own terminology in indexed datasheet chunks.
"""

import logging

logger = logging.getLogger(__name__)


SPEC_SYNONYMS: dict[str, list[str]] = {
    "dante_channels": [
        "Dante",
        "network audio",
        "audio over IP",
        "AoIP",
        "AES67 channels",
        "media network channels",
    ],
    "analog_inputs": [
        "mic preamps",
        "line inputs",
        "analog audio inputs",
        "balanced inputs",
        "XLR inputs",
        "combo inputs",
    ],
    "analog_outputs": [
        "line outputs",
        "analog outs",
        "balanced outputs",
        "monitor outs",
        "main outs",
        "aux outs",
        "bus outs",
        "mix outs",
        "matrix outs",
        "group outs",
        "sub outs",
    ],
    "aes_io": [
        "AES3",
        "AES/EBU",
        "digital audio",
        "AES output",
        "AES input",
    ],
    "madi_io": [
        "MADI",
        "AES10",
        "multichannel digital",
    ],
    "power": [
        "power supply",
        "mains",
        "PoE",
        "Power over Ethernet",
        "USB bus-powered",
        "DC input",
        "wall adapter",
    ],
    "rack_units": [
        "rack units",
        "RU",
        "1U",
        "2U",
        "half-rack",
        "rack-mountable",
    ],
    "dimensions": [
        "dimensions",
        "size",
        "WxHxD",
        "depth",
        "footprint",
    ],
    "rf_band": [
        "frequency range",
        "RF band",
        "G50",
        "H50",
        "operating frequency",
        "tuning range",
    ],
    "rf_io": [
        "receiver channels",
        "antenna inputs",
        "antenna outputs",
        "BNC",
        "RF cascade",
        "antenna distribution",
        "diversity antennas",
    ],
}

DOC_TYPE_GUIDANCE: dict[str, str] = {
    "spec_sheet": (
        "concrete numbers — channel counts, connector tables, weight, RU, "
        "frequency response. Don't expect prose explanations."
    ),
    "user_manual": (
        "routing diagrams, bridge behavior (which inputs feed which outputs), "
        "DSP block diagrams, scene/preset descriptions."
    ),
    "install_guide": (
        "mounting (rack/wall/truss), power requirements (PoE class, IEC, voltage range), "
        "thermal/airflow, cabling distances."
    ),
}


def build_chunk_query_prompt(spec_target: str, available_doc_types: list[str]) -> str:
    """Compose a prompt that asks an agent to generate search queries for *spec_target*."""
    synonyms = SPEC_SYNONYMS.get(spec_target)
    if synonyms is None:
        logger.warning(
            "build_chunk_query_prompt: no synonyms mapped for spec_target=%r — "
            "falling back to literal target. Add a SPEC_SYNONYMS entry.",
            spec_target,
        )
        synonyms = [spec_target]
    synonym_bullets = "\n".join(f"  - {s}" for s in synonyms)

    guidance_lines = []
    for dt in available_doc_types:
        hint = DOC_TYPE_GUIDANCE.get(dt)
        if hint:
            guidance_lines.append(f"- {dt}: {hint}")
    guidance_block = "\n".join(guidance_lines) if guidance_lines else ""

    return (
        f"You are searching a vector store of datasheet chunks for spec '{spec_target}'.\n"
        f"Also try these synonyms / related phrases:\n{synonym_bullets}\n\n"
        f"Document-type guidance:\n{guidance_block}\n\n"
        "Return a list of 3-5 short search queries that combined cover the synonyms. "
        "Reply ONLY with a JSON array of strings."
    )
