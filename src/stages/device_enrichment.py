"""Deterministic enrichment for well-documented devices.

Injects known missing ports when the LLM extraction drops them due to
token limits.  Acts as a safety net, not a replacement for extraction.
"""

from __future__ import annotations

import copy

# ---------------------------------------------------------------------------
# Hard-coded enrichment table
# ---------------------------------------------------------------------------

_ENRICHMENT: dict[str, list[dict]] = {
    # SQ-5 main outputs are now reliably found by the focused ports pass;
    # enrichment removed to avoid duplicates.
    "QSC:Core 110f": [
        {"name": "Network", "direction": "input_output", "connector": "RJ45", "channels": 2, "attributes": ["enriched"]},
    ],
    "Yamaha:Rio1608-D2": [
        {"name": "Dante", "direction": "input_output", "connector": "etherCON", "channels": 16, "attributes": ["enriched", "primary"]},
    ],
}


def _ensure_attributes(port: dict) -> list[str]:
    """Return the port's attributes as a list, creating one if absent."""
    attrs = port.get("attributes")
    if attrs is None:
        return []
    if isinstance(attrs, list):
        return list(attrs)
    # Handle scalar attribute (shouldn't happen post-normalization, but be safe)
    return [str(attrs)]


def enrich_extraction(extracted: dict, manufacturer: str, model_number: str) -> dict:
    """Augment *extracted* with known missing ports for well-documented devices.

    Args:
        extracted: The normalized extraction dict (mutated in place and returned).
        manufacturer: Device manufacturer.
        model_number: Device model number.

    Returns:
        The mutated *extracted* dict.
    """
    result = extracted
    signal_flow = result.setdefault("signal_flow", {})
    ports: list[dict] = signal_flow.get("ports") or []

    lookup_key = f"{manufacturer}:{model_number}"
    enriched_ports = _ENRICHMENT.get(lookup_key)
    if enriched_ports is None:
        # Try case-insensitive match
        for key, ports_template in _ENRICHMENT.items():
            if key.lower() == lookup_key.lower():
                enriched_ports = ports_template
                break

    if not enriched_ports:
        return result

    # Build a lookup of existing ports by name + connector for precise matching
    existing_by_name_connector: dict[tuple[str, str | None], dict] = {}
    existing_by_name: dict[str, list[dict]] = {}
    for port in ports:
        name = port.get("name", "")
        connector = port.get("connector")
        existing_by_name_connector[(name, connector)] = port
        existing_by_name.setdefault(name, []).append(port)

    for template in enriched_ports:
        template_name = template["name"]
        template_connector = template.get("connector")

        # Try exact name + connector match first
        existing = existing_by_name_connector.get((template_name, template_connector))

        if existing is None:
            # Fall back to name-only match if we can't find name+connector
            candidates = existing_by_name.get(template_name, [])
            # Prefer candidate with null connector (needs filling in)
            for cand in candidates:
                if cand.get("connector") is None:
                    existing = cand
                    break
            # Otherwise just take the first candidate
            if existing is None and candidates:
                existing = candidates[0]

        if existing is None:
            # Port is completely missing — append a deep copy
            new_port = copy.deepcopy(template)
            attrs = _ensure_attributes(new_port)
            if "enriched" not in attrs:
                attrs.append("enriched")
            new_port["attributes"] = attrs
            ports.append(new_port)
        else:
            # Port exists — fill in null fields and ensure "enriched" attribute
            if existing.get("connector") is None and template.get("connector") is not None:
                existing["connector"] = template["connector"]
            if existing.get("channels") is None and template.get("channels") is not None:
                existing["channels"] = template["channels"]
            if existing.get("direction") is None and template.get("direction") is not None:
                existing["direction"] = template["direction"]

            attrs = _ensure_attributes(existing)
            if "enriched" not in attrs:
                attrs.append("enriched")
            existing["attributes"] = attrs

    signal_flow["ports"] = ports
    return result
