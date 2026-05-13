"""Combined device context from patchify + EasySchematic.

Provides a merged view of device data from both sources. The pipeline agents
can use this as starting context instead of searching from scratch.

Known limitations (the agent should verify, not trust blindly):
- Port lists may be incomplete or wrong
- Bridge/routing data is NEVER present in either source
- Physical specs are estimates or missing
"""

import json
from pathlib import Path
from typing import Any, Optional

_EASYSCHEMATIC_API_URL = "https://api.easyschematic.live/templates"
_EASYSCHEMATIC_INDEX: dict[str, dict] = {}
_PATCHIFY_INDEX: dict[str, dict] = {}


def _normalize_key(manufacturer: str, model: str) -> str:
    """Create a lookup key from manufacturer + model."""
    mfg = manufacturer.lower().strip().replace(" ", "-").replace("_", "-")
    model_part = model.lower().strip().replace(" ", "-").replace("_", "-")
    return f"{mfg}::{model_part}"


def _build_easyschematic_index() -> None:
    """Load EasySchematic templates from local cache and build lookup index."""
    global _EASYSCHEMATIC_INDEX
    if _EASYSCHEMATIC_INDEX:
        return

    # Use local cache from prior API fetch (May 3 2026)
    cache_path = Path("output/easyschematic/all_devices.json")
    try:
        with open(cache_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        templates = data.get("devices", [])
    except Exception:
        # Fall back to API if cache missing
        import urllib.request
        try:
            with urllib.request.urlopen(_EASYSCHEMATIC_API_URL, timeout=30) as resp:
                templates = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            print(f"Warning: failed to load EasySchematic templates: {e}")
            templates = []

    for t in templates:
        mfg = (t.get("manufacturer") or "").strip()
        model = (t.get("modelNumber") or "").strip()
        label = (t.get("label") or "").strip()
        if not mfg:
            continue

        # Index by modelNumber (SKU) if present
        if model:
            key = _normalize_key(mfg, model)
            _EASYSCHEMATIC_INDEX[key] = t
            key_swapped = _normalize_key(model, mfg)
            if key_swapped != key:
                _EASYSCHEMATIC_INDEX[key_swapped] = t

        # Also index by human-readable label for lookup by product name
        if label:
            key_label = _normalize_key(mfg, label)
            _EASYSCHEMATIC_INDEX[key_label] = t
            key_label_swapped = _normalize_key(label, mfg)
            if key_label_swapped != key_label:
                _EASYSCHEMATIC_INDEX[key_label_swapped] = t


def _build_patchify_index() -> None:
    """Build patchify lookup index by device_id."""
    global _PATCHIFY_INDEX
    if _PATCHIFY_INDEX:
        return

    patchify_path = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
    if not patchify_path.exists():
        return

    with open(patchify_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    for item in data:
        mfg = item.get("manufacturer", "")
        name = item.get("name", "")

        mfg_slug = mfg.lower().replace(" ", "-").replace("_", "-")
        name_slug = name.lower().replace(" ", "-").replace("/", "-").replace("_", "-")
        name_slug = "-".join(filter(None, name_slug.split("-")))
        device_id = f"{mfg_slug}-{name_slug}"[:64]

        if device_id:
            _PATCHIFY_INDEX[device_id] = item


def get_combined_context(device_id: str, manufacturer: str, model: str) -> Optional[dict]:
    """Return merged device context from patchify + EasySchematic.

    Returns None if neither source has data for this device.
    """
    _build_patchify_index()
    _build_easyschematic_index()

    patchify_item = _PATCHIFY_INDEX.get(device_id)

    es_key = _normalize_key(manufacturer, model)
    es_item = _EASYSCHEMATIC_INDEX.get(es_key)

    if not patchify_item and not es_item:
        return None

    # Start with EasySchematic data (more structured)
    context: dict[str, Any] = {
        "sources": [],
        "device_type": None,
        "category": None,
        "manufacturer": manufacturer,
        "model": model,
        "reference_url": None,
        "search_terms": [],
        "physical_specs": {},
        "ports": [],
        "notes": [],
    }

    if es_item:
        context["sources"].append("easyschematic")
        context["device_type"] = es_item.get("deviceType")
        context["category"] = es_item.get("category")
        context["reference_url"] = es_item.get("referenceUrl")
        context["search_terms"] = es_item.get("searchTerms") or []

        # Physical specs
        phys = {}
        if es_item.get("powerDrawW"):
            phys["power_draw_w"] = es_item["powerDrawW"]
        if es_item.get("heightMm"):
            phys["height_mm"] = es_item["heightMm"]
        if es_item.get("widthMm"):
            phys["width_mm"] = es_item["widthMm"]
        if es_item.get("depthMm"):
            phys["depth_mm"] = es_item["depthMm"]
        if es_item.get("weightKg"):
            phys["weight_kg"] = es_item["weightKg"]
        if phys:
            context["physical_specs"] = phys

        # Ports from EasySchematic
        for p in es_item.get("ports", []):
            context["ports"].append({
                "name": p.get("label"),
                "direction": p.get("direction"),
                "signal_type": p.get("signalType"),
                "connector": p.get("connectorType"),
                "source": "easyschematic",
            })

    if patchify_item:
        context["sources"].append("patchify")

        # Use patchify category if EasySchematic didn't have one
        if not context["category"]:
            context["category"] = patchify_item.get("category")

        # Ports from patchify (may overlap or conflict with EasySchematic)
        for inp in patchify_item.get("inputs", []):
            context["ports"].append({
                "name": inp.get("label"),
                "direction": "input",
                "signal_type": inp.get("signal") or inp.get("type"),
                "connector": inp.get("connector"),
                "source": "patchify",
            })
        for out in patchify_item.get("outputs", []):
            context["ports"].append({
                "name": out.get("label"),
                "direction": "output",
                "signal_type": out.get("signal") or out.get("type"),
                "connector": out.get("connector"),
                "source": "patchify",
            })

    # Deduplicate ports by name (EasySchematic wins over patchify on conflict)
    seen: dict[str, dict] = {}
    for p in context["ports"]:
        name = (p.get("name") or "").lower().strip()
        if name and name not in seen:
            seen[name] = p
    context["ports"] = list(seen.values())

    # Warning note
    context["notes"].append(
        "This context is compiled from community-submitted data (patchify) and "
        "template data (EasySchematic). It may contain errors, omissions, or outdated "
        "information. The agent should verify all claims against authoritative sources."
    )
    context["notes"].append(
        "CRITICAL: Neither source contains bridge/routing information. The agent must "
        "investigate datasheets or technical documentation to determine how ports connect "
        "internally (e.g., which inputs feed which outputs, mix bus routing, matrix paths)."
    )

    return context


def get_context_as_prompt_text(device_id: str, manufacturer: str, model: str) -> str:
    """Return the combined context formatted as prompt text for an LLM."""
    ctx = get_combined_context(device_id, manufacturer, model)
    if not ctx:
        return ""

    lines = [
        "=== KNOWN DEVICE CONTEXT (from patchify + EasySchematic) ===",
        f"Sources: {', '.join(ctx['sources'])}",
        f"Device Type: {ctx['device_type'] or 'unknown'}",
        f"Category: {ctx['category'] or 'unknown'}",
    ]

    if ctx["reference_url"]:
        lines.append(f"Reference URL: {ctx['reference_url']}")

    if ctx["search_terms"]:
        lines.append(f"Search Terms: {', '.join(ctx['search_terms'])}")

    if ctx["physical_specs"]:
        lines.append("Physical Specs (may be estimates):")
        for k, v in ctx["physical_specs"].items():
            lines.append(f"  {k}: {v}")

    if ctx["ports"]:
        lines.append("Known Ports (may be incomplete or wrong):")
        for p in ctx["ports"]:
            conn = f" ({p['connector']})" if p.get("connector") else ""
            sig = f" [{p['signal_type']}]" if p.get("signal_type") else ""
            lines.append(f"  {p['direction']:12} {p['name']}{conn}{sig}  (from {p['source']})")

    lines.append("")
    for note in ctx["notes"]:
        lines.append(f"NOTE: {note}")

    lines.append("=== END CONTEXT ===")
    return "\n".join(lines)
