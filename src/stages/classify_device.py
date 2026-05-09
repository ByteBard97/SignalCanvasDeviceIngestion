"""Two-tier device classifier: rule-based regex table → LLM fallback."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Optional

# Ensure src/ is on path when running standalone
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(_REPO_ROOT / "src"))

from moonshot_client import MoonshotClient  # noqa: E402

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CLASSIFIER_MODEL = "moonshot-v1-8k"
LLM_FALLBACK_CONFIDENCE = 0.6
DEFAULT_CONFIDENCE_RULE = 1.0
MARKDOWN_EXCERPT_CHAR_LIMIT = 800

# ---------------------------------------------------------------------------
# Classification result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Classification:
    """Device classification result."""

    class_: str
    confidence: float
    source: str  # 'rule' | 'llm'


# ---------------------------------------------------------------------------
# Rule table: list of (manufacturer_pattern, model_pattern, class_)
# ---------------------------------------------------------------------------
RULE_TABLE: list[tuple[str, str, str]] = [
    # IT / Networking — explicitly out of scope for SignalCanvas
    (r"Cisco", r"SF.*|Catalyst.*|Nexus.*|ISR.*|ASA.*|Meraki.*|SG.*|CBS.*", "it_networking"),
    (r"Ubiquiti", r"USW.*|UDM.*|USG.*|EdgeRouter.*|EdgeSwitch.*|UAP.*", "it_networking"),
    (r"MikroTik", r"CRS.*|CCR.*|RB.*|hEX.*|CRS.*", "it_networking"),
    (r"Aruba", r".*", "it_networking"),
    (r"Juniper", r".*", "it_networking"),
    (r"Fortinet", r".*", "it_networking"),
    (r"Palo\s+Alto", r".*", "it_networking"),
    (r"TP-Link", r".*", "it_networking"),
    (r"Netgear", r".*", "it_networking"),
    (r"D-Link", r".*", "it_networking"),
    (r"Linksys", r".*", "it_networking"),
    (r"HP", r".*", "it_networking"),
    (r"HPE", r".*", "it_networking"),
    (r"Dell", r"PowerConnect.*|Networking.*", "it_networking"),
    (r"SonicWall", r".*", "it_networking"),
    (r"WatchGuard", r".*", "it_networking"),
    (r"Extreme", r".*", "it_networking"),
    (r"Ruckus", r".*", "it_networking"),
    # Dante stageboxes
    (r"Yamaha", r"Rio.*", "dante_stagebox"),
    # Dante input adapters (analog → Dante)
    (r"Audinate", r"AVIO-AI.*", "dante_adapter_input"),
    # Dante output adapters (Dante → analog)
    (r"Audinate", r"AVIO-AO.*", "dante_adapter_output"),
    # Wireless receivers
    (r"Shure", r"ULXD.*", "wireless_rx"),
    (r"Sennheiser", r"EW-DX-EM.*", "wireless_rx"),
    # Mixing consoles
    (r"Allen.*Heath", r"SQ-.*", "mixing_console"),
    # DSP processors
    (r"QSC", r"Core.*", "dsp_processor"),
]

VALID_CLASSES = {
    "dante_stagebox",
    "dante_adapter_input",
    "dante_adapter_output",
    "wireless_rx",
    "mixing_console",
    "dsp_processor",
    "it_networking",
    "generic",
}

# Pre-compile for speed
_COMPILED_RULES: list[tuple[re.Pattern, re.Pattern, str]] = [
    (re.compile(mfr, re.IGNORECASE), re.compile(mdl, re.IGNORECASE), cls)
    for mfr, mdl, cls in RULE_TABLE
]


# ---------------------------------------------------------------------------
# Tier A: rule-based classification
# ---------------------------------------------------------------------------

def _classify_by_rule(manufacturer: str, model: str) -> Optional[Classification]:
    """Match against compiled regex rule table."""
    for mfr_re, mdl_re, cls in _COMPILED_RULES:
        if mfr_re.search(manufacturer) and mdl_re.search(model):
            return Classification(class_=cls, confidence=DEFAULT_CONFIDENCE_RULE, source="rule")
    return None


# ---------------------------------------------------------------------------
# Tier B: LLM fallback classification
# ---------------------------------------------------------------------------

_CLASSIFICATION_SYSTEM_PROMPT = (
    "You are a device classifier for professional audio/video equipment. "
    "SignalCanvas documents AV signal flow (mixers, speakers, cameras, switchers, DSP, etc.) "
    "and explicitly does NOT document IT/networking infrastructure.\n\n"
    "Given a manufacturer and model, emit exactly one classification token from this list:\n"
    "dante_stagebox, dante_adapter_input, dante_adapter_output, "
    "wireless_rx, mixing_console, dsp_processor, it_networking, generic\n\n"
    "Use 'it_networking' for switches, routers, firewalls, access points, and other pure "
    "networking/IT infrastructure. Use 'generic' for any AV device that does not fit the "
    "specific categories above.\n"
    "Respond with ONLY the token, no punctuation or explanation."
)


async def _classify_by_llm(
    manufacturer: str,
    model: str,
    markdown_excerpt: Optional[str] = None,
    moonshot: Optional[MoonshotClient] = None,
) -> Classification:
    """Ask Moonshot to classify when no rule matches."""
    client = moonshot or MoonshotClient()
    excerpt = markdown_excerpt or ""
    if excerpt:
        excerpt = excerpt[:MARKDOWN_EXCERPT_CHAR_LIMIT]

    prompt = f"Manufacturer: {manufacturer}\nModel: {model}"
    if excerpt:
        prompt += f"\n\nExcerpt from datasheet:\n{excerpt}"

    try:
        text, _usage = await client.chat_completion(
            prompt,
            model=CLASSIFIER_MODEL,
            system=_CLASSIFICATION_SYSTEM_PROMPT,
            temperature=0.0,
        )
    except Exception as exc:
        logger.warning(f"LLM classification failed for {manufacturer} {model}: {exc}")
        return Classification(class_="generic", confidence=LLM_FALLBACK_CONFIDENCE, source="llm")
    finally:
        if moonshot is None:
            await client.close()

    token = text.strip().lower().rstrip(".")
    if token in VALID_CLASSES:
        return Classification(class_=token, confidence=LLM_FALLBACK_CONFIDENCE, source="llm")

    # Fuzzy match: if the response contains a valid class substring
    for valid in VALID_CLASSES:
        if valid in token:
            return Classification(class_=valid, confidence=LLM_FALLBACK_CONFIDENCE, source="llm")

    return Classification(class_="generic", confidence=LLM_FALLBACK_CONFIDENCE, source="llm")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def classify(
    manufacturer: str,
    model: str,
    markdown_excerpt: Optional[str] = None,
    moonshot: Optional[MoonshotClient] = None,
) -> Classification:
    """Classify a device using rules first, then LLM fallback."""
    rule_result = _classify_by_rule(manufacturer, model)
    if rule_result is not None:
        return rule_result
    return await _classify_by_llm(manufacturer, model, markdown_excerpt, moonshot)
