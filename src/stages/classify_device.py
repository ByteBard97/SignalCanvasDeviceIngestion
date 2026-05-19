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
    # Cisco AV codecs and cameras (in scope) — must come before IT rules
    # so the LLM never sees them and misclassifies based on brand bias
    (r"Cisco", r"Codec.*|CS-CPRO.*|CS-CODEC.*|SX.*|MX.*|Room\s*Kit.*|Room\s*Bar.*|Room\s*Navigator.*|Webex.*|Precision.*|SpeakerTrack.*|TelePresence.*|P40.*|P60.*|Desk.*", "codec"),
    # Confirmed AV brands that the LLM might misclassify due to brand association —
    # must come before any it_networking catch-alls
    (r"LynxTechnik|Lynx.Technik", r".*", "generic"),
    (r"AJA", r".*", "generic"),
    (r"Ross.Video|Ross", r".*", "generic"),
    (r"Blackmagic.Design|Blackmagic", r".*", "generic"),
    (r"AVMATRIX|AVMatrix", r".*", "generic"),
    (r"AVPro.Edge|AVPro", r".*MXnet.*|.*MX.NET.*|.*SW\d+.*", "generic"),
    (r"Crestron", r".*", "generic"),
    # Ubiquiti Enterprise AV Fiber — SMPTE 2110 AV-over-IP switch, not a standard Ubiquiti IT switch
    (r"Ubiquiti", r".*Enterprise.*AV.*|.*EAV.*", "generic"),
    # ELC and DMXTranscension — DMX lighting control, not Dante
    (r"ELC", r".*", "generic"),
    (r"DMXTranscension|DMX.Transcension", r".*", "generic"),
    # Encore/Clear-Com intercom wall panels — intercom is an AV signal type
    (r"Encore.*Pullman|Encore", r".*Wall.*Plate.*|.*Intercom.*|.*Ops.*", "generic"),
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
    # Intercom systems — must come before generic LLM fallback
    (r"Green-GO", r".*", "intercom"),
    (r"Clear-Com", r".*", "intercom"),
    (r"RTS", r".*", "intercom"),
    (r"Riedel", r".*Artist.*|.*Bolero.*|.*MediorNet.*|.*MicroN.*", "intercom"),
    # HDBaseT extenders / AV-over-twisted-pair — no RF, no Dante, just passive AV transport
    (r"Josawa|Lightware|Atlona|Kramer|Extron|Liberty\s*AV", r".*TPUK.*|.*HDBaseT.*|.*TP.*Rx.*|.*TP.*Tx.*|.*DTP.*", "generic"),
    # HP Poly video conferencing — must come before the HP→it_networking catch-all
    (r"HP", r".*G\d+.*|.*Poly.*|.*Engage.*|.*Presence.*|.*Studio.*", "codec"),
    # Dante stageboxes
    (r"Yamaha", r"Rio.*", "dante_stagebox"),
    # Yamaha MY expansion cards — ADAT/AES/analog cards for Yamaha consoles, not Dante
    (r"Yamaha", r"MY.*", "generic"),
    # Meyer Sound passive/powered speakers and subwoofers — not DSPs
    (r"Meyer\s*Sound", r".*", "speaker"),
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
    # NDI devices — use NDI not Dante; never dante_*
    (r"BirdDog", r".*", "generic"),
    # Passive speakers and subwoofers — no network audio
    (r"L-Acoustics", r"SB.*|KS.*|X.*|A.*|ARCS.*|K1.*|K2.*|K3.*|V.*", "generic"),
    # Power amplifiers — analogue signal only
    (r"Behringer", r"NX.*|iNUKE.*|EP.*|KM.*|A500.*", "generic"),
    # USB audio interfaces — USB only, no Dante
    (r"ESI", r"MAYA.*|UGM.*|U46.*|M4U.*|PLANET.*", "generic"),
    (r"Zoom", r"AMS.*|UAC.*|U-.*|H.*|R.*", "generic"),
    # Riedel intercom — uses proprietary Artist/Bolero network, not Dante
    (r"Riedel", r"DCP.*|RSP.*|RCP.*|CCP.*|1200.*|1300.*|2300.*|2400.*|MediorNet.*|Bolero.*", "generic"),
    # LED video processors — video only, no audio network
    (r"Novastar", r".*", "generic"),
    (r"NovaStar", r".*", "generic"),
    # HDMI/HDBaseT extenders — video over IP, no audio Dante
    (r"OREI", r".*", "generic"),
    # Broadcast playout / monitoring
    (r"Domo.*Broadcast", r".*", "generic"),
    (r"EVS", r"CGP.*|XS.*|XT.*|IPD.*", "generic"),
    # Allen & Heath expansion cards — M-AIN is analogue, M-DL-AES* is AES3
    # Only M-DL-DANTE* cards are actual Dante cards
    # DX expanders use dSNAKE protocol, not Dante
    (r"Allen.*Heath", r"DX.*", "generic"),
    (r"Allen.*Heath", r"M-AIN.*|M-DL-AES.*|M-DL-ACE.*|M-DL-MADI.*", "generic"),
    (r"Allen.*Heath", r"M-DL-DANTE.*|M-DL-WAVES.*", "dante_adapter_input"),
    # Yamaha wireless USB receivers — USB dongle, not Dante
    (r"Yamaha", r"URX.*", "generic"),
    # Extron: only "AT" suffix models use Audinate Dante (e.g. "DMP 128 Plus AT")
    # All other Extron models use DTP/HDBaseT/analogue — not Dante
    (r"Extron", r".*\bAT\b.*|.*Dante.*", "dante_adapter_output"),
    (r"Extron", r".*", "generic"),
    # Biamp Tesira uses AVB, not Dante. EX-* expansion modules are small (1-2 ports),
    # so use generic rather than dsp_processor (which requires 4+ ports)
    (r"Biamp", r".*", "generic"),
    # Sonifex AVN series uses Ravenna/AES67, not Dante
    (r"Sonifex", r".*", "generic"),
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
    "CLASS DEFINITIONS:\n"
    "- dante_stagebox: A box whose primary purpose is bridging Audinate Dante network audio to "
    "analogue/AES/MADI, with many I/O channels (e.g. Yamaha Rio, Focusrite RedNet, Luminex LumiNode). "
    "MUST use Audinate Dante specifically — NOT NDI, AVB, Ravenna, AES67, or other AoIP.\n"
    "- dante_adapter_input: A small adapter whose sole job is converting analogue/AES input to Dante "
    "(e.g. Audinate AVIO). MUST use Audinate Dante specifically.\n"
    "- dante_adapter_output: A small adapter whose sole job is converting Dante to analogue/AES output. "
    "MUST use Audinate Dante specifically.\n"
    "- wireless_rx: A wireless microphone or IEM receiver (e.g. Shure ULXD4, Sennheiser EW-DX EM).\n"
    "- mixing_console: A hardware audio mixing console.\n"
    "- dsp_processor: A dedicated DSP signal processor (e.g. QSC Core, Biamp Tesira, BSS Audio).\n"
    "- it_networking: Switches, routers, firewalls, access points, NAS/storage — IT infrastructure.\n"
    "- generic: Everything else — cameras, projectors, displays, speakers, amplifiers, recorders, "
    "intercoms, video converters, NDI devices, AVB devices, AES67 devices, USB audio interfaces, "
    "power amplifiers, passive speakers, or any device you are not certain fits a specific class.\n\n"
    "CRITICAL: Only use dante_stagebox/dante_adapter_input/dante_adapter_output if you are CERTAIN "
    "the device uses Audinate Dante. When in doubt, use 'generic'.\n\n"
    "IMPORTANT: DMX512 (stage lighting control) is NOT Dante and NOT audio networking. "
    "DMX dimmers, relay controllers, and lighting nodes are always 'generic'.\n\n"
    "IMPORTANT: Some manufacturers make both AV and IT products. Classify based on the specific model.\n"
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
