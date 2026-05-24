"""Stage implementations for device ingestion pipeline.

Each stage is a pure async function that processes a device node and persists results.
Stages use constants for queue IDs and stage codes (no magic numbers).
"""

import asyncio
import os
import json
import logging
import re
import ssl
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .harness.manifest import (
    DeviceNode,
    Manifest,
    FailureCategory,
    QUEUE_1_CANNOT_FIND_PDF,
    QUEUE_2_POLLING_RAGSCALLION,
    QUEUE_3_READY_FOR_EXTRACTION,
    QUEUE_4_MANUAL_REVIEW,
    QUEUE_5_COMPLETED,
    STAGE_FIND_PDF,
    STAGE_DOWNLOAD_PDF,
    STAGE_INDEX_RAG,
    STAGE_EXTRACT_SPECS,
    STAGE_RESOLVE_SKU,
    DOC_TYPE_SPEC_SHEET,
    DOC_TYPE_USER_MANUAL,
    DOC_TYPE_INSTALL_GUIDE,
)
from .config import settings
from .prompts.finding_datasheets import build_doc_search_prompt
from .ragscallion_client import (
    RagscallionClient,
    RagscallionCollisionError,
    RagscallionError,
    RagscallionUnavailableError,
)

logger = logging.getLogger(__name__)

# Stage progress constants (no magic numbers)
STAGE_NOT_STARTED = 0
STAGE_IN_PROGRESS = 1
STAGE_COMPLETED = 2
STAGE_FAILED = 3

# Concurrency control
# EXTRACTION_CONCURRENCY env var allows tuning without code changes.
# Default 3: previously shown to be safe; 5 caused heartbeat death (event loop starvation).
# Increase carefully: monitor for asyncio heartbeat timeouts and Claude API 429s.
MAX_CONCURRENT_EXTRACTIONS = int(os.environ.get("EXTRACTION_CONCURRENCY", "3"))
EXTRACTION_SEMAPHORE = asyncio.Semaphore(MAX_CONCURRENT_EXTRACTIONS)
FIND_PDF_SEMAPHORE = asyncio.Semaphore(3)    # Max 3 concurrent Stage 1 calls
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)    # Max 5 concurrent Stage 2 calls
EXTRACTION_TIMEOUT_SECONDS = 900  # Complex devices (matrix switchers, consoles) need extra time
FIND_PDF_TIMEOUT_SECONDS = 120
FIND_PDF_MAX_STEPS = 5  # tight budget; force fail-fast instead of broad exploration
FIND_PDF_RETRY_ATTEMPTS = 2  # retry on empty Kimi output or model-mismatch URL
PDF_CANDIDATES_PER_SEARCH = 3  # max URLs Stage 1 returns per Kimi call
PDF_KICKBACK_MAX = 3  # max times Stage 2 may kick back to Stage 1 for new URLs
RESOLVE_SKU_TIMEOUT_SECONDS = 180  # generous: alias lookup may need a few searches
RESOLVE_SKU_MAX_STEPS = 8  # more breadth than Stage 1 — alias may map to several SKUs
RESOLVE_SKU_SEMAPHORE = asyncio.Semaphore(3)
DOWNLOAD_TIMEOUT_SECONDS = 60
DOWNLOAD_FALLBACK_ATTEMPTS = 1  # if first URL fails (e.g. bad cert), ask Kimi for one alternate


def _is_ssl_error(exc: Exception) -> bool:
    """Return True if *exc* is an SSL certificate verification failure."""
    msg = str(exc).lower()
    return (
        "certificate verify failed" in msg
        or "ssl" in msg
        or isinstance(exc, ssl.SSLError)
    )
FALLBACK_URL_TIMEOUT_SECONDS = 90
FALLBACK_URL_MAX_STEPS = 5
FALLBACK_URL_KIMI_RETRIES = 2  # the kimi CLI itself occasionally exits non-zero; retry once

# Minimum PDF size to accept (bytes). Anything smaller is likely a redirect
# stub, landing page, or one-page product info sheet.
_MIN_PDF_SIZE_BYTES = 30 * 1024  # 30 KB


def _build_find_pdf_prompt(manufacturer: str, model: str, exclude_urls: list[str]) -> str:
    """Build the Stage 1 Kimi prompt asking for spec sheet + supplementary docs."""
    base = (
        f'Web search for PDFs for the "{manufacturer} {model}". '
        f'Find up to {PDF_CANDIDATES_PER_SEARCH} PDFs: the official datasheet/spec sheet '
        f'MUST be first, then optionally the user manual and/or install guide. '
        f'Return as JSON: {{"docs":[{{"url":"<url>","doc_type":"spec_sheet|user_manual|install_guide"}}]}}. '
        f'First entry must be the spec sheet. Include only real .pdf URLs you found. '
        f'If none found, reply: {{"docs":[]}}. No commentary.'
    )
    if exclude_urls:
        base += (
            f' The following URLs previously failed; do NOT return them: {json.dumps(exclude_urls)}.'
        )
    return base


def _resolved_sku(node: DeviceNode) -> str:
    """Return the canonical SKU if Stage 0 ran, else fall back to the raw model alias.

    Centralizing this avoids every later stage having to know whether resolution happened.
    """
    return node.canonical_sku or node.model


# Lazy-loaded AV-iQ lookup table: {(manufacturer_lower, model_lower): pdf_url}
_av_iq_lookup: dict[tuple[str, str], str] | None = None


def _load_av_iq_lookup() -> dict[tuple[str, str], str]:
    """Load AV-iQ matches from scanner output."""
    global _av_iq_lookup
    if _av_iq_lookup is not None:
        return _av_iq_lookup

    lookup: dict[tuple[str, str], str] = {}
    av_iq_path = Path("output/av_iq_matches.json")
    if av_iq_path.exists():
        try:
            with open(av_iq_path) as f:
                data = json.load(f)
            for match in data.get("matches", []):
                mfg = match.get("manufacturer", "").strip().lower()
                name = match.get("name", "").strip().lower()
                url = match.get("av_iq_url", "").strip()
                if mfg and name and url:
                    lookup[(mfg, name)] = url
            logger.info(f"Loaded {len(lookup)} AV-iQ matches from {av_iq_path}")
        except Exception as e:
            logger.warning(f"Failed to load AV-iQ matches: {e}")
    else:
        logger.info(f"AV-iQ matches file not found at {av_iq_path}")

    _av_iq_lookup = lookup
    return lookup


def _av_iq_url_for_device(manufacturer: str, model: str) -> Optional[str]:
    """Return a known AV-iQ datasheet URL if one exists for this device.

    Matching is fuzzy to handle naming variations:
    - "SHURE" / "Shure" (case)
    - "Mix console XR18" / "XR 18" (extra words, spaces)
    - "SD8 8-channel Stage Box" / "SD8" (subset)
    """
    lookup = _load_av_iq_lookup()
    mfg_key = manufacturer.strip().lower()
    model_key = model.strip().lower()
    model_key_norm = model_key.replace(" ", "")

    # Exact match
    if (mfg_key, model_key) in lookup:
        return lookup[(mfg_key, model_key)]

    # Try each AV-iQ entry
    for (lk_mfg, lk_model), url in lookup.items():
        # Manufacturer must match (case-insensitive)
        if lk_mfg != mfg_key:
            continue
        lk_norm = lk_model.replace(" ", "")
        # Model exact match
        if lk_model == model_key:
            return url
        # Normalized match ("XR 18" vs "XR18")
        if lk_norm == model_key_norm:
            return url
        # Subset match: AV-iQ model contained in manifest model
        # e.g. "SD8" contained in "SD8 8-channel Stage Box"
        if lk_norm in model_key_norm or model_key_norm in lk_norm:
            return url

    return None


# Domains known to serve fake / stub / non-PDF content masquerading as datasheets.
_BLOCKED_PDF_DOMAINS: set[str] = {
    "router-switch.com",
    "www.router-switch.com",
    "manuals.plus",           # Serves generic/manual placeholder pages
    "www.manuals.plus",
    "gearmashers.com",        # SEO aggregator, not manufacturer docs
    "www.gearmashers.com",
}

# Generic filenames that signal a non-device-specific (aggregator) PDF.
_GENERIC_PDF_FILENAMES: set[str] = {
    "speaker.pdf",
    "product.pdf",
    "datasheet.pdf",
    "manual.pdf",
    "brochure.pdf",
    "spec.pdf",
    "specs.pdf",
}


def _is_blocked_domain(url: str) -> bool:
    """Return True if *url* is on the known-bad domain blocklist."""
    try:
        host = urllib.parse.urlparse(url).netloc.lower()
        return host in _BLOCKED_PDF_DOMAINS
    except Exception:
        return False


def _is_generic_av_iq_url(url: str, manufacturer: str, model: str) -> bool:
    """Return True if an AV-iQ URL looks like a generic catch-all, not device-specific.

    AV-iQ sometimes indexes placeholder pages (e.g. ``Speaker.pdf``) that match
    fuzzy heuristics but don't actually describe the requested device.
    """
    path = urllib.parse.urlparse(url).path.lower()
    filename = path.split("/")[-1]

    # Reject known generic filenames
    if filename in _GENERIC_PDF_FILENAMES:
        return True

    # Build a set of tokens to look for in the URL path
    tokens: set[str] = set()
    for part in manufacturer.lower().split():
        if len(part) >= 2:
            tokens.add(part)
    for part in model.lower().replace("/", " ").replace("-", " ").split():
        if len(part) >= 2:
            tokens.add(part)
            # Also add numeric-only substrings for model numbers like "18C"
            if any(c.isdigit() for c in part):
                tokens.add(part)

    # URL must contain at least one recognizable token
    for token in tokens:
        if token in path:
            return False

    # No token matched → URL is likely generic
    return True


async def _search_duckduckgo_for_pdf(
    manufacturer: str,
    model: str,
    rejected_urls: list[str],
) -> Optional[str]:
    """Fallback web search via DuckDuckGo HTML endpoint.

    Searches for ``{manufacturer} {model} pdf`` and returns the first
    candidate URL that:
    - ends in ``.pdf`` (or returns ``application/pdf`` on HEAD)
    - passes ``_url_likely_matches_model``
    - is not in *rejected_urls*

    Returns ``None`` if no valid PDF is found.
    """
    import httpx

    query = f'{manufacturer} {model} pdf manual datasheet'
    search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(search_url, headers=headers)
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"DDG search failed for {manufacturer} {model}: {e}")
        return None

    # DuckDuckGo wraps results in redirect URLs like:
    #   //duckduckgo.com/l/?uddg=<url-encoded-target>&rut=...
    redirect_links = re.findall(
        r'//duckduckgo\.com/l/\?uddg=([^&"\s]+)',
        resp.text,
    )

    candidates: list[str] = []
    for encoded in redirect_links:
        try:
            decoded = urllib.parse.unquote(encoded)
        except Exception:
            continue
        if not decoded.lower().endswith(".pdf"):
            continue
        if decoded in rejected_urls:
            continue
        candidates.append(decoded)

    # Also look for direct .pdf links in the raw HTML
    direct_links = re.findall(
        r'href="(https?://[^"]+\.pdf)"',
        resp.text,
        flags=re.IGNORECASE,
    )
    for link in direct_links:
        if link in rejected_urls or link in candidates:
            continue
        candidates.append(link)

    if not candidates:
        logger.info(f"DDG search for {manufacturer} {model}: no .pdf candidates")
        return None

    logger.info(
        f"DDG search for {manufacturer} {model}: {len(candidates)} candidate(s)"
    )

    for url in candidates:
        # Content-type validation for non-.pdf endings (rare here, but keep parity)
        if not url.lower().endswith(".pdf"):
            is_pdf = await _verify_pdf_content_type(url)
            if not is_pdf:
                logger.debug(f"DDG candidate rejected (non-PDF): {url}")
                continue

        logger.info(f"DDG fallback found PDF: {url}")
        return url

    return None


async def _search_duckduckgo_web(
    manufacturer: str,
    model: str,
) -> list[dict]:
    """General web search via DuckDuckGo HTML endpoint.

    Returns a list of result dicts with keys: title, snippet, url.
    """
    import httpx

    query = f'"{manufacturer}" "{model}"'
    search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(query)}"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            resp = await client.get(search_url, headers=headers)
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"DDG web search failed for {manufacturer} {model}: {e}")
        return []

    results: list[dict] = []

    # DuckDuckGo result titles are in <a class="result__a"> tags
    # Snippets are in <a class="result__snippet"> tags
    title_links = re.findall(
        r'<a[^>]+class="result__a"[^>]*>(.*?)</a>',
        resp.text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    snippet_links = re.findall(
        r'<a[^>]+class="result__snippet"[^>]*>(.*?)</a>',
        resp.text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    # URLs are in the redirect links
    redirect_links = re.findall(
        r'//duckduckgo\.com/l/\?uddg=([^&"\s]+)',
        resp.text,
    )

    for i, title in enumerate(title_links[:5]):
        # Strip HTML tags from title
        clean_title = re.sub(r'<[^>]+>', '', title)
        snippet = snippet_links[i] if i < len(snippet_links) else ""
        clean_snippet = re.sub(r'<[^>]+>', '', snippet)
        url = ""
        if i < len(redirect_links):
            try:
                url = urllib.parse.unquote(redirect_links[i])
            except Exception:
                pass
        results.append({
            "title": clean_title.strip(),
            "snippet": clean_snippet.strip(),
            "url": url,
        })

    return results


async def _attempt_name_correction(
    node: DeviceNode,
    manifest: Manifest,
    rejected_urls: list[str],
) -> bool:
    """Detect and correct swapped/wrong manufacturer/model, then retry PDF search.

    Uses a web search + LLM to determine if the upstream patchify data has
    manufacturer and model backwards, typos, or is completely wrong. If a
    correction is found, updates the node and re-runs Stage 1 with the new name.

    Returns True if a PDF was found after correction.
    """
    from .claude_runner import run_extraction as run_kimi, extract_json_block

    # Only attempt correction for devices that look suspicious or already failed
    # PDF search. Skip obviously-good names.
    original_mfg = node.manufacturer.strip()
    original_model = node.model.strip()

    # Heuristic: skip if both look like real pro-audio names.
    # Well-known manufacturers that should NEVER appear in the model field.
    _WELL_KNOWN_MFGS = {
        "sony", "shure", "sennheiser", "audio-technica", "akg", "beyerdynamic",
        "neumann", "rode", "dbx", "qsc", "yamaha", "mackie", "behringer",
        "allen & heath", "allen_heath", "allen-heath", "avid", "presonus",
        "focusrite", "universal audio", "apogee", "rme", "motu", "tascam",
        "zoom", "blackmagic design", "blackmagic", "panasonic", "canon",
        "jbl", "bose", "ev", "electro-voice", "rcf", "nexo", "d&b",
        "dbaudiotechnik", "l-acoustics", "martin audio", "meyer sound",
        "barco", "christie", "epson", "panasonic", "sony", "nec", "lg",
        "samsung", "extron", "kramer", "crestron", "amx", "biamp",
        "audinate", "dante", "cisco", "netgear", "ubiquiti", "meraki",
        "aruba", "dell", "hp", "lenovo", "apple",
    }
    model_words = set(original_model.lower().split())
    mfg_is_well_known = original_mfg.lower() in _WELL_KNOWN_MFGS
    model_contains_well_known_mfg = bool(model_words & _WELL_KNOWN_MFGS)

    looks_suspicious = (
        original_mfg.lower() in {"generic", "unknown", ""}
        or len(original_model) <= 2
        or original_mfg.lower() in original_model.lower()
        or original_model.lower() in original_mfg.lower()
        or model_contains_well_known_mfg  # e.g. model="Sony" when mfg="Lav K33 RX"
    )
    if not looks_suspicious:
        return False

    logger.info(
        f"Device {node.device_id}: attempting name correction for "
        f"'{original_mfg} {original_model}'"
    )

    # Step 1: do a general web search
    search_results = await _search_duckduckgo_web(original_mfg, original_model)
    if not search_results:
        return False

    # Step 2: ask Kimi to analyze the search results
    results_text = "\n".join(
        f"{i+1}. {r['title']}\n   {r['snippet']}\n   {r['url']}"
        for i, r in enumerate(search_results[:5])
    )

    prompt = (
        f'A device is listed as "{original_mfg} {original_model}". '
        f'Here are the top web search results for this name:\n\n'
        f'{results_text}\n\n'
        f'Analyze these results. Is "{original_mfg} {original_model}" a real '
        f'professional audio/video device (mixer, speaker, camera, converter, etc.)? '
        f'If the manufacturer and model appear to be swapped, misspelled, or wrong, '
        f'reply with ONLY this JSON line:\n'
        f'  {{"manufacturer":"<correct manufacturer>","model":"<correct model>"}}\n'
        f'If the name is already correct and it is a real AV device, reply:\n'
        f'  {{"manufacturer":null,"model":null}}\n'
        f'No commentary.'
    )

    repo_root = Path(__file__).resolve().parent.parent
    skills_dir = repo_root / ".claude" / "skills"

    try:
        stdout = await run_kimi(
            prompt,
            skills_dir=skills_dir,
            work_dir=repo_root,
            timeout=60,
            max_steps=5,
        )
    except Exception as e:
        logger.warning(f"Device {node.device_id} name correction Kimi failed: {e}")
        return False

    if not stdout:
        return False

    json_block = extract_json_block(stdout)
    if not json_block:
        return False

    try:
        data = json.loads(json_block)
    except json.JSONDecodeError:
        return False

    corrected_mfg = data.get("manufacturer")
    corrected_model = data.get("model")

    if not corrected_mfg or not corrected_model:
        logger.info(f"Device {node.device_id}: name correction found no better name")
        return False

    corrected_mfg = corrected_mfg.strip()
    corrected_model = corrected_model.strip()

    # Don't retry if the "correction" is essentially the same
    if (
        corrected_mfg.lower() == original_mfg.lower()
        and corrected_model.lower() == original_model.lower()
    ):
        return False

    logger.info(
        f"Device {node.device_id}: name corrected from "
        f"'{original_mfg} {original_model}' -> '{corrected_mfg} {corrected_model}'"
    )

    # Update node with corrected name
    node.manufacturer = corrected_mfg
    node.model = corrected_model
    # Also update device_id to reflect the corrected name
    new_device_id = f"{corrected_mfg.lower().replace(' ', '-').replace('_', '-')}-{corrected_model.lower().replace(' ', '-').replace('/', '-').replace('_', '-')}"
    new_device_id = "-".join(filter(None, new_device_id.split("-")))[:64]
    if new_device_id != node.device_id:
        logger.info(f"Device {node.device_id}: renamed to {new_device_id}")
        # We can't easily rename the device_id in the manifest because it's the PK.
        # Instead, just update the manufacturer/model and keep the old device_id.
        # The corpus_id should also be updated for Ragscallion compatibility.
        import re
        node.corpus_id = re.sub(r"[^a-z0-9_-]", "_", new_device_id.lower())[:64]
        if node.corpus_id and not re.match(r"^[a-z0-9]", node.corpus_id):
            node.corpus_id = "d" + node.corpus_id[:63]

    manifest.persist(node)

    # Retry PDF search with corrected name (disable further correction to
    # prevent infinite recursion).
    return await _find_spec_sheet_url(node, manifest, _allow_correction=False)


def _build_fallback_url_prompt(
    manufacturer: str,
    sku: str,
    failed_url: str,
    error_summary: str,
    tried_urls: list[str],
) -> str:
    """Build the Stage 2 fallback prompt — request an alternate URL after a download failure.

    Strongly prefers manufacturer-direct hosting over reseller CDN aggregators
    (e.g. av-iq.com, gearank.com), which are the usual source of broken cert chains.
    """
    return (
        f'The PDF URL previously found for "{manufacturer} {sku}" failed to download.\n'
        f'  URL: {failed_url}\n'
        f'  Error: {error_summary}\n\n'
        f'Find an ALTERNATE PDF URL for the same product. STRONGLY PREFER the '
        f'manufacturer\'s own domain (e.g. {manufacturer.lower()}.com or their official '
        f'documentation portal) over reseller, distributor, or aggregator CDNs. '
        f'Do ONE web search and reply with ONLY this JSON line: '
        f'{{"pdf_url":"<url>"}}. If no alternate URL exists, reply '
        f'{{"pdf_url":null}}. Already-tried URLs to avoid: {json.dumps(tried_urls)}. '
        f'No commentary.'
    )


def _looks_canonical_sku(model: str) -> bool:
    """True if model looks like a canonical SKU and Stage 0 can skip Kimi.

    Heuristic: contains a digit + letter, or is short with uppercase letters
    (common for pro-audio gear like "CL1", "ATEM Mini Extreme ISO", "EW 100 G4").
    """
    has_digit = any(c.isdigit() for c in model)
    has_upper = any(c.isupper() for c in model)
    has_letter = any(c.isalpha() for c in model)
    return (has_digit and has_letter) or (has_upper and len(model) <= 30)


def _build_resolve_sku_prompt(manufacturer: str, alias: str) -> str:
    """Build the Stage 0 Kimi prompt — alias to canonical manufacturer SKU."""
    return (
        f'A user typed the device name "{manufacturer} {alias}". This may be the '
        f'canonical manufacturer SKU/part number, OR it may be a colloquial alias '
        f'(e.g. "AVIO-AO2" is shorthand for Audinate\'s "ADP-DAO-AU-0X2", a 2-channel '
        f'Dante AVIO Analog Output adapter).\n\n'
        f'Web search the manufacturer\'s site to identify the canonical SKU and '
        f'product name. Reply with ONLY this JSON line:\n'
        f'  {{"canonical_sku":"<sku>","product_name":"<full product name>","is_alias":<true|false>}}\n'
        f'Set is_alias=false if "{alias}" is already the canonical SKU; set true if you '
        f'had to map it. If you cannot identify the product, reply '
        f'{{"canonical_sku":null,"product_name":null,"is_alias":null}}. '
        f'No commentary.'
    )


async def stage_0_resolve_sku(
    node: DeviceNode,
    manifest: Manifest,
) -> bool:
    """Stage 0: Resolve user-facing alias to canonical manufacturer SKU.

    Input:
        node: Device node with manufacturer + model (model may be a colloquial alias).

    Output:
        True if canonical_sku populated, stage_resolve_sku=COMPLETED.
        False if failure metadata set, moved to QUEUE_1_CANNOT_FIND_PDF.

    Side Effects:
        - Updates node: canonical_sku, canonical_product_name, stage_resolve_sku.
        - Persists node to manifest.

    Idempotent: if canonical_sku is already set, returns True without re-running.
    """
    if node.canonical_sku:
        return True

    # Fast-path: if the model already looks like a canonical SKU, skip the
    # expensive Kimi CLI call.
    if _looks_canonical_sku(node.model):
        node.canonical_sku = node.model
        node.canonical_product_name = node.model
        node.stage_resolve_sku = STAGE_COMPLETED
        manifest.persist(node)
        logger.info(f"Device {node.device_id} fast-path SKU resolution: {node.model!r}")
        return True

    async with RESOLVE_SKU_SEMAPHORE:
        from .claude_runner import run_extraction as run_kimi, extract_json_block

        repo_root = Path(__file__).resolve().parent.parent
        skills_dir = repo_root / ".claude" / "skills"

        prompt = _build_resolve_sku_prompt(node.manufacturer, node.model)

        stdout = None
        for attempt in range(2):
            try:
                stdout = await run_kimi(
                    prompt,
                    skills_dir=skills_dir,
                    work_dir=repo_root,
                    timeout=RESOLVE_SKU_TIMEOUT_SECONDS * (attempt + 1),
                    max_steps=RESOLVE_SKU_MAX_STEPS,
                )
                if stdout:
                    break
            except Exception as e:
                logger.error(
                    f"Device {node.device_id} Stage 0 Kimi invocation failed "
                    f"(attempt {attempt + 1}): {e}"
                )
                stdout = None

        if not stdout:
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_RESOLVE_SKU,
                category=FailureCategory.SKU_RESOLUTION_FAILED,
                message="Kimi CLI returned no output or failed",
                retryable=True,
            )
            return False

        json_block = extract_json_block(stdout)
        if not json_block:
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_RESOLVE_SKU,
                category=FailureCategory.SKU_RESOLUTION_FAILED,
                message=f"Kimi returned unparseable output: {stdout[:500]}",
                retryable=True,
            )
            return False

        try:
            data = json.loads(json_block)
        except json.JSONDecodeError as e:
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_RESOLVE_SKU,
                category=FailureCategory.SKU_RESOLUTION_FAILED,
                message=f"JSON parse error: {e}. Raw: {json_block[:500]}",
                retryable=True,
            )
            return False

        sku = data.get("canonical_sku") if isinstance(data, dict) else None
        product_name = data.get("product_name") if isinstance(data, dict) else None
        if not sku or not isinstance(sku, str):
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_RESOLVE_SKU,
                category=FailureCategory.SKU_RESOLUTION_FAILED,
                message=f"Kimi could not identify canonical SKU. Raw: {json_block[:500]}",
                retryable=True,
            )
            return False

        node.canonical_sku = sku.strip()
        node.canonical_product_name = product_name.strip() if isinstance(product_name, str) else None
        node.stage_resolve_sku = STAGE_COMPLETED
        manifest.persist(node)

        if node.canonical_sku.lower() != node.model.lower():
            logger.info(
                f"Device {node.device_id} alias resolved: "
                f"{node.model!r} -> {node.canonical_sku!r} ({node.canonical_product_name!r})"
            )
        else:
            logger.info(f"Device {node.device_id} input is already canonical SKU: {node.canonical_sku}")
        return True


async def stage_1_find_pdf(
    node: DeviceNode,
    manifest: Manifest,
) -> bool:
    """Stage 1: Find spec sheet (required) plus best-effort secondary docs.

    Spec sheet failure → stage fails. Secondary docs (user_manual,
    install_guide) are best-effort — failures are logged and ignored.
    """
    if not await _find_spec_sheet_url(node, manifest):
        return False
    if settings.find_secondary_docs:
        await _gather_secondary_docs(node, manifest)
    return True


async def _find_spec_sheet_url(
    node: DeviceNode,
    manifest: Manifest,
    _allow_correction: bool = True,
) -> bool:
    """Find canonical manufacturer datasheet PDF URL via Kimi CLI.

    Input:
        node: Device node with manufacturer and model set.

    Output:
        True if pdf_url populated, stage_find_pdf=COMPLETED, queue advanced.
        False if failure metadata set, moved to QUEUE_1_CANNOT_FIND_PDF.

    Processing:
        - Invoke Kimi CLI with a tight prompt asking for the PDF URL.
        - Require JSON response: {"pdf_url": "..."}.
        - Validate URL ends in .pdf or returns application/pdf on HEAD request.
        - Parse with extract_json_block.

    Side Effects:
        - Updates node: pdf_url, stage_find_pdf, queue (on failure)
        - Persists node to manifest
    """
    async with FIND_PDF_SEMAPHORE:
        # Check structured sources first — skip agentic search if we already
        # know the PDF URL.
        av_iq_url = _av_iq_url_for_device(node.manufacturer, node.model)
        if av_iq_url:
            if _is_generic_av_iq_url(av_iq_url, node.manufacturer, node.model):
                logger.warning(
                    f"Device {node.device_id}: AV-iQ URL rejected as generic: {av_iq_url}"
                )
            elif _is_blocked_domain(av_iq_url):
                logger.warning(
                    f"Device {node.device_id}: AV-iQ URL rejected (blocked domain): {av_iq_url}"
                )
            else:
                logger.info(
                    f"Device {node.device_id}: AV-iQ match found, skipping agentic search"
                )
                node.pdf_url = av_iq_url
                node.stage_find_pdf = STAGE_COMPLETED
                manifest.add_document(
                    node.device_id, DOC_TYPE_SPEC_SHEET, url=av_iq_url
                )
                manifest.persist(node)
                return True

        # Seed rejected_urls from: (a) URLs Stage 2 confirmed are not PDFs, and
        # (b) any blocklisted URL currently in node.pdf_url.
        rejected_urls: list[str] = json.loads(node.rejected_pdf_urls or "[]")
        if _is_blocked_domain(node.pdf_url or ""):
            rejected_urls.append(node.pdf_url or "")

        from .kimi_runner import run_kimi, extract_json_block  # PDF search: Kimi first

        repo_root = Path(__file__).resolve().parent.parent
        skills_dir = repo_root / ".claude" / "skills"

        last_failure_message = "Kimi CLI returned no output or failed"
        sku_for_search = _resolved_sku(node)

        for attempt in range(1, FIND_PDF_RETRY_ATTEMPTS + 1):
            prompt = _build_find_pdf_prompt(node.manufacturer, sku_for_search, rejected_urls)

            try:
                stdout = await run_kimi(
                    prompt,
                    skills_dir=skills_dir,
                    work_dir=repo_root,
                    timeout=FIND_PDF_TIMEOUT_SECONDS,
                    max_steps=FIND_PDF_MAX_STEPS,
                )
            except Exception as e:
                logger.error(f"Device {node.device_id} Kimi invocation failed (attempt {attempt}): {e}")
                stdout = None

            if not stdout:
                last_failure_message = f"Kimi CLI returned no output (attempt {attempt})"
                logger.warning(f"Device {node.device_id} Stage 1 attempt {attempt}: empty output")
                continue

            json_block = extract_json_block(stdout)
            if not json_block:
                last_failure_message = f"Kimi returned unparseable output: {stdout[:500]}"
                continue

            try:
                data = json.loads(json_block)
            except json.JSONDecodeError as e:
                last_failure_message = f"JSON parse error: {e}. Raw: {json_block[:500]}"
                continue

            # Parse {"docs": [{"url": "...", "doc_type": "..."}]} (new),
            # {"pdf_urls": [...]} (previous multi-candidate), or
            # {"pdf_url": "..."} (legacy single-URL).
            if not isinstance(data, dict):
                last_failure_message = f"Kimi returned non-dict JSON. Raw: {json_block[:500]}"
                continue

            _VALID_DOC_TYPES = {DOC_TYPE_SPEC_SHEET, DOC_TYPE_USER_MANUAL, DOC_TYPE_INSTALL_GUIDE}
            if "docs" in data:
                raw_docs = data.get("docs") or []
                if not isinstance(raw_docs, list):
                    raw_docs = []
                # Position 0 is always spec_sheet regardless of Kimi's label.
                raw_entries = []
                for i, entry in enumerate(raw_docs):
                    if not isinstance(entry, dict):
                        continue
                    url = entry.get("url", "")
                    if not url or not isinstance(url, str):
                        continue
                    doc_type = entry.get("doc_type", DOC_TYPE_USER_MANUAL)
                    if doc_type not in _VALID_DOC_TYPES:
                        doc_type = DOC_TYPE_USER_MANUAL
                    if i == 0:
                        doc_type = DOC_TYPE_SPEC_SHEET
                    raw_entries.append((url.strip(), doc_type))
            elif "pdf_urls" in data:
                raw_list = data.get("pdf_urls") or []
                raw_entries = [
                    (u.strip(), DOC_TYPE_SPEC_SHEET)
                    for u in raw_list
                    if u and isinstance(u, str)
                ]
            else:
                single = data.get("pdf_url")
                raw_entries = [(single.strip(), DOC_TYPE_SPEC_SHEET)] if single and isinstance(single, str) else []

            # Filter blocked/non-PDF URLs; track spec_sheet separately from supplements.
            spec_sheet_urls: list[str] = []
            supplement_entries: list[tuple[str, str]] = []
            for url, doc_type in raw_entries:
                if _is_blocked_domain(url):
                    rejected_urls.append(url)
                    continue
                if not url.lower().endswith(".pdf"):
                    is_pdf = await _verify_pdf_content_type(url)
                    if not is_pdf:
                        rejected_urls.append(url)
                        continue
                if doc_type == DOC_TYPE_SPEC_SHEET:
                    spec_sheet_urls.append(url)
                else:
                    supplement_entries.append((url, doc_type))

            if not spec_sheet_urls:
                last_failure_message = f"Kimi returned no usable spec sheet URL (attempt {attempt})"
                continue

            # Primary spec sheet (gates queue progression).
            primary_url = spec_sheet_urls[0]
            backup_spec_sheets = spec_sheet_urls[1:]
            node.pdf_url = primary_url
            node.candidate_pdf_urls = json.dumps(backup_spec_sheets) if backup_spec_sheets else None
            node.stage_find_pdf = STAGE_COMPLETED
            manifest.add_document(node.device_id, DOC_TYPE_SPEC_SHEET, url=primary_url)

            # Supplementary docs (user manual, install guide) — best-effort.
            for supp_url, supp_type in supplement_entries:
                manifest.add_document(node.device_id, supp_type, url=supp_url)
                logger.info(f"Device {node.device_id}: queued {supp_type} for download: {supp_url}")

            manifest.persist(node)
            logger.info(
                f"Device {node.device_id} spec sheet found (attempt {attempt}): {primary_url}"
                + (f" + {len(backup_spec_sheets)} spec backup(s)" if backup_spec_sheets else "")
                + (f" + {len(supplement_entries)} supplement(s)" if supplement_entries else "")
            )
            return True

        # Fallback: try DuckDuckGo direct search if Kimi CLI couldn't find anything.
        # Use the human-readable model name, not the canonical SKU — web search
        # engines don't know internal SKUs like SWRCONVRCK2.
        logger.info(
            f"Device {node.device_id} Kimi search exhausted; trying DDG fallback"
        )
        ddg_url = await _search_duckduckgo_for_pdf(
            node.manufacturer, node.model, rejected_urls
        )
        if ddg_url:
            node.pdf_url = ddg_url
            node.stage_find_pdf = STAGE_COMPLETED
            manifest.add_document(
                node.device_id, DOC_TYPE_SPEC_SHEET, url=ddg_url
            )
            manifest.persist(node)
            logger.info(f"Device {node.device_id} PDF URL found via DDG fallback: {ddg_url}")
            return True

        # Before giving up, try to detect and correct bad manufacturer/model data.
        # This handles cases like swapped manufacturer/model or typos in the
        # upstream patchify database.
        if _allow_correction:
            corrected = await _attempt_name_correction(node, manifest, rejected_urls)
            if corrected:
                return True

        _set_stage_failure(
            node,
            manifest,
            stage=STAGE_FIND_PDF,
            category=FailureCategory.PDF_NOT_FOUND,
            message=f"All {FIND_PDF_RETRY_ATTEMPTS} attempts failed. Last: {last_failure_message}",
            retryable=True,
        )
        return False


def _build_find_html_prompt(manufacturer: str, model: str, exclude_urls: list[str]) -> str:
    """Build the Stage 1b Kimi prompt — find an HTML product page when PDF is not available."""
    base = (
        f'Web search for the official manufacturer product page or spec page URL '
        f'of "{manufacturer} {model}". '
        f'Do ONE web search, pick the best result that is an HTML page (NOT a PDF), '
        f'and reply with ONLY this JSON line: {{"html_url":"<url>"}}. '
        f'Prefer pages that contain technical specifications (I/O, connectors, signal flow). '
        f'If no HTML page URL is in the search results, reply: {{"html_url":null}}. '
        f'No commentary.'
    )
    if exclude_urls:
        base += (
            f' The following URLs were already rejected as wrong-device or unreachable; '
            f'do NOT return them: {json.dumps(exclude_urls)}.'
        )
    return base


FIND_HTML_SEMAPHORE = asyncio.Semaphore(3)
FIND_HTML_TIMEOUT_SECONDS = 120
FIND_HTML_RETRY_ATTEMPTS = 2


async def stage_1b_find_html_source(
    node: DeviceNode,
    manifest: Manifest,
    cache_dir: Path,
) -> bool:
    """Stage 1b: When PDF search fails, try to find an HTML product page.

    Uses Kimi to find an HTML spec page, then fetches and prepares the source:
    - Static/simple pages: extracted as Markdown (.md) via trafilatura
    - JS-heavy pages: rendered to PDF via Playwright, then ingested via Marker

    On success, marks Stage 1 and Stage 2 as completed and moves node to
    QUEUE_2_POLLING_RAGSCALLION (bypassing download since source is local).

    Input:
        node: Device node that failed Stage 1 (PDF_NOT_FOUND).
        cache_dir: Directory to cache downloaded/rendered source files.

    Output:
        True if a source file was produced and node advanced to queue_2.
        False if failure metadata set (HTML_SOURCE_NOT_FOUND).
    """
    async with FIND_HTML_SEMAPHORE:
        from .kimi_runner import run_kimi, extract_json_block  # HTML source search: Kimi first
        from .stages.fetch_html_source import fetch_and_prepare_source

        repo_root = Path(__file__).resolve().parent.parent
        skills_dir = repo_root / ".claude" / "skills"
        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)

        rejected_urls: list[str] = []
        last_failure_message = "Kimi CLI returned no output or failed"
        sku_for_search = _resolved_sku(node)

        for attempt in range(1, FIND_HTML_RETRY_ATTEMPTS + 1):
            prompt = _build_find_html_prompt(node.manufacturer, sku_for_search, rejected_urls)

            try:
                stdout = await run_kimi(
                    prompt,
                    skills_dir=skills_dir,
                    work_dir=repo_root,
                    timeout=FIND_HTML_TIMEOUT_SECONDS,
                    max_steps=FIND_PDF_MAX_STEPS,
                )
            except Exception as e:
                logger.error(f"Device {node.device_id} HTML search Kimi failed (attempt {attempt}): {e}")
                stdout = None

            if not stdout:
                last_failure_message = f"Kimi CLI returned no output (attempt {attempt})"
                logger.warning(f"Device {node.device_id} Stage 1b attempt {attempt}: empty output")
                continue

            json_block = extract_json_block(stdout)
            if not json_block:
                last_failure_message = f"Kimi returned unparseable output: {stdout[:500]}"
                continue

            try:
                data = json.loads(json_block)
            except json.JSONDecodeError as e:
                last_failure_message = f"JSON parse error: {e}. Raw: {json_block[:500]}"
                continue

            html_url = data.get("html_url") if isinstance(data, dict) else None
            if not html_url or not isinstance(html_url, str):
                last_failure_message = f"Kimi JSON missing 'html_url' field. Raw: {json_block[:500]}"
                continue

            html_url = html_url.strip()
            if html_url.lower().endswith(".pdf"):
                last_failure_message = f"URL is a PDF, not HTML: {html_url}"
                rejected_urls.append(html_url)
                continue

            # Try to fetch and prepare the source
            logger.info(f"Device {node.device_id}: fetching HTML source from {html_url}")
            try:
                source_path = await fetch_and_prepare_source(
                    html_url=html_url,
                    device_id=node.device_id,
                    cache_dir=cache_dir,
                )
            except Exception as e:
                logger.warning(f"Device {node.device_id}: fetch_and_prepare_source failed: {e}")
                last_failure_message = f"Source fetch failed: {e}"
                rejected_urls.append(html_url)
                continue

            if source_path is None:
                last_failure_message = f"Source fetch returned nothing for {html_url}"
                rejected_urls.append(html_url)
                continue

            # Success! Record the source and advance the node.
            node.pdf_path = str(source_path)  # Field name is a misnomer; it holds the source file
            node.stage_find_pdf = STAGE_COMPLETED
            node.stage_download_pdf = STAGE_COMPLETED  # Skip download — source is already local
            node.queue = QUEUE_2_POLLING_RAGSCALLION

            # Record as a document
            manifest.add_document(
                node.device_id,
                DOC_TYPE_SPEC_SHEET,
                url=html_url,
                local_path=str(source_path),
            )
            manifest.persist(node)

            source_type = "Markdown" if source_path.suffix == ".md" else source_path.suffix
            logger.info(
                f"Device {node.device_id}: HTML source resolved ({source_type}) "
                f"from {html_url} -> {source_path}"
            )
            return True

        _set_stage_failure(
            node,
            manifest,
            stage=8,  # STAGE_FIND_HTML — not in canonical list, handled gracefully
            category=FailureCategory.HTML_SOURCE_NOT_FOUND,
            message=f"All {FIND_HTML_RETRY_ATTEMPTS} HTML search attempts failed. Last: {last_failure_message}",
            retryable=True,
        )
        return False


async def _gather_secondary_docs(node: DeviceNode, manifest: Manifest) -> None:
    """Best-effort search for non-spec-sheet docs in parallel. Never raises."""
    results = await asyncio.gather(
        _find_secondary_doc(node, manifest, DOC_TYPE_USER_MANUAL),
        _find_secondary_doc(node, manifest, DOC_TYPE_INSTALL_GUIDE),
        return_exceptions=True,
    )
    for doc_type, result in zip((DOC_TYPE_USER_MANUAL, DOC_TYPE_INSTALL_GUIDE), results):
        if isinstance(result, Exception):
            logger.warning(
                f"Device {node.device_id}: {doc_type} search raised: {result}"
            )


async def _find_secondary_doc(
    node: DeviceNode,
    manifest: Manifest,
    doc_type: str,
) -> Optional[str]:
    """One Kimi search for a secondary doc type. Records via add_document on hit.

    Single attempt, no retry — secondary docs are best-effort. Returns the URL
    on success or None on any failure (Kimi error, no match, validation reject).
    """
    async with FIND_PDF_SEMAPHORE:
        from .kimi_runner import run_kimi, extract_json_block  # PDF retry: Kimi first

        repo_root = Path(__file__).resolve().parent.parent
        skills_dir = repo_root / ".claude" / "skills"
        sku_for_search = _resolved_sku(node)

        existing_urls = [
            d.url for d in manifest.list_documents(node.device_id, doc_type)
            if d.url
        ]
        prompt = build_doc_search_prompt(
            node.manufacturer, sku_for_search, doc_type, existing_urls
        )

        try:
            stdout = await run_kimi(
                prompt,
                skills_dir=skills_dir,
                work_dir=repo_root,
                timeout=FIND_PDF_TIMEOUT_SECONDS,
                max_steps=FIND_PDF_MAX_STEPS,
            )
        except Exception as e:
            logger.warning(f"Device {node.device_id}: {doc_type} Kimi failed: {e}")
            return None

        if not stdout:
            logger.info(f"Device {node.device_id}: no {doc_type} found (empty)")
            return None

        json_block = extract_json_block(stdout)
        if not json_block:
            logger.info(f"Device {node.device_id}: no {doc_type} found (unparseable)")
            return None

        try:
            data = json.loads(json_block)
        except json.JSONDecodeError:
            logger.info(f"Device {node.device_id}: no {doc_type} found (bad JSON)")
            return None

        pdf_url = data.get("pdf_url") if isinstance(data, dict) else None
        if not pdf_url or not isinstance(pdf_url, str):
            logger.info(f"Device {node.device_id}: no {doc_type} found (null url)")
            return None

        pdf_url = pdf_url.strip()
        if not pdf_url.lower().endswith(".pdf"):
            if not await _verify_pdf_content_type(pdf_url):
                logger.info(
                    f"Device {node.device_id}: {doc_type} URL not a PDF, skipping: {pdf_url}"
                )
                return None

        # Secondary docs intentionally skip the filename-mismatch check.
        # Series-level or generic filenames (e.g. 2000ser_om.pdf) are common
        # for user manuals and install guides, and the Kimi prompt already
        # scopes the search to the correct manufacturer/product family.

        manifest.add_document(node.device_id, doc_type, url=pdf_url)
        logger.info(f"Device {node.device_id}: {doc_type} found: {pdf_url}")
        return pdf_url


async def _download_secondary_docs(
    node: DeviceNode,
    manifest: Manifest,
    cache_dir: Path,
) -> None:
    """Best-effort download of every non-spec-sheet doc recorded for this device.

    Each failure logs a warning and continues — secondary docs never fail Stage 2.
    """
    import httpx

    docs = manifest.list_documents(node.device_id)
    for doc in docs:
        if doc.doc_type == DOC_TYPE_SPEC_SHEET:
            continue
        if not doc.url or doc.local_path:
            continue

        local_path = cache_dir / f"{node.device_id}__{doc.doc_type}.pdf"
        content: bytes | None = None
        headers = _download_headers(
            doc.url, manufacturer=node.manufacturer, model=node.model
        )
        try:
            async with httpx.AsyncClient(
                follow_redirects=True, timeout=DOWNLOAD_TIMEOUT_SECONDS
            ) as client:
                resp = await client.get(doc.url, headers=headers)
                resp.raise_for_status()
                content = resp.content
        except Exception as e:
            if _is_ssl_error(e):
                logger.warning(
                    f"Device {node.device_id}: {doc.doc_type} SSL verify failed, "
                    f"retrying with verify=False"
                )
                try:
                    async with httpx.AsyncClient(
                        follow_redirects=True,
                        timeout=DOWNLOAD_TIMEOUT_SECONDS,
                        verify=False,
                    ) as client:
                        resp = await client.get(doc.url, headers=headers)
                        resp.raise_for_status()
                        content = resp.content
                except Exception as inner:
                    logger.warning(
                        f"Device {node.device_id}: {doc.doc_type} download failed: {inner}"
                    )
                    continue
            else:
                logger.warning(
                    f"Device {node.device_id}: {doc.doc_type} download failed: {e}"
                )
                continue

        if content is None:
            continue
        if len(content) < 4 or content[:4] != b"%PDF":
            logger.warning(
                f"Device {node.device_id}: {doc.doc_type} not a valid PDF, skipping"
            )
            continue
        local_path.write_bytes(content)
        manifest.set_document_local_path(
            node.device_id, doc.doc_type, doc.url, str(local_path)
        )
        logger.info(
            f"Device {node.device_id}: {doc.doc_type} downloaded to {local_path} "
            f"({len(content)} bytes)"
        )


# Shared browser-like headers for requests that need to look like a real user.
# Sec-Fetch-* headers make the request look like it originated from a real
# browser following a link (not a direct fetch by a script).
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Sec-Fetch-User": "?1",
}


def _normalize_pdf_url(url: str) -> str:
    """Rewrite known bad URL patterns to their downloadable equivalents.

    GitHub blob pages return HTML, not PDFs. Transform to raw.githubusercontent.com.
    Pattern: https://github.com/<user>/<repo>/blob/<branch>/<path>
          → https://raw.githubusercontent.com/<user>/<repo>/<branch>/<path>
    """
    import re as _re
    m = _re.match(
        r"https?://github\.com/([^/]+/[^/]+)/blob/(.+)",
        url,
    )
    if m:
        return f"https://raw.githubusercontent.com/{m.group(1)}/{m.group(2)}"
    return url


def _download_headers(url: str, manufacturer: str = "", model: str = "") -> dict[str, str]:
    """Return request headers that look like a real browser following a search result.

    The Referer points to a Google search for the device model instead of a
    generic or self-referential URL, which some bot filters flag.
    """
    query = urllib.parse.quote(f"{manufacturer} {model} datasheet pdf".strip())
    return {
        **_BROWSER_HEADERS,
        "Referer": f"https://www.google.com/search?q={query}",
    }


async def _verify_pdf_content_type(url: str) -> bool:
    """Verify that a URL returns application/pdf via HEAD request."""
    import httpx
    headers = _download_headers(url)
    try:
        async with httpx.AsyncClient(
            follow_redirects=True, timeout=10.0
        ) as client:
            resp = await client.head(url, headers=headers)
            content_type = resp.headers.get("content-type", "").lower()
            return "pdf" in content_type
    except Exception as e:
        if _is_ssl_error(e):
            logger.debug(f"HEAD request SSL failed for {url}, retrying with verify=False")
            try:
                async with httpx.AsyncClient(
                    follow_redirects=True, timeout=10.0, verify=False
                ) as client:
                    resp = await client.head(url, headers=headers)
                    content_type = resp.headers.get("content-type", "").lower()
                    return "pdf" in content_type
            except Exception as inner:
                logger.debug(f"HEAD request failed for {url}: {inner}")
                return False
        logger.debug(f"HEAD request failed for {url}: {e}")
        return False


async def stage_2_download_pdf(
    node: DeviceNode,
    manifest: Manifest,
    *,
    cache_dir: Path,
) -> bool:
    """Stage 2: Download PDF and validate it is a real PDF file.

    Input:
        node: Device node with pdf_url set.
        cache_dir: Directory to save downloaded PDFs.

    Output:
        True if pdf_path set to local file, stage_download_pdf=COMPLETED.
        False if failure metadata set, moved to QUEUE_1_CANNOT_FIND_PDF.

    Processing:
        - Download via httpx.AsyncClient with 60s timeout, follow redirects.
        - Validate first 4 bytes are %PDF.
        - Save as <device_id>.pdf under cache_dir.

    Side Effects:
        - Writes file to cache_dir
        - Updates node: pdf_path, stage_download_pdf, queue (on failure)
        - Persists node to manifest
    """
    async with DOWNLOAD_SEMAPHORE:
        import httpx

        if not node.pdf_url:
            _set_stage_failure(
                node,
                manifest,
                stage=STAGE_DOWNLOAD_PDF,
                category=FailureCategory.PDF_DOWNLOAD_FAILED,
                message="No pdf_url available for download",
                retryable=True,
            )
            return False

        cache_dir = Path(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = cache_dir / f"{node.device_id}.pdf"

        tried_urls: list[str] = []
        current_url = _normalize_pdf_url(node.pdf_url)
        if current_url != node.pdf_url:
            logger.info(f"Device {node.device_id}: URL rewritten {node.pdf_url} → {current_url}")
            node.pdf_url = current_url
        last_error = "no attempts made"

        async def _try_download(url: str) -> bytes:
            """Download PDF bytes, transparently retrying with verify=False on SSL errors.

            Uses a single httpx client so cookies (if any) persist across the
            normal → SSL-fallback retry path.
            """
            import httpx
            headers = _download_headers(
                url, manufacturer=node.manufacturer, model=node.model
            )
            async with httpx.AsyncClient(
                follow_redirects=True,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
            ) as client:
                try:
                    r = await client.get(url, headers=headers)
                    r.raise_for_status()
                    return r.content
                except Exception as e:
                    if _is_ssl_error(e):
                        logger.warning(
                            f"Device {node.device_id} SSL verify failed for {url}, "
                            f"retrying with verify=False"
                        )
                        # httpx does not accept verify= on get(); create a new client
                        async with httpx.AsyncClient(
                            follow_redirects=True,
                            timeout=DOWNLOAD_TIMEOUT_SECONDS,
                            verify=False,
                        ) as ssl_client:
                            r = await ssl_client.get(url, headers=headers)
                            r.raise_for_status()
                            return r.content
                    raise

        for attempt in range(1 + DOWNLOAD_FALLBACK_ATTEMPTS):
            tried_urls.append(current_url)

            try:
                content = await _try_download(current_url)
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                # 403/429/503 are usually bot-protection (Cloudflare, rate limits,
                # temporary unavailability) — not a definitive "no". Treat them
                # like connection errors and try a fallback URL.
                if status in (403, 429, 503):
                    last_error = f"HTTP {status} (bot protection or temporary unavailable)"
                    logger.warning(
                        f"Device {node.device_id} Stage 2 attempt {attempt + 1} "
                        f"download blocked ({last_error}); trying fallback URL"
                        if attempt < DOWNLOAD_FALLBACK_ATTEMPTS
                        else f"Device {node.device_id} Stage 2 attempt {attempt + 1} download blocked ({last_error})"
                    )
                    if attempt < DOWNLOAD_FALLBACK_ATTEMPTS:
                        # First: ask DDG for the next candidate (cheaper & faster than Kimi).
                        # Add the blocked URL to rejected_urls so DDG skips it.
                        tried_urls.append(current_url)
                        ddg_alt = await _search_duckduckgo_for_pdf(
                            node.manufacturer, node.model, tried_urls
                        )
                        if ddg_alt:
                            logger.info(
                                f"Device {node.device_id} DDG next candidate after block: {ddg_alt}"
                            )
                            current_url = ddg_alt
                            continue

                        # Second: ask Kimi for a manufacturer-direct alternate.
                        alt_url = await _request_alternate_pdf_url(
                            manufacturer=node.manufacturer,
                            sku=_resolved_sku(node),
                            failed_url=current_url,
                            error_summary=last_error,
                            tried_urls=tried_urls,
                        )
                        if alt_url:
                            current_url = alt_url
                            continue
                    _set_stage_failure(
                        node,
                        manifest,
                        stage=STAGE_DOWNLOAD_PDF,
                        category=FailureCategory.PDF_DOWNLOAD_FAILED,
                        message=f"HTTP download error after {len(tried_urls)} URL(s): {last_error}",
                        retryable=True,
                    )
                    return False
                # Other 4xx/5xx (404, 410, 500, etc.) are definitive failures.
                _set_stage_failure(
                    node,
                    manifest,
                    stage=STAGE_DOWNLOAD_PDF,
                    category=FailureCategory.PDF_DOWNLOAD_FAILED,
                    message=f"HTTP download error: {e}",
                    retryable=True,
                )
                return False
            except Exception as e:
                # Connection-level failure (SSL cert chain, DNS, timeout, refused).
                # The URL itself is unreachable; an alternate manufacturer-direct URL
                # often works. Try one Kimi fallback before giving up.
                last_error = f"{type(e).__name__}: {e}"
                logger.warning(
                    f"Device {node.device_id} Stage 2 attempt {attempt + 1} download failed "
                    f"({last_error}); trying fallback URL" if attempt < DOWNLOAD_FALLBACK_ATTEMPTS
                    else f"Device {node.device_id} Stage 2 attempt {attempt + 1} download failed ({last_error})"
                )
                if attempt < DOWNLOAD_FALLBACK_ATTEMPTS:
                    alt_url = await _request_alternate_pdf_url(
                        manufacturer=node.manufacturer,
                        sku=_resolved_sku(node),
                        failed_url=current_url,
                        error_summary=last_error,
                        tried_urls=tried_urls,
                    )
                    if alt_url:
                        current_url = alt_url
                        continue
                _set_stage_failure(
                    node,
                    manifest,
                    stage=STAGE_DOWNLOAD_PDF,
                    category=FailureCategory.PDF_DOWNLOAD_FAILED,
                    message=f"HTTP download error after {len(tried_urls)} URL(s): {last_error}",
                    retryable=True,
                )
                return False

            if len(content) < 4 or content[:4] != b"%PDF":
                tried_urls.append(current_url)
                logger.warning(
                    f"Device {node.device_id} URL is not a valid PDF: {current_url}"
                )
                # Persist bad URL to rejected list so Stage 1 won't re-suggest it.
                existing_rejected = json.loads(node.rejected_pdf_urls or "[]")
                if current_url not in existing_rejected:
                    existing_rejected.append(current_url)
                node.rejected_pdf_urls = json.dumps(existing_rejected)

                # Try next candidate before giving up.
                candidates = json.loads(node.candidate_pdf_urls or "[]")
                if candidates:
                    current_url = candidates.pop(0)
                    node.candidate_pdf_urls = json.dumps(candidates) if candidates else None
                    node.pdf_url = current_url
                    manifest.persist(node)
                    logger.info(f"Device {node.device_id} trying next candidate: {current_url}")
                    continue

                # All candidates exhausted — kick back to Stage 1 for a fresh search.
                if node.pdf_kickback_count >= PDF_KICKBACK_MAX:
                    _set_stage_failure(
                        node,
                        manifest,
                        stage=STAGE_DOWNLOAD_PDF,
                        category=FailureCategory.PDF_INVALID,
                        message=(
                            f"All {node.pdf_kickback_count} Stage 1 searches exhausted "
                            f"({len(existing_rejected)} URLs tried, none valid PDFs)"
                        ),
                        retryable=False,
                    )
                    return False

                node.pdf_kickback_count += 1
                node.stage_find_pdf = STAGE_NOT_STARTED
                node.stage_download_pdf = STAGE_NOT_STARTED
                node.pdf_url = None
                node.candidate_pdf_urls = None
                node.failure_category = FailureCategory.PDF_INVALID.value
                node.failure_retryable = True
                node.failure_at = datetime.now(timezone.utc).isoformat()
                manifest.persist(node)
                logger.info(
                    f"Device {node.device_id} kicked back to Stage 1 "
                    f"(kickback #{node.pdf_kickback_count}, "
                    f"{len(existing_rejected)} rejected URL(s))"
                )
                return False

            if len(content) < _MIN_PDF_SIZE_BYTES:
                logger.warning(
                    f"Device {node.device_id} downloaded PDF is only {len(content)} bytes "
                    f"(< {_MIN_PDF_SIZE_BYTES} threshold); treating as stub/redirect. "
                    f"Trying fallback URL."
                )
                last_error = f"PDF too small ({len(content)} bytes)"
                if attempt < DOWNLOAD_FALLBACK_ATTEMPTS:
                    alt_url = await _request_alternate_pdf_url(
                        manufacturer=node.manufacturer,
                        sku=_resolved_sku(node),
                        failed_url=current_url,
                        error_summary=last_error,
                        tried_urls=tried_urls,
                    )
                    if alt_url:
                        current_url = alt_url
                        continue
                _set_stage_failure(
                    node,
                    manifest,
                    stage=STAGE_DOWNLOAD_PDF,
                    category=FailureCategory.PDF_INVALID,
                    message=f"Downloaded PDF too small ({len(content)} bytes) after {len(tried_urls)} URL(s)",
                    retryable=True,
                )
                return False

            # Success: persist whichever URL actually worked (may differ from the original).
            pdf_path.write_bytes(content)
            node.pdf_url = current_url
            node.pdf_path = str(pdf_path)
            node.stage_download_pdf = STAGE_COMPLETED
            # When _request_alternate_pdf_url returned a different URL than
            # the one Stage 1 recorded, no spec_sheet row exists for
            # current_url — UPDATE-by-URL would silently no-op and the row
            # would stay forever without local_path or job_id. Insert-or-
            # ignore first, then update local_path on whichever row matches.
            manifest.add_document(
                node.device_id,
                DOC_TYPE_SPEC_SHEET,
                url=current_url,
                local_path=str(pdf_path),
            )
            manifest.set_document_local_path(
                node.device_id,
                DOC_TYPE_SPEC_SHEET,
                current_url,
                str(pdf_path),
            )
            manifest.persist(node)
            logger.info(
                f"Device {node.device_id} PDF downloaded to {pdf_path} ({len(content)} bytes)"
                + (f" via fallback URL {current_url}" if len(tried_urls) > 1 else "")
            )
            if settings.find_secondary_docs:
                await _download_secondary_docs(node, manifest, cache_dir)
            return True

        # Loop exited without success or explicit failure (shouldn't happen).
        _set_stage_failure(
            node,
            manifest,
            stage=STAGE_DOWNLOAD_PDF,
            category=FailureCategory.PDF_DOWNLOAD_FAILED,
            message=f"Exhausted {len(tried_urls)} URL(s) without success. Last: {last_error}",
            retryable=True,
        )
        return False


async def _request_alternate_pdf_url(
    *,
    manufacturer: str,
    sku: str,
    failed_url: str,
    error_summary: str,
    tried_urls: list[str],
) -> Optional[str]:
    """Ask Kimi for an alternate PDF URL after a download-time connection failure.

    Returns a stripped URL string, or None if Kimi cannot find one or the response
    is malformed. The caller is responsible for re-attempting the download with the
    returned URL — this function does not touch the manifest.
    """
    from .kimi_runner import run_kimi, extract_json_block  # URL fallback search: Kimi first

    repo_root = Path(__file__).resolve().parent.parent
    skills_dir = repo_root / ".claude" / "skills"

    prompt = _build_fallback_url_prompt(
        manufacturer=manufacturer,
        sku=sku,
        failed_url=failed_url,
        error_summary=error_summary,
        tried_urls=tried_urls,
    )

    # Kimi CLI occasionally exits non-zero with empty stderr (transient agentic-loop
    # state). One retry covers it without amplifying cost on real failures.
    stdout = None
    for kimi_attempt in range(1, FALLBACK_URL_KIMI_RETRIES + 1):
        try:
            stdout = await run_kimi(
                prompt,
                skills_dir=skills_dir,
                work_dir=repo_root,
                timeout=FALLBACK_URL_TIMEOUT_SECONDS,
                max_steps=FALLBACK_URL_MAX_STEPS,
            )
        except Exception as e:
            logger.error(f"Stage 2 fallback Kimi invocation failed (attempt {kimi_attempt}): {e}")
            stdout = None
        if stdout:
            break
        if kimi_attempt < FALLBACK_URL_KIMI_RETRIES:
            logger.warning(
                f"Stage 2 fallback Kimi returned empty (attempt {kimi_attempt}); retrying"
            )

    if not stdout:
        return None

    json_block = extract_json_block(stdout)
    if not json_block:
        return None

    try:
        data = json.loads(json_block)
    except json.JSONDecodeError:
        return None

    alt_url = data.get("pdf_url") if isinstance(data, dict) else None
    if not alt_url or not isinstance(alt_url, str):
        return None
    return alt_url.strip()


def _set_stage_failure(
    node: DeviceNode,
    manifest: Manifest,
    *,
    stage: int,
    category: FailureCategory,
    message: str,
    retryable: bool,
) -> None:
    """Helper to set failure metadata and persist node to manifest."""
    node.failure_stage = stage
    node.failure_category = category.value
    node.failure_message = message
    node.failure_retryable = retryable
    node.failure_attempts += 1
    node.failure_at = datetime.now(timezone.utc).isoformat()
    # Stage 0 (RESOLVE_SKU) and Stage 1 (FIND_PDF) / Stage 2 (DOWNLOAD_PDF) are all
    # input-acquisition failures — the user can fix the input and retry. Everything
    # later is a content/processing failure that needs human review.
    if stage in (STAGE_RESOLVE_SKU, STAGE_FIND_PDF, STAGE_DOWNLOAD_PDF):
        node.queue = QUEUE_1_CANNOT_FIND_PDF
    else:
        node.queue = QUEUE_4_MANUAL_REVIEW

    stage_attrs = [
        "stage_find_pdf",
        "stage_download_pdf",
        "stage_convert_marker",
        "stage_index_rag",
        "stage_extract_specs",
        "stage_generate_patch",
        "stage_validate_patch",
        "stage_resolve_sku",  # index 7 = STAGE_RESOLVE_SKU
    ]
    if 0 <= stage < len(stage_attrs):
        setattr(node, stage_attrs[stage], STAGE_FAILED)

    manifest.persist(node)
    logger.warning(f"Device {node.device_id} stage {stage} failed: {category.value} — {message}")


async def stage_3_4_submit_to_ragscallion(
    node: DeviceNode,
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
) -> bool:
    """Submit spec_sheet (required) plus best-effort secondary docs to Ragscallion.

    Spec_sheet failure → device fails (existing semantics: queue_4 + retryable
    metadata). Secondary submission failures → log warning, continue; the
    device still advances on spec_sheet success.

    Output:
        True if spec_sheet successfully submitted (queue_2 polling).
        False if spec_sheet failed (queue_4 manual review).
    """
    # Use sanitized corpus_id (may differ from device_id).
    # Backfill from device_id if missing (legacy rows created before corpus_id
    # was populated).
    corpus_id = node.corpus_id or node.device_id

    # Locate the spec_sheet document row. If missing, fall back to node.pdf_path
    # for back-compat with rows created before device_documents existed.
    spec_docs = manifest.list_documents(node.device_id, DOC_TYPE_SPEC_SHEET)
    spec_doc = next((d for d in spec_docs if d.local_path), None)
    spec_local_path = spec_doc.local_path if spec_doc else node.pdf_path
    spec_label = f"{node.manufacturer} {node.model} ({node.device_id}) [spec_sheet]"

    if spec_local_path is None:
        _set_stage_failure(
            node, manifest,
            stage=STAGE_INDEX_RAG,
            category=FailureCategory.RAGDB_SUBMISSION_ERROR,
            message="spec_local_path is None — PDF not available for submission",
            retryable=True,
        )
        return False

    try:
        # Submit PDF to Ragscallion with retry logic handled by client
        job = await ragscallion_client.submit_ingest(
            pdf_path=spec_local_path,
            corpus_id=corpus_id,
            source_label=spec_label,
            on_conflict="error",  # Reject accidental re-submissions
        )

        # Success: store job metadata.
        # TODO: remove node.ragscallion_job_id once polling_loop reads
        # device_documents exclusively.
        node.ragscallion_job_id = job["job_id"]
        node.corpus_id = corpus_id
        node.ragscallion_submitted_at = datetime.now(timezone.utc).isoformat()
        node.stage_convert_marker = STAGE_IN_PROGRESS
        node.stage_index_rag = STAGE_IN_PROGRESS
        node.queue = QUEUE_2_POLLING_RAGSCALLION

        if spec_doc:
            manifest.set_document_job_id(spec_doc.id, job["job_id"])
        manifest.persist(node)
        logger.info(
            f"Device {node.device_id} spec_sheet submitted to Ragscallion, job_id={job['job_id']}"
        )

        await _submit_secondary_docs(node, ragscallion_client, manifest, corpus_id)
        return True

    except RagscallionCollisionError as e:
        # source_label already exists in corpus → retry with replace
        logger.warning(
            f"Device {node.device_id} collision: {e}. Retrying with on_conflict=replace..."
        )
        try:
            job = await ragscallion_client.submit_ingest(
                pdf_path=spec_local_path,
                corpus_id=corpus_id,
                source_label=spec_label,
                on_conflict="replace",
            )
            node.ragscallion_job_id = job["job_id"]
            node.corpus_id = corpus_id
            node.ragscallion_submitted_at = datetime.now(timezone.utc).isoformat()
            node.stage_convert_marker = STAGE_IN_PROGRESS
            node.stage_index_rag = STAGE_IN_PROGRESS
            node.queue = QUEUE_2_POLLING_RAGSCALLION
            if spec_doc:
                manifest.set_document_job_id(spec_doc.id, job["job_id"])
            manifest.persist(node)
            logger.info(
                f"Device {node.device_id} spec_sheet re-submitted to Ragscallion (replace), job_id={job['job_id']}"
            )
            await _submit_secondary_docs(node, ragscallion_client, manifest, corpus_id)
            return True
        except Exception as retry_e:
            node.failure_stage = STAGE_INDEX_RAG
            node.failure_category = FailureCategory.RAGDB_COLLISION.value
            node.failure_message = f"Collision retry failed: {retry_e}"
            node.failure_retryable = True
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_index_rag = STAGE_FAILED
            manifest.persist(node)
            logger.error(f"Device {node.device_id} collision retry failed: {retry_e}")
            return False

    except RagscallionUnavailableError as e:
        # Ragscallion unavailable after 3 retries
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = FailureCategory.RAGSCALLION_UNAVAILABLE.value
        node.failure_message = f"Failed to submit after 3 retries: {e}"
        node.failure_retryable = True
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission failed: {e}")
        return False

    except RagscallionError as e:
        # Other Ragscallion errors (validation, etc.)
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = FailureCategory.RAGDB_SUBMISSION_ERROR.value
        node.failure_message = str(e)
        node.failure_retryable = True
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission error: {e}")
        return False

    except Exception as e:
        # Catch-all for unexpected errors (network timeouts, DB locks, etc.)
        node.failure_stage = STAGE_INDEX_RAG
        node.failure_category = FailureCategory.RAGDB_SUBMISSION_ERROR.value
        node.failure_message = f"Unexpected error during RAG submission: {type(e).__name__}: {e}"
        node.failure_retryable = True
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} unexpected RAG submission error: {type(e).__name__}: {e}")
        return False


async def _submit_secondary_docs(
    node: DeviceNode,
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
    corpus_id: str,
) -> None:
    """Best-effort submit each non-spec-sheet downloaded doc to Ragscallion.

    Each doc is its own Ragscallion job under the same corpus_id. Failures
    log a warning and continue — the device's queue/stage state is not changed
    here (already advanced by the spec_sheet success path).
    """
    docs = manifest.list_documents(node.device_id)
    for doc in docs:
        if doc.doc_type == DOC_TYPE_SPEC_SHEET:
            continue
        if not doc.local_path or doc.ragscallion_job_id:
            continue

        label = f"{node.manufacturer} {node.model} ({node.device_id}) [{doc.doc_type}]"
        try:
            job = await ragscallion_client.submit_ingest(
                pdf_path=doc.local_path,
                corpus_id=corpus_id,
                source_label=label,
                on_conflict="error",
            )
        except RagscallionCollisionError:
            try:
                job = await ragscallion_client.submit_ingest(
                    pdf_path=doc.local_path,
                    corpus_id=corpus_id,
                    source_label=label,
                    on_conflict="replace",
                )
            except Exception as retry_e:
                logger.warning(
                    f"Device {node.device_id}: {doc.doc_type} replace-submit failed: {retry_e}"
                )
                continue
        except Exception as e:
            logger.warning(
                f"Device {node.device_id}: {doc.doc_type} submit failed: {e}"
            )
            continue

        manifest.set_document_job_id(doc.id, job["job_id"])
        logger.info(
            f"Device {node.device_id}: {doc.doc_type} submitted to Ragscallion, "
            f"job_id={job['job_id']}"
        )


async def stage_5_extract_specs(
    node: DeviceNode,
    ragscallion_client: RagscallionClient,
    manifest: Manifest,
) -> bool:
    """Extract device specs via Haiku agent with RAG search context.

    Uses semaphore to limit concurrent Haiku calls to 5 (I/O-bound, doesn't
    compete with GPU). Reduces extraction time from 30+ hours to ~10 hours
    for 7,000 devices.

    Input:
        node: Device node with corpus_id from stage 3-4
        ragscallion_client: RagscallionClient for RAG corpus search
        manifest: Manifest instance for persistence

    Output:
        True if specs successfully extracted and stored
        False if extraction failed and moved to queue_4 (retryable)

    Processing:
        - Acquire semaphore (max 5 concurrent)
        - Search Ragscallion corpus for device context
        - Call Haiku agent to extract specs from corpus hits
        - Store specs_json, mark stage_extract_specs=COMPLETED
        - On failure: move to queue_4 with EXTRACTION_FAILED

    Side Effects:
        - Updates node: specs_json, stage_extract_specs, queue (on failure)
        - Persists node to manifest
        - Logs extraction result

    Note:
        Anthropic rate limits (4000 RPM) are safe at 5 concurrent.
        If hitting 429s, reduce MAX_CONCURRENT_EXTRACTIONS.
    """
    async with EXTRACTION_SEMAPHORE:
        try:
            # If a prior extraction failed completeness review, feed the concerns
            # back to the agent so it can critique its own work.
            critique_concerns: list[str] | None = None
            if node.specs_json:
                try:
                    prior = json.loads(node.specs_json)
                    critique_concerns = prior.get("_completeness_concerns")
                    if critique_concerns:
                        logger.info(
                            f"Device {node.device_id}: re-extracting with "
                            f"{len(critique_concerns)} completeness concern(s)"
                        )
                except Exception:
                    pass

            from .stages.classify_device import classify as _classify
            _classification = await _classify(node.manufacturer, node.model)
            device_class = _classification.class_

            spec_json = await _extract_specs_via_agent(
                manufacturer=node.manufacturer,
                model=node.model,
                corpus_id=node.corpus_id,
                rag_search=lambda q: ragscallion_client.search(q, corpus=node.corpus_id),
                device_id=node.device_id,
                critique_concerns=critique_concerns,
                device_class=device_class,
            )

            if not spec_json:
                # Extraction failed: mark for retry
                node.failure_stage = STAGE_EXTRACT_SPECS
                node.failure_category = FailureCategory.EXTRACTION_FAILED.value
                node.failure_message = "Agent couldn't extract specs from corpus"
                node.failure_retryable = True
                node.failure_attempts += 1
                node.failure_at = datetime.now(timezone.utc).isoformat()
                node.queue = QUEUE_4_MANUAL_REVIEW
                node.stage_extract_specs = STAGE_FAILED
                manifest.persist(node)
                logger.warning(f"Device {node.device_id} extraction failed")
                return False

            # Success: store specs, mark complete, advance out of queue_3
            # so the runner does not re-pick it up in subsequent Stage 5 batches.
            node.specs_json = spec_json
            node.stage_extract_specs = STAGE_COMPLETED
            node.queue = QUEUE_5_COMPLETED
            manifest.persist(node)
            logger.info(f"Device {node.device_id} specs extracted successfully")
            return True

        except asyncio.TimeoutError:
            # Extraction timeout (per-device EXTRACTION_TIMEOUT_SECONDS)
            node.failure_stage = STAGE_EXTRACT_SPECS
            node.failure_category = FailureCategory.EXTRACTION_TIMEOUT.value
            node.failure_message = f"Extraction timeout after {EXTRACTION_TIMEOUT_SECONDS}s"
            node.failure_retryable = True
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_extract_specs = STAGE_FAILED
            manifest.persist(node)
            logger.error(f"Device {node.device_id} extraction timeout")
            return False

        except Exception as e:
            # Unexpected error: log and move to manual review
            node.failure_stage = STAGE_EXTRACT_SPECS
            node.failure_category = FailureCategory.EXTRACTION_FAILED.value
            node.failure_message = f"Unexpected error: {str(e)}"
            node.failure_retryable = False
            node.failure_attempts += 1
            node.failure_at = datetime.now(timezone.utc).isoformat()
            node.queue = QUEUE_4_MANUAL_REVIEW
            node.stage_extract_specs = STAGE_FAILED
            manifest.persist(node)
            logger.error(f"Device {node.device_id} extraction error: {e}")
            return False


async def _extract_specs_via_agent(
    manufacturer: str,
    model: str,
    corpus_id: str,
    rag_search,
    device_id: str = "",
    critique_concerns: list[str] | None = None,
    device_class: str = "generic",
) -> Optional[str]:
    """Extract device specs via Kimi CLI using RAG context.

    Builds a prompt with device metadata and Ragscallion coordinates, shells out
    to the Kimi CLI (with the device-extraction skill), and parses the resulting
    JSON from stdout.

    Args:
        manufacturer: Device manufacturer (e.g., "YAMAHA")
        model: Device model (e.g., "R08D")
        corpus_id: Ragscallion corpus ID for this device
        rag_search: Callable that searches Ragscallion corpus for context
        device_id: Optional device ID for looking up combined context

    Returns:
        JSON string of extracted specs, or None if extraction failed.
    """
    from .claude_runner import run_extraction_routed, extract_json_block
    from .stages.combined_device_context import get_context_as_prompt_text

    repo_root = Path(__file__).resolve().parent.parent
    skills_dir = repo_root / ".claude" / "skills"

    from .config import settings as _settings
    ragscallion_base_url = _settings.ragscallion_base_url()

    # Fetch combined context from patchify + EasySchematic
    combined_context = ""
    if device_id:
        combined_context = get_context_as_prompt_text(device_id, manufacturer, model)

    prompt = (
        f"You are the SignalCanvas device-extraction agent.\n"
        f"Use the device-extraction skill.\n"
        f"\n"
        f"Inputs:\n"
        f"- manufacturer: {manufacturer}\n"
        f"- model: {model}\n"
        f"- corpus_id: {corpus_id}\n"
        f"- ragscallion_base_url: {ragscallion_base_url}\n"
    )

    if combined_context:
        prompt += (
            f"\n"
            f"{combined_context}\n"
            f"\n"
            f"IMPORTANT: The context above is a STARTING POINT from community data.\n"
            f"It may be WRONG, OUTDATED, or INCOMPLETE. Your job is to:\n"
            f"1. START from the known port list above.\n"
            f"2. ADD any additional ports found in the Ragscallion corpus (datasheet chunks).\n"
            f"3. PRESERVE context ports even if the corpus does not mention them — "
            f"the corpus may be incomplete. Only REMOVE a context port if the corpus "
            f"EXPLICITLY CONTRADICTS it (e.g., says 'this model does NOT have XLR outputs').\n"
            f"4. CORRECT any clear errors in the known ports (wrong connector, wrong direction).\n"
            f"5. DISCOVER bridge/routing information that the context explicitly says is missing.\n"
            f"6. Use the reference URL (if provided) as a starting point for verification.\n"
        )

    if critique_concerns:
        prompt += (
            f"\n"
            f"CRITIQUE — A PREVIOUS EXTRACTION OF THIS DEVICE WAS FLAGGED AS INCOMPLETE:\n"
        )
        for i, concern in enumerate(critique_concerns, 1):
            prompt += f"{i}. {concern}\n"
        prompt += (
            f"\n"
            f"You MUST address every concern above. Re-examine the corpus carefully "
            f"and produce a MORE COMPLETE extraction. Do not simply repeat the previous output.\n"
        )

    prompt += (
        f"\n"
        f"Task:\n"
        f"1. Query the Ragscallion corpus with targeted curl searches.\n"
        f"2. Extract a structured device template.\n"
        f"3. Emit ONLY valid JSON on stdout — no markdown, no explanations.\n"
    )

    stdout = await run_extraction_routed(
        prompt,
        skills_dir=skills_dir,
        work_dir=repo_root,
        device_class=device_class,
        has_critique=bool(critique_concerns),
        timeout=EXTRACTION_TIMEOUT_SECONDS,
    )
    if stdout is None:
        logger.warning(f"Extraction for {manufacturer} {model}: Kimi returned no output")
        return None
    if not stdout.strip():
        logger.warning(f"Extraction for {manufacturer} {model}: Kimi returned empty stdout")
        return None

    json_block = extract_json_block(stdout)
    if not json_block:
        logger.warning(
            f"Extraction for {manufacturer} {model}: no JSON block in stdout. "
            f"First 500 chars: {stdout[:500]!r}"
        )
        return None
    return json_block


async def process_stage_5_batch(
    manifest: Manifest,
    ragscallion_client: RagscallionClient,
) -> dict[str, int]:
    """Process all nodes in queue_3 (ready for extraction) with semaphore limit.

    Batch processes all devices ready for stage 5 extraction in parallel,
    enforcing max 5 concurrent Haiku calls via semaphore.

    Input:
        manifest: Manifest instance with device nodes
        ragscallion_client: RagscallionClient for RAG searches

    Output:
        Dictionary with counts: {successful, failed, exceptions}

    Processing:
        - Get all nodes in queue_3 (ready to extract specs)
        - Create async tasks for all nodes
        - Run with asyncio.gather(..., return_exceptions=True)
        - Semaphore enforces max 5 concurrent extraction calls
        - Log final counts

    Side Effects:
        - Updates all processed nodes in manifest
        - Moves failed nodes to queue_4 (manual review)
        - Logs batch processing summary

    Note:
        At 5 concurrent, with ~20s per extraction:
        - 7,000 devices / 5 concurrent = 1,400 batches
        - 1,400 * 20s = 28,000 seconds = ~7.8 hours
        vs. 7,000 * 20s = 140,000 seconds = ~39 hours serial
    """
    # Get all nodes ready for extraction (queue_3)
    nodes = manifest.list_by_queue(QUEUE_3_READY_FOR_EXTRACTION)
    # Also retry queue_4 nodes that failed extraction but are retryable
    retry_nodes = [
        n for n in manifest.list_by_queue(QUEUE_4_MANUAL_REVIEW)
        if n.stage_extract_specs == STAGE_FAILED
        and n.failure_retryable
        and not n.specs_json  # Skip if we already have specs from a prior success
    ]
    nodes = list({n.device_id: n for n in nodes + retry_nodes}.values())

    if not nodes:
        logger.info("No nodes in queue_3 (ready for extraction)")
        return {"successful": 0, "failed": 0, "exceptions": 0}

    logger.info(
        f"Processing {len(nodes)} nodes for spec extraction "
        f"(semaphore limit: {MAX_CONCURRENT_EXTRACTIONS})"
    )

    # Run all extractions in parallel with semaphore enforcing max 5 concurrent
    tasks = [
        stage_5_extract_specs(node, ragscallion_client, manifest)
        for node in nodes
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Count results
    successful = sum(1 for r in results if r is True)
    failed = sum(1 for r in results if r is False)
    exceptions = sum(1 for r in results if isinstance(r, Exception))

    logger.info(
        f"Stage 5 batch complete: {successful} successful, "
        f"{failed} failed, {exceptions} exceptions"
    )

    return {
        "successful": successful,
        "failed": failed,
        "exceptions": exceptions,
    }
