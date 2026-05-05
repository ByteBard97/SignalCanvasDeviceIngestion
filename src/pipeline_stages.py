"""Stage implementations for device ingestion pipeline.

Each stage is a pure async function that processes a device node and persists results.
Stages use constants for queue IDs and stage codes (no magic numbers).
"""

import asyncio
import json
import logging
import re
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
)
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
EXTRACTION_SEMAPHORE = asyncio.Semaphore(5)  # Max 5 concurrent Haiku calls
FIND_PDF_SEMAPHORE = asyncio.Semaphore(3)    # Max 3 concurrent Stage 1 calls
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(5)    # Max 5 concurrent Stage 2 calls

# Concurrency constants (no magic numbers)
MAX_CONCURRENT_EXTRACTIONS = 5
EXTRACTION_TIMEOUT_SECONDS = 360  # Consoles (e.g. CL5, SQ-5) need extra search time
FIND_PDF_TIMEOUT_SECONDS = 120
FIND_PDF_MAX_STEPS = 5  # tight budget; force fail-fast instead of broad exploration
FIND_PDF_RETRY_ATTEMPTS = 2  # retry on empty Kimi output or model-mismatch URL
RESOLVE_SKU_TIMEOUT_SECONDS = 180  # generous: alias lookup may need a few searches
RESOLVE_SKU_MAX_STEPS = 8  # more breadth than Stage 1 — alias may map to several SKUs
RESOLVE_SKU_SEMAPHORE = asyncio.Semaphore(3)
DOWNLOAD_TIMEOUT_SECONDS = 60
DOWNLOAD_FALLBACK_ATTEMPTS = 1  # if first URL fails (e.g. bad cert), ask Kimi for one alternate
FALLBACK_URL_TIMEOUT_SECONDS = 90
FALLBACK_URL_MAX_STEPS = 5
FALLBACK_URL_KIMI_RETRIES = 2  # the kimi CLI itself occasionally exits non-zero; retry once

# URL relevance heuristic constants
_OPAQUE_SLUG_MIN_LEN = 12  # below this, treat slug as a product code, not a hash
_OPAQUE_SLUG_HEX_RATIO = 0.7  # ratio of hex chars above which slug is "opaque"
_MODEL_TOKEN_MIN_LEN = 2  # ignore single-character model tokens like "5" in "SQ-5"


def _model_tokens(model: str) -> list[str]:
    """Split a model string into significant tokens.

    Splits on non-alphanumeric punctuation AND on letter/digit boundaries, so
    'ULXD4' yields ['ULXD', '4'] (matching shure.com/view/guide/ULXD/...) and
    'Rio1608-D2' yields ['Rio', '1608', 'D', '2'].
    Tokens shorter than _MODEL_TOKEN_MIN_LEN are dropped.
    """
    import re
    raw = re.split(r"[^A-Za-z0-9]+", model)
    tokens: list[str] = []
    for chunk in raw:
        # Keep the chunk itself as a token (handles short codes like H6, X32)
        if len(chunk) >= _MODEL_TOKEN_MIN_LEN:
            tokens.append(chunk)
        # Also split letter→digit and digit→letter transitions
        for sub in re.findall(r"[A-Za-z]+|[0-9]+", chunk):
            tokens.append(sub)
    result = [t for t in tokens if len(t) >= _MODEL_TOKEN_MIN_LEN]
    # Fallback: if nothing survived, try the stripped model name as one token
    if not result:
        stripped = re.sub(r"[^A-Za-z0-9]", "", model)
        if stripped:
            result.append(stripped)
    return result


def _looks_opaque(slug: str) -> bool:
    """A slug like '1078f753f2bbafa663cc873b1299a43e1fd6' carries no product hint.

    Treat as opaque only when long enough to plausibly be a content hash; short
    slugs like 'adp' are product codes, not hashes, and must be checked.
    """
    if len(slug) < _OPAQUE_SLUG_MIN_LEN:
        return False
    hex_count = sum(1 for c in slug.lower() if c in "0123456789abcdef")
    return hex_count / len(slug) > _OPAQUE_SLUG_HEX_RATIO


def _url_likely_matches_model(url: str, model: str) -> bool:
    """Return False when the URL filename looks like a product code unrelated to the model.

    Accepts the URL when:
      - any model token appears in the URL path, or
      - the filename is opaque (CDN content hash) — we can't tell from the URL.

    Rejects only when the filename is name-like but contains no model token,
    e.g. AVIO-AO2 vs '.../dataSheet/ADP.pdf'.
    """
    from urllib.parse import urlparse
    path = urlparse(url).path.lower()
    slug = path.rsplit("/", 1)[-1]
    if slug.endswith(".pdf"):
        slug = slug[:-4]
    tokens = [t.lower() for t in _model_tokens(model)]
    if not tokens:
        return True
    if any(t in path for t in tokens):
        return True
    if _looks_opaque(slug):
        return True
    return False


def _build_find_pdf_prompt(manufacturer: str, model: str, exclude_urls: list[str]) -> str:
    """Build the Stage 1 Kimi prompt, optionally excluding URLs from a previous attempt."""
    base = (
        f'Web search for the official manufacturer datasheet PDF URL of '
        f'"{manufacturer} {model}". '
        f'Do ONE web search, pick the best result that ends in .pdf, and reply '
        f'with ONLY this JSON line: {{"pdf_url":"<url>"}}. '
        f'If no PDF URL is in the search results, reply: {{"pdf_url":null}}. '
        f'No commentary.'
    )
    if exclude_urls:
        base += (
            f' The following URLs were already rejected as wrong-device or unreachable; '
            f'do NOT return them: {json.dumps(exclude_urls)}.'
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

    query = f'{manufacturer} {model} pdf'
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

        if not _url_likely_matches_model(url, model):
            logger.debug(f"DDG candidate rejected (model mismatch): {url}")
            continue

        logger.info(f"DDG fallback found valid PDF: {url}")
        return url

    return None


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
    # expensive Kimi CLI call. Heuristic: contains a digit + letter, or is
    # short with uppercase letters (common for pro-audio gear like "CL1",
    # "ATEM Mini Extreme ISO", "EW 100 G4").
    model = node.model
    has_digit = any(c.isdigit() for c in model)
    has_upper = any(c.isupper() for c in model)
    has_letter = any(c.isalpha() for c in model)
    if (has_digit and has_letter) or (has_upper and len(model) <= 30):
        node.canonical_sku = model
        node.canonical_product_name = model
        node.stage_resolve_sku = STAGE_COMPLETED
        manifest.persist(node)
        logger.info(f"Device {node.device_id} fast-path SKU resolution: {model!r}")
        return True

    async with RESOLVE_SKU_SEMAPHORE:
        from .kimi_runner import run_kimi, extract_json_block

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
    """Stage 1: Find canonical manufacturer datasheet PDF URL via Kimi CLI.

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

        from .kimi_runner import run_kimi, extract_json_block

        repo_root = Path(__file__).resolve().parent.parent
        skills_dir = repo_root / ".claude" / "skills"

        rejected_urls: list[str] = []
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

            pdf_url = data.get("pdf_url") if isinstance(data, dict) else None
            if not pdf_url or not isinstance(pdf_url, str):
                last_failure_message = f"Kimi JSON missing 'pdf_url' field. Raw: {json_block[:500]}"
                continue

            pdf_url = pdf_url.strip()
            if not pdf_url.lower().endswith(".pdf"):
                is_pdf = await _verify_pdf_content_type(pdf_url)
                if not is_pdf:
                    last_failure_message = f"URL does not end in .pdf and HEAD returned non-PDF: {pdf_url}"
                    rejected_urls.append(pdf_url)
                    continue

            # Validate URL against both SKU and model name. Some manufacturers
            # use internal SKUs (e.g. SWRCONVRCK2) that never appear in PDF
            # filenames; the model name (e.g. "ATEM Studio Converter 2") is
            # what actually shows up in URLs.
            if not _url_likely_matches_model(
                pdf_url, sku_for_search
            ) and not _url_likely_matches_model(pdf_url, node.model):
                last_failure_message = (
                    f"URL filename does not match model {sku_for_search}: {pdf_url}"
                )
                logger.warning(
                    f"Device {node.device_id} Stage 1 attempt {attempt}: rejecting wrong-device URL {pdf_url}"
                )
                rejected_urls.append(pdf_url)
                continue

            node.pdf_url = pdf_url
            node.stage_find_pdf = STAGE_COMPLETED
            manifest.add_document(
                node.device_id, DOC_TYPE_SPEC_SHEET, url=pdf_url
            )
            manifest.persist(node)
            logger.info(f"Device {node.device_id} PDF URL found (attempt {attempt}): {pdf_url}")
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

        _set_stage_failure(
            node,
            manifest,
            stage=STAGE_FIND_PDF,
            category=FailureCategory.PDF_NOT_FOUND,
            message=f"All {FIND_PDF_RETRY_ATTEMPTS} attempts failed. Last: {last_failure_message}",
            retryable=True,
        )
        return False


# Shared browser-like headers for requests that need to look like a real user
_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,*/*;q=0.9",
    "Accept-Language": "en-US,en;q=0.9",
}


async def _verify_pdf_content_type(url: str) -> bool:
    """Verify that a URL returns application/pdf via HEAD request."""
    try:
        import httpx
        async with httpx.AsyncClient(follow_redirects=True, timeout=10.0) as client:
            resp = await client.head(url, headers=_BROWSER_HEADERS)
            content_type = resp.headers.get("content-type", "").lower()
            return "pdf" in content_type
    except Exception as e:
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
        current_url = node.pdf_url
        last_error = "no attempts made"

        for attempt in range(1 + DOWNLOAD_FALLBACK_ATTEMPTS):
            tried_urls.append(current_url)

            try:
                async with httpx.AsyncClient(
                    follow_redirects=True, timeout=DOWNLOAD_TIMEOUT_SECONDS
                ) as client:
                    resp = await client.get(
                        current_url,
                        headers={
                            **_BROWSER_HEADERS,
                            "Referer": f"https://www.google.com/search?q={urllib.parse.quote(current_url)}",
                        },
                    )
                    resp.raise_for_status()
            except httpx.HTTPStatusError as e:
                # Server reached, returned 4xx/5xx. Don't waste a fallback on this —
                # an HTTP error code is a definitive "no" from the server, not
                # an infrastructure issue Kimi can route around.
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

            content = resp.content
            if len(content) < 4 or content[:4] != b"%PDF":
                _set_stage_failure(
                    node,
                    manifest,
                    stage=STAGE_DOWNLOAD_PDF,
                    category=FailureCategory.PDF_INVALID,
                    message="Downloaded file is not a valid PDF (first 4 bytes != %PDF)",
                    retryable=False,
                )
                return False

            # Success: persist whichever URL actually worked (may differ from the original).
            pdf_path.write_bytes(content)
            node.pdf_url = current_url
            node.pdf_path = str(pdf_path)
            node.stage_download_pdf = STAGE_COMPLETED
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
    from .kimi_runner import run_kimi, extract_json_block

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
    """Submit PDF to Ragscallion for indexing. Retry up to 3 times on transient failures.

    Input:
        node: Device node with pdf_path from stage 2
        ragscallion_client: RagscallionClient instance (handles retries)
        manifest: Manifest instance for persistence

    Output:
        True if successfully submitted to queue_2 (polling)
        False if moved to queue_4 (manual review) due to collision or failure

    Processing:
        - Submit PDF with corpus_id (device_id) and source_label (manufacturer model)
        - Success: store job metadata, move to queue_2 for polling
        - Collision (409): move to queue_4 with RAGDB_COLLISION category
        - Transient failures: RagscallionClient retries with backoff
        - After 3 failures: move to queue_4 for manual review

    Side Effects:
        - Updates node: ragscallion_job_id, corpus_id, ragscallion_submitted_at, stage markers, queue
        - Persists node to manifest
        - Logs submission result
    """
    corpus_id = node.corpus_id  # Use sanitized corpus_id (may differ from device_id)
    source_label = f"{node.manufacturer} {node.model} ({node.device_id})"

    try:
        # Submit PDF to Ragscallion with retry logic handled by client
        job = await ragscallion_client.submit_ingest(
            pdf_path=node.pdf_path,
            corpus_id=corpus_id,
            source_label=source_label,
            on_conflict="error",  # Reject accidental re-submissions
        )

        # Success: store job metadata
        node.ragscallion_job_id = job["job_id"]
        node.corpus_id = corpus_id
        node.ragscallion_submitted_at = datetime.now(timezone.utc).isoformat()
        node.stage_convert_marker = STAGE_IN_PROGRESS
        node.stage_index_rag = STAGE_IN_PROGRESS
        node.queue = QUEUE_2_POLLING_RAGSCALLION

        manifest.persist(node)
        logger.info(
            f"Device {node.device_id} submitted to Ragscallion, job_id={job['job_id']}"
        )
        return True

    except RagscallionCollisionError as e:
        # source_label already exists in corpus → retry with replace
        logger.warning(
            f"Device {node.device_id} collision: {e}. Retrying with on_conflict=replace..."
        )
        try:
            job = await ragscallion_client.submit_ingest(
                pdf_path=node.pdf_path,
                corpus_id=corpus_id,
                source_label=source_label,
                on_conflict="replace",
            )
            node.ragscallion_job_id = job["job_id"]
            node.corpus_id = corpus_id
            node.ragscallion_submitted_at = datetime.now(timezone.utc).isoformat()
            node.stage_convert_marker = STAGE_IN_PROGRESS
            node.stage_index_rag = STAGE_IN_PROGRESS
            node.queue = QUEUE_2_POLLING_RAGSCALLION
            manifest.persist(node)
            logger.info(
                f"Device {node.device_id} re-submitted to Ragscallion (replace), job_id={job['job_id']}"
            )
            return True
        except Exception as retry_e:
            node.failure_stage = STAGE_INDEX_RAG
            node.failure_category = FailureCategory.RAGDB_COLLISION.value
            node.failure_message = f"Collision retry failed: {retry_e}"
            node.failure_retryable = False
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
        node.failure_retryable = False
        node.failure_attempts += 1
        node.failure_at = datetime.now(timezone.utc).isoformat()
        node.queue = QUEUE_4_MANUAL_REVIEW
        node.stage_index_rag = STAGE_FAILED
        manifest.persist(node)
        logger.error(f"Device {node.device_id} submission error: {e}")
        return False


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
            # Perform RAG search for device context
            # TODO: Implement RAG search and Haiku agent call
            # For now, placeholder for extraction logic
            spec_json = await _extract_specs_via_agent(
                manufacturer=node.manufacturer,
                model=node.model,
                corpus_id=node.corpus_id,
                rag_search=lambda q: ragscallion_client.search(q, corpus=node.corpus_id),
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

    Returns:
        JSON string of extracted specs, or None if extraction failed.
    """
    from .kimi_runner import run_kimi, extract_json_block

    repo_root = Path(__file__).resolve().parent.parent
    skills_dir = repo_root / ".claude" / "skills"

    ragscallion_base_url = "http://192.168.0.200:8086"

    prompt = (
        f"You are the SignalCanvas device-extraction agent.\n"
        f"Use the device-extraction skill.\n"
        f"\n"
        f"Inputs:\n"
        f"- manufacturer: {manufacturer}\n"
        f"- model: {model}\n"
        f"- corpus_id: {corpus_id}\n"
        f"- ragscallion_base_url: {ragscallion_base_url}\n"
        f"\n"
        f"Task:\n"
        f"1. Query the Ragscallion corpus with targeted curl searches.\n"
        f"2. Extract a structured device template.\n"
        f"3. Emit ONLY valid JSON on stdout — no markdown, no explanations.\n"
    )

    stdout = await run_kimi(
        prompt,
        skills_dir=skills_dir,
        work_dir=repo_root,
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
        if n.stage_extract_specs == STAGE_FAILED and n.failure_retryable
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
