"""HTML source fetcher with JS-detection and Playwright PDF fallback.

When a manufacturer only publishes specs as HTML (no PDF), this module:
1. Fetches the HTML page
2. Detects if it's JS-heavy (React/Vue/Angular, dynamic tabs, etc.)
3. If JS-heavy: renders with Playwright → PDF → submit to Ragscallion as PDF
4. If static: extracts clean Markdown via trafilatura → submit as .md
"""

from __future__ import annotations

import asyncio
import logging
import re
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# Thresholds for quality detection
MIN_EXTRACTED_CHARS = 500
MAX_BOILERPLATE_RATIO = 0.3  # 30% nav/footer terms = suspicious

# Patterns that indicate heavy JS frameworks
JS_FRAMEWORK_PATTERNS = [
    r'react', r'vue\.js', r'angular', r'next\.js', r'nuxt',
    r'data-reactroot', r'ng-', r'v-', r'__nuxt',
]

# Boilerplate terms that suggest extraction failed
BOILERPLATE_TERMS = [
    "cookie policy", "privacy policy", "terms of use", "all rights reserved",
    "sign in", "log in", "register", "shopping cart", "checkout",
    "home", "about us", "contact us", "site map", "search",
]


async def fetch_html(url: str, timeout: int = 30) -> Optional[str]:
    """Fetch raw HTML from a URL."""
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=timeout) as client:
            headers = {"User-Agent": "Mozilla/5.0 (compatible; SignalCanvas/1.0)"}
            r = await client.get(url, headers=headers)
            r.raise_for_status()
            return r.text
    except Exception as e:
        logger.warning(f"Failed to fetch {url}: {e}")
        return None


def detect_js_heavy(html: str) -> bool:
    """Return True if the page appears to be JS-rendered or uses heavy frameworks."""
    html_lower = html.lower()
    for pattern in JS_FRAMEWORK_PATTERNS:
        if re.search(pattern, html_lower):
            logger.info(f"JS framework detected: {pattern}")
            return True

    # Check for very little visible content in raw HTML (suggests JS rendering)
    body_match = re.search(r"<body[^>]*>(.*?)</body>", html, re.DOTALL | re.IGNORECASE)
    if body_match:
        body_text = re.sub(r"<[^>]+>", "", body_match.group(1))
        visible_chars = len(body_text.strip())
        if visible_chars < 200:
            logger.info(f"Very little visible content in raw HTML ({visible_chars} chars) — likely JS-rendered")
            return True

    return False


def extract_with_trafilatura(html: str) -> Optional[str]:
    """Extract main article content from HTML using trafilatura."""
    try:
        import trafilatura
        result = trafilatura.extract(
            html,
            output_format="markdown",
            include_comments=False,
            include_tables=True,
        )
        return result
    except ImportError:
        logger.warning("trafilatura not installed, skipping extraction")
        return None
    except Exception as e:
        logger.warning(f"trafilatura extraction failed: {e}")
        return None


def is_quality_extraction(text: str) -> bool:
    """Check if extracted text looks like real content vs boilerplate."""
    if not text or len(text.strip()) < MIN_EXTRACTED_CHARS:
        logger.info(f"Extraction too short: {len(text or '')} chars")
        return False

    text_lower = text.lower()
    boilerplate_hits = sum(1 for term in BOILERPLATE_TERMS if term in text_lower)
    ratio = boilerplate_hits / len(BOILERPLATE_TERMS)

    if ratio > MAX_BOILERPLATE_RATIO:
        logger.info(f"Too much boilerplate ({ratio:.0%}): likely failed extraction")
        return False

    return True


async def render_to_pdf_with_playwright(url: str, output_path: Path, wait_ms: int = 5000) -> bool:
    """Render a URL with Playwright and save as PDF."""
    try:
        cmd = [
            "npx", "playwright", "pdf",
            "--wait-for-timeout", str(wait_ms),
            url,
            str(output_path),
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=60)
        if proc.returncode != 0:
            err = stderr.decode("utf-8", errors="replace").strip()
            logger.warning(f"Playwright failed: {err}")
            return False
        if output_path.exists() and output_path.stat().st_size > 1000:
            logger.info(f"Playwright PDF saved: {output_path} ({output_path.stat().st_size} bytes)")
            return True
        return False
    except asyncio.TimeoutError:
        logger.warning("Playwright PDF render timed out")
        return False
    except Exception as e:
        logger.warning(f"Playwright PDF render failed: {e}")
        return False


async def fetch_and_prepare_source(
    html_url: str,
    device_id: str,
    cache_dir: Path,
) -> Optional[Path]:
    """Fetch HTML source, decide best format, return path to file ready for Ragscallion.

    Returns:
        Path to .md file (if trafilatura succeeded) or .pdf file (if Playwright fallback)
        None if everything failed
    """
    logger.info(f"Fetching HTML source for {device_id}: {html_url}")

    html = await fetch_html(html_url)
    if not html:
        return None

    # Detect JS-heavy pages
    if detect_js_heavy(html):
        logger.info(f"{device_id}: JS-heavy page detected, falling back to Playwright PDF")
        pdf_path = cache_dir / f"{device_id}__web_rendered.pdf"
        if await render_to_pdf_with_playwright(html_url, pdf_path):
            return pdf_path
        return None

    # Try trafilatura extraction
    extracted = extract_with_trafilatura(html)
    if extracted and is_quality_extraction(extracted):
        md_path = cache_dir / f"{device_id}__web_extracted.md"
        md_path.write_text(extracted, encoding="utf-8")
        logger.info(f"{device_id}: trafilatura extraction succeeded ({len(extracted)} chars)")
        return md_path

    # Extraction was poor — try Playwright PDF fallback
    logger.info(f"{device_id}: trafilatura extraction poor, falling back to Playwright PDF")
    pdf_path = cache_dir / f"{device_id}__web_rendered.pdf"
    if await render_to_pdf_with_playwright(html_url, pdf_path):
        return pdf_path

    return None
