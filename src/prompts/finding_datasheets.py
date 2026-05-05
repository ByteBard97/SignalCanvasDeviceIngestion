"""Prompt-string constants for Stage 1 (find_pdf) datasheet searches.

Each constant provides domain-specific guidance for locating one of the three
document types we collect per device: spec sheet, user manual, and install guide.
They are consumed by `build_doc_search_prompt()` which assembles the full Kimi
search prompt with rejection rules, fallback sources, and strict JSON output
instructions.
"""

SPEC_SHEET_GUIDANCE = """Find the official spec sheet / datasheet / technical data PDF for the device.

Prefer:
- URLs on the manufacturer domain ending in .pdf
- Page titles containing "datasheet", "spec sheet", "specifications", or "technical data"
- Documents with I/O tables, signal-flow diagrams, frequency-response plots, or connector pinouts

Reject:
- Pure marketing PDFs (signals: "brochure", "sell-sheet", glossy product hero with no technical tables)
- Generic distributor catalogs that list many SKUs without per-device specs
- Any document that is clearly a press release or promotional flyer rather than a technical reference
"""

USER_MANUAL_GUIDANCE = """Find the official user manual / user guide / owner's manual / operating manual PDF.

Prefer:
- URLs on the manufacturer domain ending in .pdf
- Page titles containing "user manual", "user guide", "owner's manual", or "operating manual"
- Documents that describe routing, bridge behavior, menu trees, or operational workflows

Reject:
- Quick-start cards, getting-started guides, or one-page setup sheets
- Marketing overviews disguised as manuals
"""

INSTALL_GUIDE_GUIDANCE = """Find the official installation guide / installation manual / installation instructions / rack-mount instructions PDF.

Prefer:
- URLs on the manufacturer domain ending in .pdf
- Page titles containing "installation guide", "installation manual", "installation instructions", or "rack-mount instructions"
- Documents with rack dimensions, power requirements, mounting hole patterns, or physical specs

Note: Not all devices have a dedicated install guide (small portable gear typically does not). It is OK to return null when none exists.
"""

FALLBACK_LADDER = """If the manufacturer site does not serve the PDF directly, try these sources in order:
1. Manufacturer official site (direct .pdf link)
2. AV-iQ (we maintain an AV-iQ index — check there first for pro-audio/broadcast gear)
3. archive.org (web.archive.org/web/*/manufacturer.com/...) for discontinued or moved documents
4. Distributor mirrors (B&H Photo, Sweetwater, Full Compass) for discontinued gear
"""

REJECTION_SIGNALS = """Reject candidates that match any of these signals:
- URL path contains "/marketing/", "/press/", "/news/", "/promo/"
- Filename contains "sell-sheet", "flyer", "press-release", or "catalog" used as a multi-SKU index
- HTTP redirect to a login form or gated download page
- PDF file size less than ~30 KB (likely a landing page or redirect stub)
- Document is a multi-SKU catalog rather than a per-device datasheet

Note: Yamaha and several other manufacturers publish authoritative spec PDFs
under titles like "Brochure & Specifications" — these are NOT marketing-only.
Reject "brochure" only when the document has no technical tables, signal-flow
diagrams, or per-channel I/O specs. A combined "brochure + specifications"
document is a valid spec_sheet.
"""

ANTIBOT_TIPS = """If the manufacturer site requires filling a form, CAPTCHA, or login before download, do NOT attempt to bypass authentication. Instead, look for the same PDF mirrored on a distributor site (B&H, Sweetwater, Full Compass) or on archive.org. Never try to circumvent bot protection or auth walls.
"""


def build_doc_search_prompt(
    manufacturer: str, model: str, doc_type: str, exclude_urls: list[str]
) -> str:
    """Compose a Kimi search prompt for finding a specific document type PDF.

    Args:
        manufacturer: Device manufacturer, e.g. "Shure".
        model: Device model, e.g. "ULXD4Q".
        doc_type: One of "spec_sheet", "user_manual", or "install_guide".
        exclude_urls: List of URLs already tried and should be excluded.

    Returns:
        A fully-formed prompt string with guidance, fallback ladder, rejection
        signals, antibot tips, and strict JSON output instructions.
    """
    if doc_type == "spec_sheet":
        doc_guidance = SPEC_SHEET_GUIDANCE
    elif doc_type == "user_manual":
        doc_guidance = USER_MANUAL_GUIDANCE
    elif doc_type == "install_guide":
        doc_guidance = INSTALL_GUIDE_GUIDANCE
    else:
        raise ValueError(f"Unknown doc_type: {doc_type!r}")

    exclude_block = ""
    if exclude_urls:
        exclude_block = "\nExclude these already-tried URLs:\n" + "\n".join(f"- {u}" for u in exclude_urls) + "\n"

    prompt = (
        f"Find the {doc_type.replace('_', ' ')} PDF for {manufacturer} {model}.\n\n"
        f"{doc_guidance}\n"
        f"{FALLBACK_LADDER}\n"
        f"{REJECTION_SIGNALS}\n"
        f"{ANTIBOT_TIPS}\n"
        f"{exclude_block}\n"
        "Reply with ONLY a JSON object. Two valid shapes:\n"
        '  Found:     {"pdf_url": "https://manufacturer.com/path/datasheet.pdf"}\n'
        '  Not found: {"pdf_url": null}\n'
        "Do not emit the word 'or', the literal string '<url>', markdown fences, "
        "or any prose. Output exactly one of the two JSON objects above."
    )
    return prompt
