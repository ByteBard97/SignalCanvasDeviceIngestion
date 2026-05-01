"""Stage 7: Validate PatchLang source using patchlang_python."""

from __future__ import annotations

import json


def validate_patch(patch_source: str) -> tuple[bool, list[str]]:
    """Validate a PatchLang source string.

    Args:
        patch_source: The PatchLang source to validate.

    Returns:
        A tuple of (is_valid, error_messages).
        is_valid is True when there are no parser errors and no
        diagnostics with severity ``error``.
    """
    try:
        import patchlang_python
    except ImportError as exc:
        return False, [f"patchlang_python not available: {exc}"]

    try:
        result = json.loads(patchlang_python.check(patch_source))
    except json.JSONDecodeError as exc:
        return False, [f"Invalid JSON from patchlang_python.check(): {exc}"]
    except Exception as exc:
        return False, [f"patchlang_python.check() failed: {exc}"]

    errors: list[str] = []

    for err in result.get("errors") or []:
        msg = err.get("message", "")
        span = err.get("span", {})
        start = span.get("start", "?")
        end = span.get("end", "?")
        errors.append(f"Parse error at {start}-{end}: {msg}")

    for diag in result.get("diagnostics") or []:
        if diag.get("severity") == "error":
            msg = diag.get("message", "")
            span = diag.get("span", {})
            start = span.get("start", "?")
            end = span.get("end", "?")
            errors.append(f"Diagnostic error at {start}-{end}: {msg}")

    return len(errors) == 0, errors
