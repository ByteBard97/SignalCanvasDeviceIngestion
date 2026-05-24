"""Shared Ragscallion search helper."""

from __future__ import annotations

import logging
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

from src.config import settings as _settings  # noqa: E402

RAGSCALLION_SEARCH_URL = _settings.ragscallion_search_url()
DEFAULT_SEARCH_LIMIT = 3
DEFAULT_SEARCH_TIMEOUT = 30.0


async def search_ragscallion(
    http: httpx.AsyncClient,
    corpus_id: str,
    query: str,
    limit: int = DEFAULT_SEARCH_LIMIT,
    timeout: Optional[float] = None,
) -> list[dict]:
    """Search Ragscallion and return a list of result dicts."""
    params = {
        "q": query,
        "corpus": corpus_id,
        "limit": limit,
    }
    resp = await http.get(
        RAGSCALLION_SEARCH_URL,
        params=params,
        timeout=timeout or DEFAULT_SEARCH_TIMEOUT,
    )
    resp.raise_for_status()
    body = resp.json()
    results = body.get("results", {}).get(corpus_id, [])
    return results
