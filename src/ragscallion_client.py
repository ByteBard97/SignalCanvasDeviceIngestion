"""Async HTTP client for multi-corpus Ragscallion API with retry logic."""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

try:
    import aiohttp
    ASYNC_LIB = "aiohttp"
except ImportError:
    try:
        import httpx
        ASYNC_LIB = "httpx"
    except ImportError:
        ASYNC_LIB = None

logger = logging.getLogger(__name__)

# Retry policy constants (no magic numbers)
BACKOFF_DELAYS = [1, 4, 16]  # seconds: 1s, 4s, 16s for 3 retries
TIMEOUT = 10  # seconds
MAX_RETRIES = 3


class RagscallionError(Exception):
    """Base exception for Ragscallion API errors."""

    pass


class RagscallionCollisionError(RagscallionError):
    """Raised when source_label already exists in corpus (409)."""

    pass


class RagscallionUnavailableError(RagscallionError):
    """Raised after 3 failed submission attempts (5xx, timeout, connection)."""

    pass


class RagscallionClient:
    """Async HTTP client for Ragscallion multi-corpus API.

    Implements retry logic with exponential backoff:
    - 5xx, timeout, connection errors → Retry up to 3 times (1s, 4s, 16s)
    - 4xx (400, 422) → Fail immediately (real errors)
    - 409 collision → Raise RagscallionCollisionError
    - After 3 submission failures → Raise RagscallionUnavailableError
    - After 3 polling failures → Return empty (forgiving for polling)
    """

    def __init__(self, base_url: str = "http://192.168.0.200:8086"):
        """Initialize Ragscallion client.

        Args:
            base_url: Base URL for Ragscallion API (default: Linux box at 8086)
        """
        self.base_url = base_url.rstrip("/")
        self._session = None

    async def _get_session(self):
        """Lazy-load aiohttp session (or httpx)."""
        if self._session is None:
            if ASYNC_LIB == "aiohttp":
                self._session = aiohttp.ClientSession()
            elif ASYNC_LIB == "httpx":
                self._session = httpx.AsyncClient()
            else:
                raise ImportError(
                    "aiohttp or httpx required for async HTTP. "
                    "Install: pip install aiohttp"
                )
        return self._session

    async def close(self):
        """Close HTTP session."""
        if self._session:
            if ASYNC_LIB == "aiohttp":
                await self._session.close()
            elif ASYNC_LIB == "httpx":
                await self._session.aclose()
            self._session = None

    async def __aenter__(self):
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.close()

    async def _retry_request(
        self,
        method: str,
        url: str,
        *,
        fail_fast_on_4xx: bool = True,
        json_body=None,
        data=None,
        **kwargs,
    ) -> dict:
        """Execute HTTP request with exponential backoff retry logic.

        Retry policy:
        - 5xx, timeout, connection error → Retry up to 3 times
        - 4xx (if fail_fast_on_4xx=True) → Fail immediately
        - 409 collision → Raise RagscallionCollisionError immediately

        Args:
            method: HTTP method (GET, POST, etc.)
            url: Full URL
            fail_fast_on_4xx: If True, raise on 4xx errors immediately (no retry)
            json_body: JSON body for POST
            data: Form data for POST (multipart)
            **kwargs: Additional aiohttp/httpx kwargs (headers, params, etc.)

        Returns:
            Parsed JSON response as dict

        Raises:
            RagscallionCollisionError: On 409 collision
            RagscallionError: On 4xx validation errors (if fail_fast_on_4xx=True)
            RagscallionUnavailableError: After 3 retries on 5xx/timeout
        """
        session = await self._get_session()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                if ASYNC_LIB == "aiohttp":
                    async with session.request(
                        method,
                        url,
                        json=json_body,
                        data=data,
                        timeout=aiohttp.ClientTimeout(total=TIMEOUT),
                        **kwargs,
                    ) as resp:
                        # Handle collision (409) immediately
                        if resp.status == 409:
                            body = await resp.json()
                            raise RagscallionCollisionError(
                                f"Collision: {body.get('message', 'source_label already exists')}"
                            )

                        # Fail fast on validation errors (400, 422, etc.)
                        if fail_fast_on_4xx and 400 <= resp.status < 500:
                            body = await resp.json()
                            raise RagscallionError(
                                f"Client error {resp.status}: {body.get('message', body)}"
                            )

                        # Success
                        if 200 <= resp.status < 300:
                            return await resp.json()

                        # Retry on 5xx
                        if resp.status >= 500:
                            if attempt < MAX_RETRIES:
                                delay = BACKOFF_DELAYS[attempt - 1]
                                logger.debug(
                                    f"Attempt {attempt}/{MAX_RETRIES} got {resp.status}. "
                                    f"Retrying in {delay}s..."
                                )
                                await asyncio.sleep(delay)
                                continue
                            else:
                                raise RagscallionUnavailableError(
                                    f"Server returned {resp.status} after {MAX_RETRIES} attempts"
                                )

                elif ASYNC_LIB == "httpx":
                    resp = await session.request(
                        method,
                        url,
                        json=json_body,
                        data=data,
                        timeout=TIMEOUT,
                        **kwargs,
                    )

                    # Handle collision (409) immediately
                    if resp.status_code == 409:
                        body = resp.json()
                        raise RagscallionCollisionError(
                            f"Collision: {body.get('message', 'source_label already exists')}"
                        )

                    # Fail fast on validation errors
                    if fail_fast_on_4xx and 400 <= resp.status_code < 500:
                        body = resp.json()
                        raise RagscallionError(
                            f"Client error {resp.status_code}: {body.get('message', body)}"
                        )

                    # Success
                    if 200 <= resp.status_code < 300:
                        return resp.json()

                    # Retry on 5xx
                    if resp.status_code >= 500:
                        if attempt < MAX_RETRIES:
                            delay = BACKOFF_DELAYS[attempt - 1]
                            logger.debug(
                                f"Attempt {attempt}/{MAX_RETRIES} got {resp.status_code}. "
                                f"Retrying in {delay}s..."
                            )
                            await asyncio.sleep(delay)
                            continue
                        else:
                            raise RagscallionUnavailableError(
                                f"Server returned {resp.status_code} after {MAX_RETRIES} attempts"
                            )

            except (asyncio.TimeoutError, ConnectionError, OSError) as e:
                # Network failure (timeout, connection refused, etc.) — treat as 5xx
                if attempt < MAX_RETRIES:
                    delay = BACKOFF_DELAYS[attempt - 1]
                    logger.debug(
                        f"Attempt {attempt}/{MAX_RETRIES} failed: {type(e).__name__}. "
                        f"Retrying in {delay}s..."
                    )
                    await asyncio.sleep(delay)
                    continue
                else:
                    logger.error(f"Network failure after {MAX_RETRIES} attempts: {e}")
                    raise RagscallionUnavailableError(
                        f"Network failure after {MAX_RETRIES} attempts: {e}"
                    ) from e

            except (RagscallionCollisionError, RagscallionError, RagscallionUnavailableError):
                # Don't retry on our custom exceptions
                raise

        # Should not reach here, but be explicit
        raise RagscallionUnavailableError("Exhausted all retries")

    async def submit_ingest(
        self,
        pdf_path: str,
        corpus_id: str,
        source_label: str,
        on_conflict: str = "error",
    ) -> dict:
        """Submit a source document for async ingestion to Ragscallion.

        Args:
            pdf_path: Local path to source file (.pdf, .md, .txt, .html)
            corpus_id: Corpus identifier (e.g., "yamaha-r08d")
            source_label: Human-readable label (e.g., "YAMAHA R08D")
            on_conflict: "error" (reject collision), "append", or "replace"

        Returns:
            {
                "job_id": "a1b2c3d4-...",
                "corpus_id": "yamaha-r08d",
                "status": "queued",
                "server_now": "2026-04-29T10:30:45.123Z"
            }

        Raises:
            RagscallionCollisionError: 409, source_label exists in corpus
            RagscallionError: 4xx validation error (bad request, etc.)
            RagscallionUnavailableError: After 3 retries on 5xx/timeout
        """
        url = f"{self.base_url}/ingest"
        path = Path(pdf_path)

        # Read file as binary (works for both text and binary formats)
        with open(pdf_path, "rb") as f:
            file_data = f.read()

        # Determine filename and MIME type from actual extension
        filename = path.name
        ext_to_mime = {
            ".pdf": "application/pdf",
            ".md": "text/markdown",
            ".txt": "text/plain",
            ".html": "text/html",
            ".htm": "text/html",
        }
        content_type = ext_to_mime.get(path.suffix.lower(), "application/octet-stream")

        # Prepare multipart form data
        if ASYNC_LIB == "aiohttp":
            import aiohttp

            form = aiohttp.FormData()
            form.add_field("file", file_data, filename=filename, content_type=content_type)
            form.add_field("corpus_id", corpus_id)
            form.add_field("source_label", source_label)
            form.add_field("on_conflict", on_conflict)

            return await self._retry_request(
                "POST",
                url,
                fail_fast_on_4xx=True,
                data=form,
            )

        elif ASYNC_LIB == "httpx":
            files = {
                "file": (filename, file_data, content_type),
                "corpus_id": (None, corpus_id),
                "source_label": (None, source_label),
                "on_conflict": (None, on_conflict),
            }

            return await self._retry_request(
                "POST",
                url,
                fail_fast_on_4xx=True,
                files=files,
            )

    async def poll_jobs(
        self,
        since: str,
        status: str = "ready,failed",
        limit: int = 100,
    ) -> dict:
        """Poll for completed or failed jobs since a given timestamp.

        Args:
            since: ISO timestamp (RFC3339) to query jobs updated after
            status: Comma-separated status filter (default: "ready,failed")
            limit: Max jobs to return (default: 100)

        Returns:
            {
                "jobs": [
                    {
                        "job_id": "...",
                        "corpus_id": "...",
                        "status": "ready" or "failed",
                        "error": "..." (if failed)
                    },
                    ...
                ],
                "server_now": "2026-04-29T10:30:45.123Z"
            }

        On failure (after 3 attempts): returns {"jobs": [], "server_now": datetime.now()}
        (does not raise, is forgiving for polling)
        """
        url = f"{self.base_url}/jobs"

        params = {
            "since": since,
            "status": status,
            "limit": limit,
        }

        try:
            return await self._retry_request(
                "GET",
                url,
                fail_fast_on_4xx=False,  # Forgiving for polling
                params=params,
            )
        except Exception as e:
            # After 3 failures, return empty (forgiving)
            logger.warning(f"Polling failed after {MAX_RETRIES} attempts: {e}")
            return {
                "jobs": [],
                "server_now": datetime.now().isoformat(),
            }

    async def health_check(self) -> bool:
        """Check if Ragscallion API is healthy.

        Returns:
            True if GET /health returns 200, False otherwise
        """
        url = f"{self.base_url}/health"

        try:
            await self._retry_request(
                "GET",
                url,
                fail_fast_on_4xx=False,
            )
            return True
        except Exception:
            return False
