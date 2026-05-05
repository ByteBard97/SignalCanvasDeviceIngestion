"""Moonshot (Kimi) chat-completion client with token-usage capture.

Why this exists separately from kimi_runner.py:
    kimi_runner runs the `kimi` CLI subprocess for stages that need its agentic
    web-search loop (Stage 0, Stage 1, Stage 2 fallback). The CLI does not
    surface per-call token counts to the caller. For stages that don't need
    agentic search (Stage 5, bridge inference, Stage 6), we want direct
    chat-completion calls so we can read .usage off the response and log
    real token consumption.

    Moonshot exposes an OpenAI-compatible chat-completion endpoint at
    https://api.moonshot.ai/v1, so we use the openai SDK with a custom
    base_url. This avoids hand-rolling HTTP requests and gets us streaming,
    retries, and async support for free.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx
from openai import AsyncOpenAI

# Explicitly load .env so MOONSHOT_API_KEY is available regardless of how
# this module is imported (direct, via runner, in background tasks, etc.)
try:
    from dotenv import load_dotenv
    _repo_root = Path(__file__).resolve().parent.parent
    load_dotenv(_repo_root / ".env", override=False)
except Exception:
    pass

logger = logging.getLogger(__name__)

# Moonshot API conventions
MOONSHOT_BASE_URL = "https://api.moonshot.ai/v1"
DEFAULT_MODEL = "moonshot-v1-auto"  # auto-tiers by context window (8K / 32K / 128K)
# Other usable IDs on the account (verified from /v1/models):
#   moonshot-v1-8k / -32k / -128k    — explicit context tier
#   kimi-k2.5                        — flagship K2.5 (Jan 2026), $0.44/$2.00 per MTok
#   kimi-k2.6                        — flagship K2.6 (latest), $0.75/$3.50 per MTok


@dataclass(frozen=True)
class UsageRecord:
    """One chat-completion call's token usage and timing.

    Fields mirror what Moonshot returns under response.usage, plus client-side
    timing so we can correlate cost with latency. Output of every chat call;
    typically logged to manifest.usage_log for retrospective cost analysis.
    """

    model: str
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    elapsed_ms: int


class MoonshotClient:
    """Thin async wrapper over the openai SDK pointed at Moonshot's endpoint."""

    def __init__(self, api_key: Optional[str] = None, base_url: str = MOONSHOT_BASE_URL):
        key = api_key or os.environ.get("MOONSHOT_API_KEY")
        if not key:
            # Fallback to pydantic-settings (loads .env automatically)
            try:
                from .config import settings
                key = settings.moonshot_api_key
            except Exception:
                pass
        if not key:
            raise RuntimeError(
                "MOONSHOT_API_KEY not set. Either export it in your shell, put it in "
                "a .env loaded by python-dotenv, or pass api_key= to MoonshotClient()."
            )
        self._key = key
        self._base_url = base_url.rstrip("/")
        self._client = AsyncOpenAI(api_key=key, base_url=base_url)
        # Separate httpx client for Moonshot-specific endpoints (balance, token estimate)
        # that don't fit OpenAI's response shape.
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            headers={"Authorization": f"Bearer {key}"},
            timeout=30.0,
        )

    async def chat_completion(
        self,
        prompt: str,
        *,
        model: str = DEFAULT_MODEL,
        system: Optional[str] = None,
        temperature: float = 0.0,
        max_tokens: Optional[int] = None,
        response_format_json: bool = False,
        seed: Optional[int] = None,
    ) -> tuple[str, UsageRecord]:
        """Send a single-turn chat completion and return (text, usage).

        This is single-turn by design — for agentic web-search loops use the
        kimi CLI via kimi_runner.run_kimi() instead.
        """
        messages: list[dict] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        kwargs: dict = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if max_tokens is not None:
            kwargs["max_tokens"] = max_tokens
        if response_format_json:
            # Moonshot supports OpenAI-style JSON mode for structured output.
            kwargs["response_format"] = {"type": "json_object"}
        if seed is not None:
            kwargs["seed"] = seed

        start = time.monotonic()
        resp = await self._client.chat.completions.create(**kwargs)
        elapsed_ms = int((time.monotonic() - start) * 1000)

        text = resp.choices[0].message.content or ""
        usage = UsageRecord(
            model=resp.model,
            prompt_tokens=resp.usage.prompt_tokens,
            completion_tokens=resp.usage.completion_tokens,
            total_tokens=resp.usage.total_tokens,
            elapsed_ms=elapsed_ms,
        )
        logger.debug(
            f"Moonshot {model}: in={usage.prompt_tokens} out={usage.completion_tokens} "
            f"({elapsed_ms}ms)"
        )
        return text, usage

    async def close(self) -> None:
        await self._client.close()
        await self._http.aclose()

    async def get_balance(self) -> dict:
        """Return the account's available_balance / voucher_balance / cash_balance.

        Endpoint: GET /v1/users/me/balance — free. Useful for sandwich measurement:
        snapshot before a workload, snapshot after, diff = real USD spent.
        """
        r = await self._http.get("/users/me/balance")
        r.raise_for_status()
        body = r.json()
        # Moonshot wraps payload as {"code": 0, "data": {...}, "status": true}
        return body.get("data", body)

    async def estimate_tokens(
        self,
        prompt: str,
        *,
        model: str = DEFAULT_MODEL,
        system: Optional[str] = None,
    ) -> int:
        """Return the number of tokens a chat call WOULD use, without running it.

        Endpoint: POST /v1/tokenizers/estimate-token-count — free. Lets us count
        input tokens across an arbitrary number of prompts (e.g. the full 6,793-
        device enrichment sweep) before authorizing the actual spend.
        """
        messages: list[dict] = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        r = await self._http.post(
            "/tokenizers/estimate-token-count",
            json={"model": model, "messages": messages},
        )
        r.raise_for_status()
        body = r.json()
        # Response shape: {"data": {"total_tokens": int}}
        if "data" in body and isinstance(body["data"], dict):
            return int(body["data"]["total_tokens"])
        return int(body.get("total_tokens", 0))
