#!/usr/bin/env python3
"""Estimate input-token cost for bridge-inference across Patchify + EasySchematic datasets.

Uses Moonshot's free /v1/tokenizers/estimate-token-count endpoint via MoonshotClient.
"""

from __future__ import annotations

import asyncio
import glob
import json
import os
import random
import re
import statistics
import sys
from pathlib import Path

# Load .env before importing project modules
from dotenv import load_dotenv

load_dotenv()

# Ensure src/ is on path so we can import MoonshotClient
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from moonshot_client import MoonshotClient  # noqa: E402

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SAMPLE_SIZE_PER_DATASET = 50
OUTPUT_TOKENS_PER_DEVICE = 200
SEMAPHORE_LIMIT = 5

# Pricing per million tokens (USD)
PRICING_8K = {"input": 0.20, "output": 2.00, "model": "moonshot-v1-8k"}
PRICING_32K = {"input": 1.00, "output": 3.00, "model": "moonshot-v1-32k"}

# Data paths
PATCHIFY_PATH = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
EASYSCHEMATIC_GLOB = Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasFrontend/tools/src/examples/devices/*.patch")

PROMPT_TEMPLATE = """You are an AV signal-flow expert. Given the ports of a device, list the internal bridges (signal flow paths) inside it as JSON.

Device: {manufacturer} {model}
Category: {category}
Ports:
{ports_summary}

Return ONLY a JSON array of bridges, each in the form
{{"from": "PortName", "to": "PortName"}}.
"""


# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------
def load_patchify_devices() -> list[dict]:
    with open(PATCHIFY_PATH, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    devices = []
    for entry in data:
        manufacturer = entry.get("manufacturer", "Unknown")
        model = entry.get("name", "Unknown")
        category = entry.get("category", "unknown")
        ports_summary = _summarize_patchify_ports(entry)
        devices.append(
            {
                "source": "patchify",
                "manufacturer": manufacturer,
                "model": model,
                "category": category,
                "ports_summary": ports_summary,
            }
        )
    return devices


def _summarize_patchify_ports(entry: dict) -> str:
    lines: list[str] = []
    for direction, key in (("in", "inputs"), ("out", "outputs")):
        for port in entry.get(key, []):
            label = port.get("label", "?")
            connector = port.get("connector") or port.get("type", "?")
            signal = port.get("signal", "?")
            lines.append(f"  - {label} ({direction}, {connector}, {signal})")
    return "\n".join(lines) if lines else "  (no ports)"


def load_easyschematic_devices() -> list[dict]:
    devices: list[dict] = []
    for patch_file in glob.glob(str(EASYSCHEMATIC_GLOB)):
        with open(patch_file, "r", encoding="utf-8") as fh:
            text = fh.read()
        # Split into template blocks
        templates = re.split(r"(?=template\s+\w+\s*\{)", text)
        for block in templates:
            block = block.strip()
            if not block.startswith("template"):
                continue
            m_meta = re.search(
                r'meta\s*\{[^}]*manufacturer:\s*"([^"]*)"[^}]*model:\s*"([^"]*)"[^}]*category:\s*"([^"]*)"',
                block,
                re.DOTALL,
            )
            if not m_meta:
                # Some templates omit model or category; be lenient
                m_meta = re.search(
                    r'meta\s*\{[^}]*manufacturer:\s*"([^"]*)"',
                    block,
                    re.DOTALL,
                )
                if not m_meta:
                    continue
                manufacturer = m_meta.group(1)
                model = "Unknown"
                category = "unknown"
                # Try to grab model/category if present
                m_model = re.search(r'model:\s*"([^"]*)"', block)
                m_cat = re.search(r'category:\s*"([^"]*)"', block)
                if m_model:
                    model = m_model.group(1)
                if m_cat:
                    category = m_cat.group(1)
            else:
                manufacturer, model, category = m_meta.groups()

            ports_summary = _summarize_easyschematic_ports(block)
            devices.append(
                {
                    "source": "easyschematic",
                    "manufacturer": manufacturer,
                    "model": model,
                    "category": category,
                    "ports_summary": ports_summary,
                }
            )
    return devices


def _summarize_easyschematic_ports(block: str) -> str:
    lines: list[str] = []
    m_ports = re.search(r"ports\s*\{(.*?)\n\}", block, re.DOTALL)
    if not m_ports:
        return "  (no ports)"
    body = m_ports.group(1)
    # Each port line looks like:  NAME: in/out/io(CONN) [SIGNAL]
    # or NAME[1..N]: in/out/io(CONN) [SIGNAL]
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        # direction
        direction = "?"
        if ": in(" in line or ": in " in line or ": in[" in line:
            direction = "in"
        elif ": out(" in line or ": out " in line or ": out[" in line:
            direction = "out"
        elif ": io(" in line or ": io " in line or ": io[" in line:
            direction = "io"
        else:
            # fallback: look for bare in/out/io after colon
            m_dir = re.search(r":\s*(in|out|io)\b", line)
            if m_dir:
                direction = m_dir.group(1)
        # connector
        m_conn = re.search(r"\(([^)]+)\)", line)
        connector = m_conn.group(1) if m_conn else "?"
        # signal
        m_sig = re.search(r"\[([^\]]+)\]", line)
        signal = m_sig.group(1) if m_sig else "?"
        # port name (everything before first colon)
        name = line.split(":")[0].strip()
        lines.append(f"  - {name} ({direction}, {connector}, {signal})")
    return "\n".join(lines) if lines else "  (no ports)"


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------
def stratified_sample(devices: list[dict], n: int, seed: int = 42) -> list[dict]:
    if len(devices) <= n:
        return devices.copy()
    rng = random.Random(seed)
    # Group by category
    by_cat: dict[str, list[dict]] = {}
    for d in devices:
        by_cat.setdefault(d["category"], []).append(d)
    # Allocate proportionally, ensuring at least 1 per category if possible
    cats = list(by_cat.keys())
    rng.shuffle(cats)
    sample: list[dict] = []
    remaining = n
    # First pass: give each category at least 1 (if we have enough categories)
    for cat in cats:
        if remaining <= 0:
            break
        if len(by_cat[cat]) > 0:
            sample.append(rng.choice(by_cat[cat]))
            remaining -= 1
    # Second pass: fill rest proportionally by population
    total_remaining = sum(len(by_cat[c]) for c in cats)
    for cat in cats:
        if remaining <= 0:
            break
        pop = len(by_cat[cat])
        if pop == 0:
            continue
        # How many already taken from this category?
        taken = sum(1 for s in sample if s["category"] == cat)
        alloc = int(remaining * pop / total_remaining)
        alloc = max(alloc, 0)
        available = [d for d in by_cat[cat] if d not in sample]
        k = min(alloc, len(available))
        if k > 0:
            sample.extend(rng.sample(available, k))
            remaining -= k
    # If rounding left us short, fill randomly from remaining
    if remaining > 0:
        available = [d for d in devices if d not in sample]
        k = min(remaining, len(available))
        sample.extend(rng.sample(available, k))
    rng.shuffle(sample)
    return sample[:n]


# ---------------------------------------------------------------------------
# Prompt building
# ---------------------------------------------------------------------------
def build_prompt(device: dict) -> str:
    return PROMPT_TEMPLATE.format(
        manufacturer=device["manufacturer"],
        model=device["model"],
        category=device["category"],
        ports_summary=device["ports_summary"],
    )


# ---------------------------------------------------------------------------
# Token estimation
# ---------------------------------------------------------------------------
async def estimate_sample(
    client: MoonshotClient, devices: list[dict], sem: asyncio.Semaphore
) -> list[int]:
    async def _one(d: dict) -> int:
        prompt = build_prompt(d)
        async with sem:
            return await client.estimate_tokens(prompt)

    return await asyncio.gather(*[_one(d) for d in devices])


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------
def percentile(sorted_vals: list[int], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return float(sorted_vals[f])
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def compute_cost(total_input: int, total_output: int, mean_input: float) -> tuple[float, dict]:
    if mean_input < 7000:
        pricing = PRICING_8K
    else:
        pricing = PRICING_32K
    in_cost = total_input * pricing["input"] / 1_000_000
    out_cost = total_output * pricing["output"] / 1_000_000
    return in_cost + out_cost, pricing


def print_report(
    name: str,
    total_devices: int,
    sample_tokens: list[int],
    output_per_device: int,
) -> tuple[int, int, float, dict]:
    sorted_tokens = sorted(sample_tokens)
    mean_tok = statistics.mean(sorted_tokens)
    median_tok = statistics.median(sorted_tokens)
    p95_tok = percentile(sorted_tokens, 95)
    projected_input = int(mean_tok * total_devices)
    projected_output = output_per_device * total_devices
    total_cost, pricing = compute_cost(projected_input, projected_output, mean_tok)

    print(f"\n=== {name} ===")
    print(f"  Total devices        : {total_devices}")
    print(f"  Sampled devices      : {len(sample_tokens)}")
    print(f"  Mean input tokens    : {mean_tok:.1f}")
    print(f"  Median input tokens  : {median_tok:.1f}")
    print(f"  P95 input tokens     : {p95_tok:.1f}")
    print(f"  Projected total input tokens : {projected_input:,}")
    print(f"  Projected total output tokens: {projected_output:,}")
    print(f"  Pricing tier         : {pricing['model']} (${pricing['input']}/MTok in, ${pricing['output']}/MTok out)")
    print(f"  Projected USD cost   : ${total_cost:.2f}")

    return projected_input, projected_output, total_cost, pricing


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    print("Loading datasets...")
    patchify_devices = load_patchify_devices()
    easyschematic_devices = load_easyschematic_devices()
    print(f"  Patchify devices      : {len(patchify_devices)}")
    print(f"  EasySchematic devices : {len(easyschematic_devices)}")

    patchify_sample = stratified_sample(patchify_devices, SAMPLE_SIZE_PER_DATASET)
    easyschematic_sample = stratified_sample(easyschematic_devices, SAMPLE_SIZE_PER_DATASET)

    client = MoonshotClient()
    sem = asyncio.Semaphore(SEMAPHORE_LIMIT)

    print("\nEstimating tokens (sample pass)...")
    patchify_tokens, easyschematic_tokens = await asyncio.gather(
        estimate_sample(client, patchify_sample, sem),
        estimate_sample(client, easyschematic_sample, sem),
    )

    await client.close()

    p_in, p_out, p_cost, _ = print_report(
        "Patchify", len(patchify_devices), patchify_tokens, OUTPUT_TOKENS_PER_DEVICE
    )
    e_in, e_out, e_cost, _ = print_report(
        "EasySchematic", len(easyschematic_devices), easyschematic_tokens, OUTPUT_TOKENS_PER_DEVICE
    )

    total_cost = p_cost + e_cost
    print(f"\n>>> COMBINED PROJECTED COST: ${total_cost:.2f} <<<")


if __name__ == "__main__":
    asyncio.run(main())
