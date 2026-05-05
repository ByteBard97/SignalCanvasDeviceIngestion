#!/usr/bin/env python3
"""Import Dante-enabled products from Audinate's public catalog API.

Audinate maintains a WordPress site with a public REST API exposing
3,394+ Dante-enabled products from 100+ manufacturers.

Endpoint: https://www.getdante.com/wp-json/wp/v2/cpt_product
Manufacturer endpoint: https://www.getdante.com/wp-json/wp/v2/cpt_manufacturer

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/audinate_importer.py
"""

import asyncio
import json
import sys
from pathlib import Path

import httpx

OUTPUT_PATH = Path("output/audinate_catalog.json")
BASE_URL = "https://www.getdante.com/wp-json/wp/v2"
CONCURRENCY = 20
TIMEOUT = 15.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
    ),
}


async def _fetch_all_pages(client: httpx.AsyncClient, endpoint: str) -> list[dict]:
    """Paginate through a WordPress REST API endpoint."""
    all_items: list[dict] = []
    page = 1
    while True:
        url = f"{BASE_URL}/{endpoint}?per_page=100&page={page}"
        try:
            resp = await client.get(url, headers=HEADERS, timeout=TIMEOUT)
            resp.raise_for_status()
            items = resp.json()
            if not items:
                break
            all_items.extend(items)
            if len(items) < 100:
                break
            page += 1
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 400:
                # No more pages
                break
            raise
    return all_items


async def _resolve_manufacturers(client: httpx.AsyncClient) -> dict[int, str]:
    """Fetch all manufacturers (taxonomy terms) and map ID -> name."""
    items = await _fetch_all_pages(client, "ct_manufacturer")
    return {m["id"]: m["name"] for m in items}


async def _fetch_products(client: httpx.AsyncClient) -> list[dict]:
    """Fetch all products with basic metadata."""
    items = await _fetch_all_pages(client, "cpt_product")
    products = []
    for item in items:
        products.append({
            "id": item["id"],
            "title": item["title"]["rendered"],
            "slug": item["slug"],
            "link": item["link"],
            "manufacturer_ids": item.get("ct_manufacturer", []),
        })
    return products


async def main() -> int:
    print("Fetching Audinate Dante catalog...")

    limits = httpx.Limits(max_connections=CONCURRENCY)
    async with httpx.AsyncClient(limits=limits) as client:
        print("  Resolving manufacturers...")
        manufacturers = await _resolve_manufacturers(client)
        print(f"  Found {len(manufacturers)} manufacturers")

        print("  Fetching products...")
        products = await _fetch_products(client)
        print(f"  Found {len(products)} products")

    # Enrich products with manufacturer names
    for p in products:
        p["manufacturers"] = [
            manufacturers.get(mid, f"ID:{mid}")
            for mid in p["manufacturer_ids"]
        ]
        del p["manufacturer_ids"]

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(
            {
                "source": "audinate_dante_catalog",
                "fetched_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
                "manufacturer_count": len(manufacturers),
                "product_count": len(products),
                "manufacturers": manufacturers,
                "products": products,
            },
            f,
            indent=2,
        )

    print(f"\nSaved {len(products)} products to {OUTPUT_PATH}")
    print(f"Manufacturers: {len(manufacturers)}")
    print(f"\nSample products:")
    for p in products[:10]:
        mfg = ", ".join(p["manufacturers"]) if p["manufacturers"] else "Unknown"
        print(f"  {mfg} - {p['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
