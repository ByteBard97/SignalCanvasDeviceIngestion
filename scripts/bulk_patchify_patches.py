#!/usr/bin/env python3
"""Bulk-generate patches directly from patchify inputs/outputs for ALL devices.

This bypasses the full pipeline entirely — no API calls, no PDF searches,
no Ragscallion indexing, no LLM classification. Just converts patchify port data
to PatchLang using rule-based classification only.

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/bulk_patchify_patches.py \
        --out /Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.stages.extract_patchify_ports import extract_specs_from_patchify
from src.stages.generate_patch import generate_patch
from src.stages.validate_patch import validate_patch
from src.stages.normalize_specs import normalize_extraction
from src.stages.classify_device import _classify_by_rule


def _shard_prefix(device_id: str) -> str:
    first = device_id[0].lower() if device_id else "_"
    return first if first.isalpha() else "_"


def bulk_generate(out_dir: Path) -> dict[str, int]:
    """Generate patches for all patchify devices with port data."""
    patchify_path = Path("/Users/ceres/Desktop/SignalCanvas/patchify-gear-all.json")
    with open(patchify_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    stats = {"total": 0, "patched": 0, "no_ports": 0, "validation_failed": 0, "errors": 0}

    for item in data:
        stats["total"] += 1
        mfg = item.get("manufacturer", "")
        name = item.get("name", "")

        # Build device_id the same way run_batch.py does
        mfg_slug = mfg.lower().replace(" ", "-").replace("_", "-")
        name_slug = name.lower().replace(" ", "-").replace("/", "-").replace("_", "-")
        name_slug = "-".join(filter(None, name_slug.split("-")))
        device_id = f"{mfg_slug}-{name_slug}"[:64]
        if not device_id:
            continue

        specs = extract_specs_from_patchify(device_id)
        if not specs:
            stats["no_ports"] += 1
            continue

        try:
            # Use rule-based classification only (no LLM calls)
            classification = _classify_by_rule(mfg, name)
            device_class = classification.class_ if classification else "unknown"

            normalized = normalize_extraction(specs, device_class)
            patch_source = generate_patch(normalized)

            is_valid, errors = validate_patch(patch_source)
            if not is_valid:
                stats["validation_failed"] += 1
                print(f"  ⚠️  {device_id}: validation failed — {errors}")
                continue

            # Write patch + JSON
            prefix = _shard_prefix(device_id)
            slug = device_id.lower().replace(" ", "-")
            device_dir = out_dir / prefix / slug
            device_dir.mkdir(parents=True, exist_ok=True)

            patch_path = device_dir / f"{device_id}.patch"
            patch_path.write_text(patch_source, encoding="utf-8")

            json_path = device_dir / f"{device_id}.json"
            json_path.write_text(json.dumps(specs, indent=2), encoding="utf-8")

            stats["patched"] += 1
            if stats["patched"] % 500 == 0:
                print(f"  ... {stats['patched']} patches generated")

        except Exception as e:
            stats["errors"] += 1
            print(f"  ❌ {device_id}: error — {e}")

    return stats


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches"),
    )
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)

    print(f"Bulk-generating patches for all patchify devices → {args.out}")
    print("(Rule-based classification only — no LLM calls)")
    stats = bulk_generate(args.out)

    print(f"\nDone:")
    print(f"  Total patchify entries: {stats['total']}")
    print(f"  Patches generated:      {stats['patched']}")
    print(f"  No ports (skipped):     {stats['no_ports']}")
    print(f"  Validation failed:      {stats['validation_failed']}")
    print(f"  Errors:                 {stats['errors']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
