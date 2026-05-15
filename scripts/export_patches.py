# scripts/export_patches.py
"""Export validated patches from a manifest DB to SignalCanvasDeviceLibrary.

Creates:
    patches/<manufacturer>/<device-id>.patch
    patches/<manufacturer>/<device-id>.json   (if specs_json present)

Usage:
    PYTHONPATH=src:. .venv/bin/python scripts/export_patches.py \
        --db output/batch_200_es_only_v1/manifest.db \
        --out /path/to/SignalCanvasDeviceLibrary/patches \
        --batch batch_200_es_only_v1
"""

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from pathlib import Path

_ANALOG_TAGS = {"analog", "analogue", "speaker", "aes", "aes3", "madi", "spdif", "midi"}
_NETWORK_TAGS = {"dante", "ethernet", "fiber", "st2110", "avb", "hdbaset", "ebus"}
_VIDEO_TAGS = {"sdi", "hdmi", "displayport", "vga", "ndi"}
_PORT_RE = re.compile(r"(\w[\w\[\]\.\s]*?):\s*(in|out|io)\([^)]*\)\s*(?:\[([^\]]*)\])?", re.MULTILINE)
_BRIDGE_RE = re.compile(r"\bbridge\b", re.MULTILINE)
_META_OPEN_RE = re.compile(r"(meta\s*\{)")
_MANAGED_FIELDS = {"quality", "source", "batch", "bridge_status"}


def _infer_manufacturer(device_id: str) -> str:
    """Return the manufacturer slug from a device_id like 'yamaha-cl5' -> 'yamaha'."""
    known_two_part = {
        "allen-heath", "allen-and-heath", "ams-neve", "analog-way", "audio-technica",
        "blackmagic-design", "clear-com", "d-b-audiotechnik", "green-go",
        "high-end-systems", "lab-gruppen", "martin-audio", "meyer-sound",
        "ross-video", "sound-devices",
    }
    parts = device_id.split("-")
    two = "-".join(parts[:2]) if len(parts) >= 2 else parts[0]
    if two in known_two_part:
        return two
    return parts[0]


def _detect_bridge_status(patch_text: str) -> str:
    if _BRIDGE_RE.search(patch_text):
        return "present"
    in_domains: set[str] = set()
    out_domains: set[str] = set()
    for m in _PORT_RE.finditer(patch_text):
        direction = m.group(2)
        tags = {t.strip().lower() for t in (m.group(3) or "").split(",")}
        domain = None
        for t in tags:
            if t in _ANALOG_TAGS:
                domain = "analog"
            elif t in _NETWORK_TAGS:
                domain = "network"
            elif t in _VIDEO_TAGS:
                domain = "video"
        if domain is None:
            continue
        if direction == "in":
            in_domains.add(domain)
        elif direction == "out":
            out_domains.add(domain)
        elif direction == "io":
            in_domains.add(domain)
            out_domains.add(domain)
    if in_domains and out_domains and in_domains != out_domains:
        return "needed"
    return "not_applicable"


def _inject_meta(patch_text: str, quality: str, source: str,
                 bridge_status: str, batch: str) -> str:
    m = _META_OPEN_RE.search(patch_text)
    if not m:
        return patch_text
    start = m.end()
    depth, pos = 1, start
    while pos < len(patch_text) and depth > 0:
        if patch_text[pos] == "{":
            depth += 1
        elif patch_text[pos] == "}":
            depth -= 1
        pos += 1
    meta_content = patch_text[start: pos - 1]
    lines = [l for l in meta_content.splitlines()
             if l.strip().split(":")[0].strip() not in _MANAGED_FIELDS]
    indent = "    "
    new_fields = [
        f'{indent}quality: "{quality}"',
        f'{indent}source: "{source}"',
        f'{indent}batch: "{batch}"',
        f'{indent}bridge_status: "{bridge_status}"',
    ]
    new_meta = "\n".join(lines + new_fields)
    return patch_text[:start] + new_meta + patch_text[pos - 1:]


def export(db_path: Path, out_dir: Path, batch_name: str) -> tuple[int, int]:
    """Export all queue-5 nodes with patch_source from DB to out_dir.

    Returns (exported_count, skipped_count).
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("""
        SELECT device_id, manufacturer, model, specs_json, patch_source
        FROM device_nodes
        WHERE queue = 5
          AND patch_source IS NOT NULL
          AND patch_source != ''
          AND stage_generate_patch = 2
          AND stage_validate_patch = 2
        ORDER BY device_id
    """)

    exported = skipped = 0
    for row in c.fetchall():
        device_id = row["device_id"]
        patch = row["patch_source"]
        specs = row["specs_json"]

        if not patch:
            skipped += 1
            continue

        mfg = _infer_manufacturer(device_id)
        device_dir = out_dir / mfg
        device_dir.mkdir(parents=True, exist_ok=True)

        bridge_status = _detect_bridge_status(patch)
        patch = _inject_meta(patch, quality="C", source="pipeline",
                             bridge_status=bridge_status, batch=batch_name)

        patch_path = device_dir / f"{device_id}.patch"
        patch_path.write_text(patch, encoding="utf-8")

        if specs:
            json_path = device_dir / f"{device_id}.json"
            json_path.write_text(specs, encoding="utf-8")

        exported += 1
        print(f"  {device_id}")

    conn.close()
    return exported, skipped


def main() -> int:
    parser = argparse.ArgumentParser(description="Export patches from manifest DB")
    parser.add_argument("--db", type=Path, default=Path("output/ingestion.db"))
    parser.add_argument("--out", type=Path,
                        default=Path("/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches"))
    parser.add_argument("--batch", type=str, default="unknown",
                        help="Batch name written into meta (e.g. batch_200_es_only_v1)")
    args = parser.parse_args()

    if not args.db.exists():
        print(f"Error: DB not found: {args.db}", file=sys.stderr)
        return 1

    args.out.mkdir(parents=True, exist_ok=True)
    print(f"Exporting from {args.db} -> {args.out}")
    exported, skipped = export(args.db, args.out, args.batch)
    print(f"\nDone: {exported} exported, {skipped} skipped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
