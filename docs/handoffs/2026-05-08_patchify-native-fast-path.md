# Handoff: Patchify-Native Fast-Path

**Date:** 2026-05-08
**Commit:** `fcad4b6`

## Discovery

The entire patchify dataset (4614/4615 entries) is `isCommunity: True` with **empty `model` fields**. Users put the product name in the `name` field and defined signal flow directly via `inputs`/`outputs` arrays. We were burning API calls hunting for datasheets when the port data was already there.

## What Changed

### 1. `src/stages/extract_patchify_ports.py`
- Loads patchify JSON lazily and indexes by device_id
- Maps patchify `type`/`connector`/`signal` values to our extraction schema
- Detects swapped manufacturer/name (e.g. "Lav K33 RX" + "Sony" → mfg=Sony, model="Lav K33 RX")
- Returns a full extraction-schema dict ready for Stage 6-7

### 2. `src/runner.py`
- Added `_run_patchify_fast_path()` called after scope check
- For devices with patchify ports: generates specs JSON, marks stages 0-5 completed, moves to queue 5
- **Zero API calls** — skips Stage 0 (SKU resolve), Stage 1 (PDF search), Stage 2 (download), Stage 3-4 (Ragscallion index), Stage 5 (LLM extraction)
- Garbage filter: rejects `Generic` + zero ports + no SKU

### 3. `_sanitize_identifier` fixes
- Patchify labels like "Data In" → `Data_In`
- "1/4\" TRS" → properly handled via mapping tables

## Retry Results (5 previously-failed devices)

| Device | Before | After | Ports |
|---|---|---|---|
| `lav-k33-rx-sony` | PDF not found | ✅ Patch generated | 2× analog audio out |
| `mcplanet-u` | PDF not found | ✅ Patch generated | TX/RX in, RJ45 out (Ethernet) |
| `generic-e-2` | Extraction failed (pump PDF) | ✅ Patch generated | 4 in, 5 out |
| `generic-m5` | Extraction failed (fitness tracker) | ✅ Patch generated | 1× Mic out (Analogue) |
| `roe-creative-display-black-pearl-2v2` | Zero ports from spec sheet | ✅ Patch generated | 2× Ethercon Ethernet (Data In, Data Thru) |

**All 5 patches validated successfully.** Stage 6-7 completed with `stage_generate_patch=2`, `stage_validate_patch=2`.

## Batch v5 Final Tally

- **17/20 devices patched** (was 12/20 before fast-path)
- **3 OOS** (Asus notebook, Netgear switch, GL.iNet router)
- **0 API calls** for the 5 retry devices
- **All 98 tests pass**

## Implications for Full Dataset

This fast-path could potentially patch **~4000+ devices** instantly without a single API call, since every patchify entry has `inputs`/`outputs` data. Physical specs (weight, dimensions, power draw) would still need PDF enrichment, but signal flow — the core value — is already there.

## Next Steps

1. Run the full dataset through the fast-path to see total coverage
2. For devices where patchify ports are generic/"other" type, still attempt PDF search for better connector specificity
3. Consider writing patchify ports to the DB as a `patchify_ports_json` field for auditability
