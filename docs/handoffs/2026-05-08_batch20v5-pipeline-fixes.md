# Handoff: Batch 20 v5 Pipeline Run + Critical Fixes

**Date:** 2026-05-08
**Context:** User requested 20 random untested devices to identify pipeline/harness failures and fix them before running larger batches.

---

## Batch 20 v5 Results

| Metric | Count |
|--------|-------|
| Total devices | 20 |
| Patched (queue=5) | 12 |
| Out of scope | 3 |
| PDF not found | 2 |
| Extraction failed (low conf / zero ports) | 3 |

**Device list:** `batch_20_random_v5.txt`
**DB:** `output/batch_20_v5.db`

### Successful Patches (12)
- `avid-s6l-24c` — Avid S6L-24C console (minimal: only 3 analog ports)
- `zoom-multitrack-f8npro` — Zoom F8nPro recorder (good: 8 XLR, timecode, sub out)
- `tascam-model-12` — Tascam Model 12 mixer (good: 10 line, 8 mic, aux, main, phones, sub)
- `blackmagic-design-atem-streaming-bridge` — BM ATEM Streaming Bridge (good: network, SDI ref, HDMI, 2x SDI out)
- `generic-2-xlr-trs-to-mini-jack` — Cable adapter (questionable patch)
- `green-go-wbpx` — Green-GO intercom beltpack (minimal: missing network)
- `apple-ipad` — Apple iPad (minimal: only dock connector)
- `onyx16-mackie-onyx16-16-channel-analog-mixer-mackie` — Mackie Onyx16 (missing main 16 inputs)
- `schiit-preamp-mani-phono` — Schiit Mani phono preamp (good but wrong model name)
- `x-keys-t-bar-124-key` — X-Keys controller (empty ports — meta only)
- `olympus-lens-40-150` — Olympus lens (empty ports — correct for lens)
- `audinate-analog-input` — Audinate AVIO analog input (had `Dante_Pri_Out[1..2]` phantom array — fixed in normalize_specs)

### Failures (8)
| Device | Category | Reason |
|--------|----------|--------|
| `asus-notebook` | OUT_OF_SCOPE | IT/networking (correct) |
| `netgear-prosafe-xs716t` | OUT_OF_SCOPE | IT/networking (correct) |
| `generic-gl.inet-gl-mt1300-beryl-router` | OUT_OF_SCOPE | IT/networking (correct) |
| `mcplanet-u` | HTML_SOURCE_NOT_FOUND | Obscure device, no datasheet |
| `lav-k33-rx-sony` | HTML_SOURCE_NOT_FOUND | Manufacturer/model swapped in patchify |
| `generic-e-2` | EXTRACTION_FAILED | Indexed PDF was herbicide SDS, not AV device |
| `generic-m5` | EXTRACTION_FAILED | Indexed PDF was fitness tracker, not AV device |
| `roe-creative-display-black-pearl-2v2` | EXTRACTION_FAILED | PDF had specs but zero connector info |

---

## Critical Bugs Found & Fixed

### 1. Timezone mismatch → ALL nodes falsely marked suspicious
- **Root cause:** `manifest.py` stored timestamps in local time; `polling_loop.py` compared against UTC
- **Impact:** Every single node entering queue_2 was immediately flagged "> 15 min stale"
- **Fix:** Standardized ALL timestamp generation in `manifest.py` to `datetime.now(timezone.utc).isoformat()`
- **File:** `src/harness/manifest.py`

### 2. Stage 1b blocked Stage 2 downloads
- **Root cause:** HTML fallback ran sequentially before PDF downloads
- **Impact:** 15 successful devices waited ~2 minutes for 2 failed devices' HTML fallback
- **Fix:** Run Stage 1b and Stage 2 concurrently via `asyncio.create_task`
- **File:** `src/runner.py`

### 3. Stage 6-7 blocked by Stage 5 extractions
- **Root cause:** `process_stage_5_batch` blocks until ALL extractions finish; while loop can't run patch generation
- **Impact:** Patches weren't generated until ALL devices were extracted
- **Fix:** Swapped order in while loop — Stage 6-7 runs BEFORE Stage 5
- **File:** `src/runner.py`

### 4. Phantom port arrays for low-channel-count Dante
- **Root cause:** `_correct_channels()` threshold was `channels >= 8`; `Dante_Pri_Out[1..2]` was not caught
- **Impact:** `audinate-analog-input` patch had `Dante_Pri_Out[1..2]` instead of single port
- **Fix:** Lowered threshold to `channels >= 2` for protocol-based multiplexed connectors
- **File:** `src/stages/normalize_specs.py`

---

## Quality Issues (Not Yet Fixed)

1. **Under-extraction for complex devices:**
   - `avid-s6l-24c` missing MADI, Dante, AES, network, USB
   - `onyx16-mackie` missing main 16 mic/line inputs
   - `green-go-wbpx` missing Ethernet (IP intercom)

2. **Wrong PDFs for generic/consumer devices:**
   - `generic-2-xlr-trs-to-mini-jack` → Rode VXLR Pro datasheet (wrong product)
   - `generic-m5` → fitness tracker manual
   - `apple-ipad` → old iOS 4 product info PDF

3. **Data quality in patchify source:**
   - `lav-k33-rx-sony`: manufacturer="Lav K33 RX", model="Sony" (swapped)
   - `onyx16-mackie...`: manufacturer="ONYX16", model="Mackie Onyx16..." (swapped)

4. **Very slow extraction:** Kimi CLI per device takes 3-5 minutes

---

## Code Changes Summary

```
src/harness/manifest.py       | 28 lines — UTC timestamps everywhere
src/runner.py                 | 127 lines — concurrent Stage 1b/2, Stage 6-7 before 5
src/stages/normalize_specs.py | 65 lines — phantom port fix for channels >= 2
```

All tests pass (98 tests).

---

## Recommendations Before Larger Batch

1. **Add pre-filter** in `run_batch.py` to skip generic/cable/consumer devices (improves yield)
2. **Improve extraction prompts** for consoles — explicitly search for MADI/Dante/AES/network
3. **Add PDF URL validation** — reject manuals.plus, non-manufacturer domains for generic devices
4. **Detect swapped manufacturer/model** in patchify data before running pipeline
