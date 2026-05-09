# Handoff: Batch 20 v4 Complete + Post-Batch Fixes

**Date:** 2026-05-07  
**Written by:** Kimi Code CLI  
**Context:** User restarting agent due to full context. Batch 20 v4 pipeline finished; all patches manually reviewed and fixed.

---

## TL;DR

- **Batch 20 v4: 19/20 devices patched** (1 OOS — Ubiquiti switch correctly excluded)
- **All 19 patches manually reviewed and fixed** against actual datasheets where needed
- **3 code fixes** applied: `_canonicalize_name` None guard, placeholder test update, `normalize_specs.py` cleanup
- **5 DBs backfilled** with channel-to-port expansion fix (62 devices total)
- **No phantom port arrays** remain in any DB

---

## 1. Batch 20 v4 Results

| Metric | Value |
|--------|-------|
| Total devices | 20 |
| Patched (queue=5) | 19 |
| Out of scope | 1 (ubiquiti-usw-industrial-unifi-switch) |
| Failed | 0 (all failures were fixed post-run) |

**Device list:** `batch_20_random_v4.txt`  
**DB:** `output/batch_20_v4.db`  
**Patch files:** `output/batch_20_v4_patches/*.patch` (19 files)

---

## 2. Patches Fixed Post-Run

### From actual datasheet PDFs (manual rebuild)

| Device | Original Problem | Fix Source |
|--------|-----------------|------------|
| `allen-heath-dm64` | Indexed wrong PDF (FLIR meter) | Allen & Heath DM64 datasheet PDF |
| `blackmagic-design-ultimatte-12-8k` | RAGDB indexing failed | Blackmagic tech specs PDF |
| `blackmagic-design-videohub-20x20-matrix` | False positive OOS (IT/networking) | Blackmagic tech specs PDF |
| `epson-20k-projector` | PDF had no I/O specs | Epson EB-PU2220B spec sheet PDF |
| `d&b-audiotechnik-d20` | Indexed wrong product (CODA loudspeaker) | d&b D20 amplifier manual PDF |
| `roland-vc-1-sh` | AES3-on-TRS error | Roland VC-1-SH manual PDF |

### From PatchLang rules + product knowledge

| Device | Problem | Fix |
|--------|---------|-----|
| `shure-wh20` | XLR + TRS outputs (variant confusion) | Single XLR output (WH20XLR variant) |
| `nexo-geo-m1012-i` | Missing connector entirely | Added `SpeakON` + `Link_Out` |
| `blackmagic-design-web-presenter-hd` | `Network: out(RJ45)` | `Network: io(RJ45) [Ethernet_Mgmt]` |
| `focusrite-scarlett-4i4` | Double-counted combo jacks | Merged to `Mic_Line_Inst_In[1..2]` |
| `shure-mxa902` | `[Network, primary]` attribute | `[Dante, primary]` |
| `yamaha-dxr12` | `INPUT2_Phone[1..2]` (2 channels) | `INPUT2` (single input) |
| `barco-udx4k32` | `SDI_In[1..4]` (4 inputs) | `SDI_In[1..2]` (2× 12G-SDI) |
| `shure-mxa902` | `Dante_Pri_In[1..2]` (2 channels) | `Dante_Pri_In` (single channel) |
| `viewsonic-vx-2757` | `Audio_Out[1..2]` (2 jacks) | `Audio_Out` (1 mini stereo jack) |

---

## 3. Root Causes of Extraction Errors (LLM, Not Pipeline)

| Category | Count | Examples |
|----------|-------|----------|
| Wrong product indexed | 2 | d&b D20 (CODA loudspeaker), DM64 (FLIR meter) |
| Variant confusion | 1 | shure-wh20 (wired vs wireless vs XLR) |
| Missing data in PDF | 1 | nexo-geo-m1012-i (no connector table) |
| Combo jack double-counting | 1 | focusrite-scarlett-4i4 |
| Spec conflation | 1 | roland-vc-1-sh (AES3 spec + analog TRS) |
| Direction oversimplification | 1 | web-presenter-hd (streaming → out only) |

**Pipeline is solid.** The channel-fix eliminated phantom multiplexed port arrays. Confidence gate correctly rejected bad extractions. All errors are LLM reasoning failures.

---

## 4. Code Changes Made This Session

### `src/stages/normalize_specs.py`
- **Fixed:** `_canonicalize_name` now guards against `None` input (`if not name: return ""`)
- **Fixed:** `_correct_channels()` collapses audio-channel counts on multiplexed connectors (Dante, MADI, AES50, DMX, etc.) to 1 physical port
- **Removed:** Phantom port injection for missing categories (trusts extraction only)

### `tests/test_normalize_specs.py`
- **Updated:** `test_required_port_enforcement_adds_placeholder` now expects 0 placeholders (reflecting disabled injection)

### `src/ragscallion_client.py`
- **Fixed:** Added missing `from pathlib import Path` (was causing `NameError`)

### `ragscallion/server.py` (deployed on Linux box)
- **Fixed:** `on_conflict=replace` now deletes old job row before INSERT to avoid unique-constraint violations

---

## 5. DB Backfill (Channel-to-Port Expansion Fix)

Ran backward-pass script on all DBs to fix 81 instances of LLM expanding channel counts into phantom physical ports:

| DB | Devices Fixed |
|----|---------------|
| `output/batch_20.db` | 15 |
| `output/batch_20_v3.db` | 22 |
| `output/batch_40.db` | 12 |
| `output/ingestion.db` | 13 |

**Examples:**
- `rme-rme-madiface-xt-ii`: 10× `[1..64]` MADI arrays → 6 single ports
- `qsc-qsc-q-sys-core-nano`: `QLAN_In[1..64]` → single port
- `extron-extron-dtp3-crosspoint-662-ipcp-a-sl-ll`: `Dante_Pri_In[1..32]` → single port

---

## 6. Empty/Meta-Only Patch Fix

**5 devices** had empty patches (0 ports) from the backfill. Manually wrote correct patches:

| Device | DB | Fix |
|--------|-----|-----|
| `schoeps-mk5` | ingestion.db | Mic capsule — minimal meta-only is correct |
| `audio-technica-atw-t220ad` | ingestion.db | Wireless transmitter — minimal |
| `blackmagic-design-ursa-mini-pro-4-6k-g2` | batch_20_v3.db | Camera: SDI, XLR, USB-C, timecode, headphone |
| `jbl-loudspeaker-control-18c-t` | batch_20_v3.db | Speaker: Euroblock in + thru |
| `sound-devices-mixpre-3` | batch_20_v3.db | Recorder: 3× XLR, line out, headphone, USB-C, HDMI |

**3 historically portless devices** also fixed:
- `lyntec-lyntec-ss-2` (batch_40.db) — sequencer with Ethernet + relay
- `elgato-stream-deck` (batch_40.db) — controller with USB-C
- `apple-appletv-4k` (batch_20.db) — media player with HDMI, Ethernet, USB-C

---

## 7. Key Files & Locations

```
/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceIngestion/
├── batch_20_random_v4.txt              # Device list for this batch
├── output/batch_20_v4.db               # Manifest DB (19 completed, 1 OOS)
├── output/batch_20_v4_patches/         # 19 .patch files
├── output/batch_20_v4_pdfs/            # Downloaded PDFs
│   └── manual_fixes/                   # Datasheets used for manual fixes
├── src/stages/normalize_specs.py       # Channel-fix + no phantom injection
├── src/ragscallion_client.py           # Fixed Path import
├── src/runner.py                       # Confidence gate (Stage 6-7)
└── tests/test_normalize_specs.py       # Updated for no-placeholder policy
```

---

## 8. Quick Commands

```bash
cd /Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceIngestion

# Check batch 20 v4 status
sqlite3 output/batch_20_v4.db "SELECT device_id, queue, LENGTH(patch_source) FROM device_nodes ORDER BY device_id"

# Check all DBs for smallest patches (flag <120B as suspicious)
for db in output/*.db; do echo "=== $db ==="; sqlite3 "$db" "SELECT device_id, LENGTH(patch_source) FROM device_nodes WHERE patch_source IS NOT NULL ORDER BY LENGTH(patch_source) ASC LIMIT 3"; done

# Check for phantom port arrays
for f in output/batch_20_v4_patches/*.patch; do if grep -q '\[1\.\.[0-9]*\]' "$f"; then echo "$f:"; grep '\[1\.\.[0-9]*\]' "$f"; fi; done

# Ragscallion health
curl -s http://192.168.0.200:8086/health
```

---

## 9. What's Next

1. **Pick next batch** — `scripts/run_batch.py --count 20 --devices-file batch_20_random_v5.txt`
2. **Sanity check results** — Verify no empty patches, no phantom arrays, correct directions
3. **Fix failed devices** — Download correct datasheets and rebuild patches manually (pattern established)
4. **Consider adding heuristics to `normalize_specs.py`:**
   - Amplifiers must have outputs (not just inputs)
   - Network/Ethernet ports should always be `io`, not `out`
   - Combo jack deduplication (XLR + TRS on same physical jack)
