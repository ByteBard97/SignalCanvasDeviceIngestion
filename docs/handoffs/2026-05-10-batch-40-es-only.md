# Handoff: Batch 40 ES-Only Pipeline Run

**Date:** 2026-05-10  
**Session ended:** After batch_40_es_only pipeline completed (~21:04 EDT previous night)  
**Next session should:** Export patches, retry failures, or start next batch

---

## What Just Happened

We ran a **40-device batch** through the full SignalCanvas ingestion pipeline using the **combined device context** feature (patchify ports + EasySchematic templates fed into Kimi extraction prompts).

### Batch Selection
- Source: EasySchematic-only pool (devices NOT in patchify)
- Diverse manufacturer mix: 10 Extron + 2 each from 15 other manufacturers
- File: `batch_40_es_only.txt` (pipe-separated: `manufacturer|model|device_id`)

### Results

| Status | Count | Details |
|--------|-------|---------|
| **COMPLETED** | 25 | Full extraction + patches generated. See list below. |
| **OUT_OF_SCOPE** | 9 | Rejected at Stage 0 as `it_networking` — cisco switches, ubiquiti gear, barco clickshare, extron netpa/sharelink, generic SFP, blackmagic 2110 IP converter |
| **EXTRACTION_TIMEOUT** | 3 | Kimi hit 10-min timeout cap: `allen-heath-a-h-sq-rack`, `extron-ac-102-fr`, `extron-dtp-crosspoint-86-4k` |
| **RAGDB_INDEXING_FAILED** | 2 | Old jobs from first run when GPU was OOM: `behringer-x32`, `extron-ca-163-pt-8-ohm-white`. Can be reset and retried. |
| **INITIAL (stuck)** | 1 | `extron-ac-102-us-with-conduit` — never found PDF |

### 25 Completed Devices

- `generic-projector`
- `sonifex-avn-aio4`, `sonifex-avn-cu4`
- `extron-fox3-sr-211-mm`, `extron-sb-33-a-65-70`, `extron-dsc-hd-3g-a`, `extron-12g-hd-sdi-101`
- `yamaha-dbr12`, `yamaha-dbr15`
- `biamp-tesira-ex-in`, `biamp-alc-404d`
- `crestron-dm-nvx-350`, `crestron-dm-nvx-384c`
- `shure-ani4in`, `shure-mxn5-c-1`
- `qsc-q-sys-core-nano`, `qsc-q-sys-cx-q-2k4`
- `aja-ha5-12g`, `aja-fido-t`
- `behringer-ha8000`
- `esi-amber-i2`, `esi-neva-uno`
- `blackmagic-design-bmd-hyperdeck-extreme`
- `barco-4k-tri-combo-input`
- `allen-heath-a-h-dlive-dm64-mixrack`

### Key Infrastructure Fix

**RAG server was down / out of GPU memory.** We:
1. SSH'd into `your-username@localhost` using `~/.ssh/local_linux_key`
2. Found `device-library-rag` at `~/projects/device-library-rag` (port 8080 by default)
3. Restarted it on **port 8086** (the pipeline's expected port)
4. Killed 3 `llama-server` processes eating GPU memory (12.5GB of 15GB)
5. After that, RAG indexing worked and the pipeline completed successfully

---

## Pipeline State

### Manifest DB
- Location: `output/batch_40_es_only/manifest.db`
- 25 completed nodes with `specs_json` and `patch_source`
- 14 failed/rejected nodes in `MANUAL_REVIEW`
- 1 node stuck in `INITIAL`

### Key Code Changes Made This Session

1. **`src/stages/combined_device_context.py`** — Enhanced EasySchematic index to also lookup by `label` (human-readable product name), not just `modelNumber` (SKU). This lets the pipeline search by product name while still matching EasySchematic entries.

2. **`batch_40_es_only.txt`** — Diverse 40-device batch with clean device IDs.

### What Still Needs Work

1. **Export patches** — 25 `.patch` + `.json` files need to be exported to `SignalCanvasDeviceLibrary/patches/`
2. **Retry 3 extraction timeouts** — Reset `allen-heath-sq-rack`, `extron-ac-102-fr`, `extron-dtp-crosspoint-86-4k` to queue 0 and re-run with longer timeout
3. **Retry 2 RAG failures** — Reset `behringer-x32`, `extron-ca-163-pt-8-ohm-white` (GPU memory is now free)
4. **Fix `extron-ac-102-us-with-conduit`** — Find PDF or remove from batch

---

## Infrastructure

| Service | Status | Endpoint |
|---------|--------|----------|
| RAG (device-library-rag) | ✅ Running | `http://192.168.0.200:8086` |
| Patchify fast-path | ✅ Active | `src/stages/extract_patchify_ports.py` |
| Combined context | ✅ Active | `src/stages/combined_device_context.py` |

### SSH Access to Linux Box
```bash
ssh -i ~/.ssh/local_linux_key your-username@localhost
# RAG server: ~/projects/device-library-rag
# Run: ./run-server.sh 8086
```

---

## Next Session Recommendations

1. **Export the 25 patches** to keep momentum:
   ```bash
   python scripts/export_patches.py --db output/batch_40_es_only/manifest.db --out ~/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/patches/
   ```

2. **Retry the 5 recoverable failures** (3 timeouts + 2 RAG fails) by resetting them in the manifest and re-running the pipeline.

3. **Pick the next batch** — we still have ~1,980 EasySchematic-only devices to process.

4. **Consider bumping `EXTRACTION_TIMEOUT_SECONDS`** in `src/pipeline_stages.py` from 600s to 900s if timeout failures keep happening.

---

## Context for New Session

- This project is **SignalCanvasDeviceIngestion** — converts AV hardware datasheets into PatchLang templates
- **Kimi** is the Stage 5 workhorse — it runs the `device-extraction` skill against RAG-indexed PDFs
- **Combined context** merges patchify ports + EasySchematic templates so Kimi doesn't have to rediscover basic port lists
- **PatchLang** is the output format — a structured signal-flow language
- **SignalCanvasDeviceLibrary** is the patch repo at `~/Desktop/SignalCanvas/SignalCanvasDeviceLibrary/`
