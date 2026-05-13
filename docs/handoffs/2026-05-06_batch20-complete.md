# Handoff: Batch 20 Complete → Batch 40 Next

**Date:** 2026-05-06  
**Written by:** Kimi Code CLI  
**Context:** User is leaving computer, will restart agent with cleared context. Next task: run 40 NEW random devices through pipeline.

---

## TL;DR

- **Batch 20 is DONE**: 17/20 AV devices have clean, valid PatchLang patches. 3 networking devices correctly marked OUT_OF_SCOPE.
- **Ragscallion now supports HTML/Markdown** (deployed to Linux box `localhost:8086`).
- **Pipeline has HTML fallback**: When PDF not found, searches for HTML spec page → trafilatura extract OR Playwright PDF render.
- **Next task**: Pick 40 random unprocessed pro-AV devices from the candidate pool, run through pipeline, vigilently observe.

---

## 1. Project Structure

```
/Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceIngestion/
├── src/
│   ├── runner.py                    # Main pipeline orchestrator (Stages 0-7)
│   ├── pipeline_stages.py           # Stage implementations (find PDF, download, submit, extract)
│   ├── ragscallion_client.py        # HTTP client for Ragscallion server
│   ├── polling_loop.py              # Background poller for Ragscallion job completion
│   ├── kimi_runner.py               # Kimi CLI wrapper for web search agents
│   ├── config.py                    # Settings (API keys, DB paths)
│   ├── harness/
│   │   └── manifest.py              # SQLite manifest DB schema + DeviceNode model
│   ├── stages/
│   │   ├── classify_device.py       # Device classifier (rule-based + LLM fallback)
│   │   ├── normalize_specs.py       # Post-process LLM extractions (NO phantom injection)
│   │   ├── generate_patch.py        # PatchLang generator from normalized specs
│   │   ├── validate_patch.py        # PatchLang syntax validator
│   │   └── fetch_html_source.py     # NEW: HTML fetch + JS detection + Playwright PDF fallback
│   └── cli/
│       └── manifest_admin.py        # NEW: CLI for manifest manipulation (status, oos, clear, reset-queue)
├── output/
│   ├── batch_20_v3.db               # Current manifest DB (batch 20 results)
│   └── pdfs/                        # Downloaded PDF cache
├── tests/                           # 62 passing tests
│   └── test_pipeline.py             # Pipeline unit tests
├── batch_20_random_v3.txt           # Current batch device list (manufacturer|model|device_id)
└── .venv/                           # Python 3.14.3 virtualenv
```

---

## 2. How to Run the Pipeline

### Full pipeline for a device list:
```bash
cd /Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceIngestion
.venv/bin/python -m src.runner \
  --devices batch_40_random.txt \
  --cache-dir output/pdfs \
  --manifest-db output/batch_40.db
```

### Check manifest status:
```bash
.venv/bin/python -m src.cli.manifest_admin --db output/batch_40.db status
```

### Mark devices out-of-scope:
```bash
.venv/bin/python -m src.cli.manifest_admin --db output/batch_40.db oos device-id-1 device-id-2
```

### Hard-reset a device (wipes all progress):
```bash
.venv/bin/python -m src.cli.manifest_admin --db output/batch_40.db clear device-id-1
```

### Check which devices from a candidate list are unprocessed:
```bash
# There's a batch_status.py utility for this
.venv/bin/python scripts/batch_status.py --manifest output/batch_40.db --candidates candidates.txt
```

---

## 3. Architecture Overview

### Pipeline Stages

| Stage | Name | What it does |
|-------|------|-------------|
| 0 | `resolve_sku` | Map user aliases to canonical manufacturer SKUs |
| 1 | `find_pdf` | Search web for manufacturer PDF datasheet |
| 1b | `find_html_source` | **NEW** Fallback when PDF not found — searches HTML page → extract or render |
| 2 | `download_pdf` | Download PDF to local cache |
| 3-4 | `submit_to_ragscallion` | Upload document to Ragscallion for indexing |
| 5 | `extract_specs` | LLM extraction from indexed corpus (I/O, ports, signal flow) |
| 6 | `generate_patch` | Generate PatchLang from extracted specs |
| 7 | `validate_patch` | Validate PatchLang syntax |

### Queues

| Queue | ID | Meaning |
|-------|----|---------|
| `QUEUE_0_INITIAL` | 0 | Brand new, not started |
| `QUEUE_1_CANNOT_FIND_PDF` | 1 | Stuck at Stage 1/2 (PDF acquisition) |
| `QUEUE_2_POLLING_RAGSCALLION` | 2 | Submitted to Ragscallion, waiting for indexing |
| `QUEUE_3_READY_FOR_EXTRACTION` | 3 | Indexed, ready for Stage 5 extraction |
| `QUEUE_4_MANUAL_REVIEW` | 4 | Failed — needs human decision |
| `QUEUE_5_COMPLETED` | 5 | Done — has valid specs_json and patch_source |

---

## 4. Key Infrastructure

### Ragscallion Server (Linux box)
- **Host:** `localhost:8086`
- **Path on Linux:** `~/projects/device-library-rag/`
- **Start:** `cd ~/projects/device-library-rag && nohup .venv/bin/python server.py 8086 > server.log 2>&1 &`
- **Health:** `curl http://localhost:8086/health`
- **Supported formats:** `.pdf`, `.md`, `.txt`, `.html`, `.htm`
- **PDF path:** Uses Marker (GPU-heavy, serialized via `MARKER_LOCK`)
- **Non-PDF path:** Uses `trafilatura` for HTML→Markdown extraction, then chunk→embed→index

### Ragscallion Files on MacBook (source of truth)
```
/Users/ceres/Desktop/SignalCanvas/ragscallion/
├── server.py          # HTTP server
├── ingest.py          # Ingestion pipeline (Marker + trafilatura)
├── pyproject.toml     # Dependencies (including trafilatura optional)
└── run-server.sh      # Startup script
```

**Deploy to Linux:**
```bash
scp /Users/ceres/Desktop/SignalCanvas/ragscallion/{server.py,ingest.py,pyproject.toml} \
  localhost:~/projects/device-library-rag/
# Then SSH to install deps and restart
```

---

## 5. Guardrails & Quality Checks

### Scope Gate (`classify_device.py`)
Rejects IT/networking gear BEFORE spending money on PDF search:
- **Patterns:** Cisco, Ubiquiti, MikroTik, Aruba, Juniper, Fortinet, Palo Alto, TP-Link, Netgear, D-Link, Linksys, HP, HPE, Dell, SonicWall, WatchGuard, Extreme, Ruckus
- **Failure:** `OUT_OF_SCOPE`, non-retryable

### Confidence Gate (`runner.py` Stage 6-7)
Rejects low-confidence extractions with zero ports:
- If `extraction_confidence == "low"` AND `len(ports) == 0` → `EXTRACTION_FAILED`
- Message: "The indexed PDF likely lacks technical I/O specifications."

### No Phantom Port Injection (`normalize_specs.py`)
- **REMOVED** `_REQUIRED_CATEGORIES` that was injecting fake Dante/Network/Analog ports
- Only ports actually found in the document are kept

### Port Filtering (`normalize_specs.py`)
- Power ports filtered out
- Control ports (Ethernet_Mgmt, USB, RS-232) filtered out
- False Dante labels sanitized

---

## 6. Known Issues & Gotchas

### 1. Scope check misses queue_1 nodes
**Problem:** `_run_scope_check()` only processes `QUEUE_0_INITIAL` nodes. If a networking device is already in `QUEUE_1` from a prior run, it bypasses classification.
**Fix needed:** Also check queue_1 nodes that haven't been classified yet.
**Workaround:** Use `manifest_admin.py oos` to manually mark them.

### 2. Ragscallion replace race condition
**Problem:** When re-submitting with `on_conflict=replace`, the polling loop may pick up the OLD completed job before the new indexing finishes.
**Mitigation:** The pipeline handles this, but watch for stale job IDs.

### 3. LLM classifier guesses wrong without context
**Problem:** The 8k model sometimes misclassifies devices (e.g., `dante_adapter_input` for a headphone amp).
**Mitigation:** Rule-based regex patterns catch common cases. Phantom port injection was killed. Confidence gate catches zero-port low-confidence extractions.

### 4. Playwright PDF rendering
**Status:** The HTML fallback pipeline can detect JS-heavy pages and render them to PDF via Playwright (`npx playwright pdf`). This path is in `fetch_html_source.py` but hasn't been battle-tested end-to-end yet.

### 5. Manifest DB locked
**Problem:** If the runner crashes or is killed, the SQLite DB may have a stale write lock.
**Fix:** `rm output/*.db-journal` or just retry — SQLite is resilient.

---

## 7. What "Vigilently Observe" Means

When running the 40-device batch, the agent should:

1. **Watch Stage 1 failures** — If PDF not found, verify Stage 1b HTML fallback triggers
2. **Watch Stage 2 downloads** — Check PDF validity (first 4 bytes = `%PDF`), size > 30KB
3. **Watch Ragscallion submissions** — Verify jobs are queued, poll for completion
4. **Watch Stage 5 extractions** — Check `extraction_confidence` field; flag "low" + zero ports
5. **Watch Stage 6-7 patch generation** — Verify patches have real ports, no `PLACEHOLDER` or `UNKNOWN`
6. **Flag networking devices** — If any slip through, mark them `oos` immediately
7. **Log everything** — The pipeline already logs extensively; tail the logs

**Key log patterns to watch for:**
```
# Good
"PDF URL found: ..."
"spec_sheet submitted to Ragscallion, job_id=..."
"Device X patch generated and validated"

# Bad — investigate
"All X attempts failed. Last: ..."
"Extraction confidence is LOW and zero ports found"
"Patch validation failed: ..."
"Scope check: X device(s) rejected"
"JS-heavy page detected, falling back to Playwright PDF"
```

---

## 8. Next Task: 40 New Random Devices

### Steps:
1. **Generate candidate list** — Pick 40 random unprocessed pro-AV devices from the master pool
2. **Create device list** — Format: `manufacturer|model|device_id` (one per line)
3. **Create new manifest DB** — `output/batch_40.db` (or whatever name)
4. **Run pipeline** — `python -m src.runner --devices batch_40.txt --manifest-db output/batch_40.db`
5. **Observe** — Watch logs, check status periodically, flag issues
6. **Fix & retry** — For failed devices, investigate root cause, fix if retryable

### Device selection criteria:
- Pro-AV gear (mixers, amps, speakers, cameras, switchers, etc.)
- NOT IT/networking (switches, routers, firewalls, access points)
- NOT already processed in batch_20_v3.db
- Diverse manufacturers (don't pick 40 Extron devices)

---

## 9. Quick Reference Commands

```bash
# Activate venv
cd /Users/ceres/Desktop/SignalCanvas/SignalCanvasDeviceIngestion
source .venv/bin/activate

# Run tests
python -m pytest tests/ -x -q

# Run pipeline
python -m src.runner --devices devices.txt --cache-dir output/pdfs --manifest-db output/batch.db

# Check Ragscallion health
curl http://localhost:8086/health

# Check Ragscallion stats
curl http://localhost:8086/stats

# Check manifest
python -m src.cli.manifest_admin --db output/batch.db status

# SSH to Linux box
ssh localhost

# Restart Ragscallion on Linux
ssh localhost "cd ~/projects/device-library-rag && pkill -f 'server.py 8086'; sleep 2; nohup .venv/bin/python server.py 8086 > server.log 2>&1 &"
```

---

## 10. Files Modified in This Session

| File | Change |
|------|--------|
| `src/stages/fetch_html_source.py` | **NEW** — HTML fetch, JS detection, trafilatura extraction, Playwright PDF fallback |
| `src/pipeline_stages.py` | Added `stage_1b_find_html_source()` + HTML search prompt |
| `src/ragscallion_client.py` | Fixed hardcoded `document.pdf` → dynamic filename + MIME type |
| `src/harness/manifest.py` | Added `HTML_SOURCE_NOT_FOUND`, `HTML_SOURCE_FETCH_FAILED` failure categories |
| `src/runner.py` | Wired Stage 1b HTML fallback into pipeline after Stage 1 fails |
| `src/cli/manifest_admin.py` | **NEW** — Admin CLI for manifest manipulation |
| `ragscallion/server.py` (deployed) | Accepts `.md`, `.txt`, `.html`, `.htm` |
| `ragscallion/ingest.py` (deployed) | Routes non-PDFs through trafilatura, skips Marker |
| `ragscallion/pyproject.toml` (deployed) | Added `trafilatura` optional dependency |

---

## 11. Batch 20 Final Results

```
Total devices: 20
  COMPLETED:      17  ✅ (clean patches, real ports)
  MANUAL_REVIEW:   3  ⚠️  (OUT_OF_SCOPE networking gear)
    - cisco-sf200-24
    - ubiquiti-usw-enterprise-8-poe
    - mikrotik-crs305
```

All 17 completed devices have valid PatchLang with real port definitions. Zero placeholder ports.
