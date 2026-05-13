# Pipeline Hardening: Kill-Safety & Routing — 2026-05-13

All bugs were the same class: a stage transition wrote state that the restart
pickup logic didn't cover, leaving devices permanently stranded.

## Fixes Applied

### runner.py — Restart-survival pickup fixes

| Bug | State | Fix |
|-----|-------|-----|
| queue-0 sku-done skip | `q0, stage_resolve_sku=DONE, stage_find_pdf=0` — Stage 1 never ran after restart | Added `queue_0_sku_done` pickup |
| queue-0 pdf-found skip | `q0, stage_find_pdf=DONE, stage_download_pdf=0` — Stage 2 never ran | Added `queue_0_pdf_found` pickup |
| queue-0 downloaded skip | `q0, stage_download_pdf=DONE, stage_index_rag=0` — Stage 3-4 never ran | Added `queue_0_downloaded` pickup |
| Stage 2 download retry | `q1, stage_download_pdf=FAILED` — filter checked `==NOT_STARTED` not `!=COMPLETED` | Changed filter to `not in (COMPLETED, IN_PROGRESS)` |
| Stage 3-4 RAG retry | `q4, stage_index_rag=FAILED, retryable` — no queue_4 retry path existed | Added `queue_4_rag_retry` pickup |
| Stage 1b stranding | `q4, category=HTML_SOURCE_NOT_FOUND` — retry only read queue_1 | Extended pickup to also read queue_4 |
| Scope check re-run | All 186 queue_0 devices re-classified via Moonshot on every loop pass | Skip nodes with `stage_resolve_sku != NOT_STARTED` |

### polling_loop.py

| Bug | Fix |
|-----|-----|
| RAG failure left `stage_index_rag=IN_PROGRESS` | Set `stage_index_rag=STAGE_FAILED` when job status == "failed" |

### kimi_runner.py

- Added `start_new_session=True` + `os.killpg()` for reliable subprocess tree kill on macOS timeout

### ragscallion_client.py

- Upload timeout: 10s → 120s (59MB and 28MB PDFs were timing out)

### classify_device.py

- Added 15 explicit rules for non-Dante devices being misclassified as `dante_*`
- Rewrote LLM fallback prompt to define Dante explicitly and default to `generic`

### runner.py — Queue-5 promotion bug

- Successfully completed queue-4 retry nodes were not promoted to queue-5 — added `node.queue = QUEUE_5_COMPLETED` on stage 6-7 success

## New Infrastructure

### claude_runner.py (new)

Haiku/Kimi routing by device complexity:
- Stage 1 PDF finding → **Kimi direct** (web search quality)
- Stage 5 simple classes (generic, speaker, camera) → **Haiku first**, Kimi fallback
- Stage 5 complex classes (dante_stagebox, dsp_processor, mixer) → **Kimi direct**
- Stage 5 re-extraction (attempt 2+) → **Kimi direct**

20-device test: 15 Haiku, 2 Kimi direct, 0 fallbacks, 19/20 extracted.

## Batches Status

| Batch | Devices | Completed | Notes |
|-------|---------|-----------|-------|
| batch_100_es_only_v3 | 100 | 89 | Exported + committed to library |
| batch_200_es_only_v1 | 200 | In progress | Running with all fixes |
| batch_300_es_only_v1 | 200 | Ready | `batch_300_es_only_v1.txt` generated, seed=123 |

Pool remaining: 1282 devices (785 Extron, 25 Yamaha, 24 Allen & Heath, ...)
