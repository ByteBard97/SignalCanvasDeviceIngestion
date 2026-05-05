# Handoff — Device Ingestion Pipeline

## Current State (2026-05-05 EOD)

All 46 devices in `output/ingestion.db` are `queue_5` COMPLETED. No failures remain.

### Latest Fixes (uncommitted)
1. **SSL retry** — `stage_2_download_pdf` detects SSL errors and retries with `verify=False` before falling back to Kimi.
2. **Remove filename-based URL filtering** — Deleted `_url_likely_matches_model`, `_looks_opaque`, `_model_tokens`, `_TRUSTED_PDF_DOMAINS`. Trusts the agent's judgment; quality enforced post-download via 30KB minimum size check.
3. **403/429/503 fallback handling** — Stage 2 no longer hard-fails on bot-protection HTTP errors. Treats them as connection-level failures and asks Kimi (or DDG next-candidate) for an alternate URL.
4. **DDG next-candidate fallback for 403s** — When Stage 2 fallback is triggered by a 403/429/503, it re-runs DDG search (excluding the blocked URL) so the next candidate is tried *before* asking Kimi.
5. **pytest config** — Added `pythonpath = src` to `pytest.ini` and `pyproject.toml` so tests resolve `from src.xxx` imports correctly.
6. **Test fixes** — Updated `test_stage_2_download_pdf.py` mock content to exceed the new 30KB minimum threshold.

### Batch Validation Results
- 5-device run: 4/5 → 80%
- 6-device retry run: 6/6 → 100%
- `qx1832` retry: 1/1 → 100% (B&H 403 → sunrise-trading.com 200 via DDG next-candidate)

### Known Future Concern
Some reseller-sourced PDFs (e.g. `sunrise-trading.com`) yield `extraction_confidence=LOW`. Pipeline warns but still completes. Future improvements: manual URL override or headless-browser download for major retailers.

## What's Done (merged to main)

### Multi-doc ingestion (Phases A–E)
Schema `device_documents`, parallel secondary doc search, best-effort submission, polling loop per-doc handling. 255 tests green.

### Phase B — Secondary doc search
`stage_1_find_pdf` refactored: spec_sheet in `_find_spec_sheet_url`; user_manual + install_guide in parallel via `_find_secondary_doc`. Each acquires `FIND_PDF_SEMAPHORE` independently. Failures log warning and continue.

### Phase C — Multi-doc submission + polling
`stage_3_4_submit_to_ragscallion` submits spec_sheet then best-effort secondaries. Polling loop `_process_job_result` per-job: matches by job_id, stamps `indexed_at`, only spec_sheet ready advances node queue_2→queue_3.

### Phase D+E — Prompt modules
`src/prompts/finding_datasheets.py` and `src/prompts/querying_chunks.py` with `build_doc_search_prompt` and `build_chunk_query_prompt`.

## Working Style
- Don't ask "what next" between obvious steps.
- Use Kimi agents for parallel code work.
- Be terse. No doc/comment bloat.
- **Never `rm`** — use `trash`.
- Code rules in `/Users/ceres/Desktop/SignalCanvas/CLAUDE.md` apply.

## Key Files
- `src/harness/manifest.py` — schema + CRUD
- `src/pipeline_stages.py` — all stages
- `src/polling_loop.py` — Ragscallion polling
- `src/prompts/finding_datasheets.py` — Stage 1 search prompts
- `src/prompts/querying_chunks.py` — Stage 5+ RAG query prompts
- `src/config.py` — runtime settings
- `src/runner.py` — pipeline orchestration + `_summarize_device_status`
- `output/ingestion.db` — 46 devices, all queue_5 completed

## Test Suite
Run with: `PYTHONPATH=src .venv/bin/python -m pytest tests/ -q`
Current: **255 passed**

## What's Next
1. **Real-batch validation** — `random_10_devices_v2.txt` end-to-end through multi-doc pipeline.
2. **Retry queue** — `retry_7_devices.txt` if any new failures surface.
