# Handoff — Multi-Doc Ingestion Work In Progress

## What this work is
Geoff wants the device-ingestion pipeline to support multiple PDFs per device (spec sheet + user manual + install guide) so Stage-5 extraction has the chunks it needs. Current pipeline only ingests one PDF per device.

## How Geoff wants to work
- Don't ask "what next" between obvious steps. If unsure, write a plan, run `advisor()`, then execute.
- Use kimi agents (`mcp__kimi-code-mcp__kimi_agent`) to do code work in parallel.
- After kimi finishes, dispatch a Claude reviewer (`Agent` with `subagent_type: feature-dev:code-reviewer`) on the result.
- Be terse. Don't add doc/comment bloat.
- **Never `rm`** — use `trash`.
- Code rules in `/Users/ceres/Desktop/SignalCanvas/CLAUDE.md` and `ClaudeCodeRules.md` apply.

## What's done

### Schema (already merged into manifest.py)
Added `device_documents` table to `src/harness/manifest.py`. Columns: `id, device_id, doc_type, url, local_path, ragscallion_job_id, indexed_at, created_at`. UNIQUE(device_id, doc_type, url). Backfilled 35 existing rows as `spec_sheet` from `device_nodes.pdf_url/pdf_path`. Verified idempotent.

New constants: `DOC_TYPE_SPEC_SHEET`, `DOC_TYPE_USER_MANUAL`, `DOC_TYPE_INSTALL_GUIDE`, `DOC_TYPE_API_DOC`, `DOC_TYPE_OTHER`.

New methods on `Manifest`:
- `add_document(device_id, doc_type, *, url=None, local_path=None, ragscallion_job_id=None)` — INSERT OR IGNORE, idempotent.
- `list_documents(device_id, doc_type=None)`
- `mark_document_indexed(doc_id, ragscallion_job_id=None)` — sets indexed_at + optional job_id.
- `set_document_local_path(device_id, doc_type, url, local_path)` — UPDATE for Stage 2 to fill in local_path after download.

### Stage 1 + 2 wiring (already in pipeline_stages.py)
- Stage 1 (`stage_1_find_pdf`) records every found URL via `manifest.add_document(node.device_id, DOC_TYPE_SPEC_SHEET, url=...)` on all 3 success paths (AV-iQ ~L522, Kimi ~L599, DDG fallback ~L618).
- Stage 2 (`stage_2_download_pdf`) calls `manifest.set_document_local_path(...)` on success ~L784.
- `pdf_url`/`pdf_path` on `device_nodes` are unchanged — they remain the "primary doc" pointer for backward compat.

### Phase D — `src/prompts/finding_datasheets.py`
Prompt-string constants for Stage 1 to find spec sheets / user manuals / install guides. Exports `SPEC_SHEET_GUIDANCE`, `USER_MANUAL_GUIDANCE`, `INSTALL_GUIDE_GUIDANCE`, `FALLBACK_LADDER`, `REJECTION_SIGNALS`, `ANTIBOT_TIPS`, and `build_doc_search_prompt(manufacturer, model, doc_type, exclude_urls)`. Reviewed and fixed: (1) "brochure" no longer false-rejected (Yamaha publishes "Brochure & Specifications" PDFs); (2) JSON output instruction uses two labeled examples instead of inline `or`.

### Phase E — `src/prompts/querying_chunks.py`
Prompt-string constants for Stage 5+ RAG queries. Exports `SPEC_SYNONYMS` dict, `DOC_TYPE_GUIDANCE`, and `build_chunk_query_prompt(spec_target, available_doc_types)`. Reviewed and fixed: (1) `analog_outputs` extended with Aux/Bus/Mix/Matrix/Group/Sub outs for consoles; (2) new `rf_io` key for wireless receiver topology (antenna, BNC, cascade); (3) unknown spec_target now logs a warning instead of silently degenerating.

### Phase A — abandoned
Manifest's `failure_category` column is NULL for all 36 devices — historical failures aren't retained across reruns. Useful failure data only lives in `output/*.log` files which are too large to scan reactively. Not worth the cost right now.

## What's next

### Phase B — DONE
`stage_1_find_pdf` now refactored: spec_sheet path runs in `_find_spec_sheet_url` (holds FIND_PDF_SEMAPHORE); after that returns success, `_gather_secondary_docs` runs user_manual + install_guide searches in parallel via `_find_secondary_doc` (each acquires the same semaphore independently — no nesting/deadlock). Each is single-attempt, best-effort: failures log a warning and continue. Stage 2 follows the spec_sheet download with `_download_secondary_docs` which iterates `manifest.list_documents` and downloads each non-spec-sheet to `cache_dir / "<device_id>__<doc_type>.pdf"`. New tests in `tests/test_stage_1_find_pdf.py::TestSecondaryDocSearch` cover happy path, URL mismatch rejection, and exception swallowing. Cost knob: `config.find_secondary_docs` (default True).

### Phase C — Stage 3 multi-doc ragscallion submission (ready to start)
Currently `stage_3_4_submit_to_ragscallion` submits `node.pdf_path` and stores `node.ragscallion_job_id`. Change:
- Iterate `list_documents(device_id)` where `local_path IS NOT NULL`. For each, call `submit_ingest` and store the per-doc job_id. **Need new helper:** `set_document_job_id(doc_id, job_id)` that does NOT touch indexed_at — split submission from index-completion (current `mark_document_indexed` conflates them).
- Polling loop (`src/polling_loop.py`): for each device in QUEUE_2, iterate its docs and poll any with `ragscallion_job_id IS NOT NULL AND indexed_at IS NULL`. When the spec_sheet finishes indexing, mark `stage_index_rag COMPLETED` and advance the queue. Secondary docs continue polling in background.
- `device_nodes.ragscallion_job_id` stays as spec_sheet job — add `# TODO: remove once polling_loop reads device_documents exclusively`.

## Key files
- `src/harness/manifest.py` — schema + CRUD
- `src/pipeline_stages.py` — Stage 1/2/3 etc.
- `src/polling_loop.py` — Stage 3 ragscallion polling
- `src/prompts/finding_datasheets.py` — Stage 1 search prompts
- `src/prompts/querying_chunks.py` — Stage 5+ RAG query prompts
- `src/config.py` — runtime settings
- `output/ingestion.db` — populated SQLite (36 devices)
- `output/manifest.db` — empty SQLite (different DB used by tests/dev)
- `PLAN_multi_doc.md` — the full plan; this handoff is the abridged status

## Gotchas
- **kimi-code-mcp does NOT load skill files.** Don't write `SKILL.md` and expect Kimi to pick it up. Prompts must be embedded in the Python prompt-modules approach (already chosen — `src/prompts/`).
- **Kimi agent timeouts:** the `kimi_agent` tool times out around 450-600s on long tasks and the wrapper output can blow past the token limit. Keep tasks tightly scoped; tell Kimi "reply under 400 words."
- **`git diff` is misleading** in this repo because there are pre-existing uncommitted changes in `pipeline_stages.py` and others — count actual function defs (`grep -c "^def "`), don't trust diff line counts.
- **Python:** use `.venv/bin/python` (`python` and `python3` aren't on PATH; `uv run` fails because `patchlang-python` dep isn't in registry).
- **Two SQLite DBs:** `output/ingestion.db` is the populated one (36 real devices); `output/manifest.db` is empty.
- **Ragscallion:** server runs at `192.168.0.200:8086` on a Linux box (Geoff has SSH as `your-username@localhost`). Vector store. Schema-flexible — already accepts arbitrary `submit_ingest` calls; multi-doc per device key works server-side without changes.

## Tasks (state at handoff)
1-2 completed (Stage 1/2 wiring), 3 deleted (superseded by 6), 4 completed-with-note (Phase A abandoned), 5 completed (Phase B), 7-10 completed (D+E plus reviewer fixes). 6 (Phase C) is ready to start.

## Last advisor feedback worth keeping
- For Phase C, partial-failure semantics: spec_sheet must succeed for stage_index_rag = COMPLETED; secondaries are best-effort (mirrors Phase B).
- Add `config.find_secondary_docs` cost knob for Phase B.
- Mark `device_nodes.ragscallion_job_id` with `# TODO: remove` so it doesn't silently linger.
