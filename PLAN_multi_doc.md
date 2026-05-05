# Multi-Doc Ingestion — Forward Plan

Context: `device_documents` table is in place. Stage 1 records found PDFs as `spec_sheet`. Stage 2 records local_path. Stage 3 still submits a single PDF per device. Goal: full coverage-set ingestion (spec sheet + user manual + install guide) so downstream extraction has the chunks it needs.

## Phase A — Failure bucketing (informs B, C, D)

Read manifest's `failure_category` distribution and grep recent run logs in `output/` for the actual stage-by-stage failure modes. Output: a short markdown bucket list (`output/failure_buckets.md`) — counts per failure mode and 2-3 representative device_ids per bucket. This is research, not code.

**Why first:** tells us whether Stage 1 fails because Kimi doesn't try the right query, or because the source genuinely lacks a public PDF. Different problems, different fixes.

## Phase B — Stage 1 multi-doc discovery

Modify `stage_1_find_pdf` so after the primary spec_sheet is found, the agent also searches for `{manufacturer} {model} user manual filetype:pdf` and `{manufacturer} {model} installation guide filetype:pdf`. Each found URL is recorded with the appropriate `doc_type`. Failures on the secondary docs are non-fatal — only spec_sheet absence fails the stage.

**New code:**
- Helper `_find_secondary_docs(node, doc_type, query_suffix)` that runs one extra Kimi search and validates with the same content-type/URL-match heuristics.
- Stage 1 calls it twice (manual, install_guide) after spec_sheet success. Each result → `manifest.add_document(...)` with the right doc_type.
- **Cost knob:** `config.find_secondary_docs: bool` (default True) gates the two extra Kimi calls. Set False for cost-sensitive batch runs.
- **Failure semantics:** spec_sheet failure → stage fails (existing behavior). Secondary doc failures → log warning, continue.

**Stage 2** then iterates `manifest.list_documents(device_id)` and downloads each, calling `set_document_local_path` per row. Spec_sheet download failure fails the stage; secondary doc download failures log + continue.

## Phase C — Stage 3 multi-doc submission

Currently `stage_3_4_submit_to_ragscallion` submits `node.pdf_path` and stores `node.ragscallion_job_id`. With multiple docs:

**Design decision:** keep `device_nodes.ragscallion_job_id` as the "primary" job (spec_sheet) for backward compat. Per-doc job IDs live on `device_documents.ragscallion_job_id`. Stage_index_rag = COMPLETED only when *all* docs for that device have `indexed_at` set.

**Failure semantics (mirror Phase B):** spec_sheet must successfully index for stage_index_rag = COMPLETED. Secondary docs that fail to index are logged but do not block the stage — extraction proceeds with whatever indexed.

**Changes:**
- Stage 3: iterate `list_documents(device_id)` where `local_path IS NOT NULL`. For each, call `submit_ingest` and store the per-doc job_id via a new method `set_document_job_id(doc_id, job_id)` (mark_document_indexed conflates submission with completion — split them). The polling loop is the only writer of `indexed_at`.
- Polling loop (`polling_loop.py`): for each device in QUEUE_2, iterate its docs and poll any with `ragscallion_job_id IS NOT NULL AND indexed_at IS NULL`. When the spec_sheet finishes indexing, mark `stage_index_rag COMPLETED` and advance the queue. Secondary docs continue polling in the background; their indexed_at stamps fill in when they finish.
- `device_nodes.ragscallion_job_id` stays as the spec_sheet job for backward compat — `# TODO: remove once polling_loop reads device_documents exclusively`.

## Phase D — Prompt module: `finding_datasheets`

**kimi-code-mcp does NOT load skill files.** So this is a Python module (`src/prompts/finding_datasheets.py`) exporting a constant string that Stage 1 prepends to its Kimi search prompts. Contents:
- Coverage checklist: spec sheet (required), user manual (preferred), install guide (preferred for racked gear)
- Search heuristics: prefer manufacturer domain; reject marketing PDFs (signals: "brochure", "sell-sheet", missing dimensions tables); fallback ladder = manufacturer site → AV-iQ → archive.org → B&H product page cache
- Anti-bot: when a manufacturer requires a form, look for the same PDF mirrored on distributor sites
- Rejection signals captured from Phase A bucketing

## Phase E — Prompt module: `querying_chunks`

Same shape — `src/prompts/querying_chunks.py`, prepended to Stage 5/6 RAG-search prompts. Synonym table for terms that vary across manufacturers (e.g. "Dante channels" ↔ "network audio I/O" ↔ "audio over IP streams"), plus per-doc-type guidance ("look in install guide for power/dimensions, in manual for routing/bridge behavior, in spec sheet for I/O counts").

## Execution order

1. Phase A (research, single agent)
2. Phase D, E (drafts can start in parallel with A; finalize using A's output)
3. Phase B (depends on D)
4. Phase C (depends on B producing multi-doc data)

## Out of scope

- Changing `pdf_url`/`pdf_path` columns on `device_nodes` — they stay as the primary pointer.
- Changing the IngestionNode pydantic model — keep dataclass DeviceNode and SQLite as the canonical state.
- Touching ragscallion server itself.
