# Harness Integration with Multi-Corpus Ragscallion

## Overview

The ingestion harness will be refactored to work with Ragscallion's new multi-corpus API. Instead of managing its own RAG database, the harness delegates PDF ingestion to Ragscallion and polls for completion.

## Queue Architecture (Revised)

```
Device Input Queue (5 states)
├── 0. INITIAL (waiting for PDF download)
├── 1. CANNOT_FIND_PDF (escalate to human)
├── 2. INGESTING (PDF submitted to Ragscallion, polling for completion)
├── 3. READY_TO_EXTRACT (Ragscallion indexed, can now query specs)
└── 4. FAILED (extraction/compilation failed, human review)
```

## Pipeline Stages (Updated for Ragscallion API)

### Stage 1: Find PDF
**Input:** Device metadata (manufacturer, model)  
**Output:** PDF URL or failure  
**Duration:** ~5-10 seconds

```python
# Uses WebSearch + Haiku validation (unchanged)
pdf_url = find_pdf_url(manufacturer, model)
if not pdf_url:
    node.add_failure(1, FailureCategory.PDF_NOT_FOUND, "No PDF found")
    queue_1.add(node)  # Escalate to human
    return

node.pdf_url = pdf_url
node.stage_find_pdf = StageStatus.COMPLETED
```

---

### Stage 2: Download PDF
**Input:** PDF URL  
**Output:** Local PDF file  
**Duration:** ~30-60 seconds

```python
# Download to local cache (unchanged)
pdf_path = download_pdf(node.pdf_url, timeout=60)
if not pdf_path:
    node.add_failure(2, FailureCategory.PDF_DOWNLOAD_FAILED, "Download failed")
    queue_1.add(node)
    return

node.pdf_path = pdf_path
node.stage_download_pdf = StageStatus.COMPLETED
```

---

### Stage 3-4: Submit to Ragscallion (NEW)
**Input:** Local PDF file  
**Output:** job_id from Ragscallion  
**Duration:** ~1 second (async submission, with retries up to 3 attempts)

```python
# NEW: Submit PDF to Ragscallion instead of converting locally
corpus_id = node.device_id  # e.g., "yamaha-r08d"
source_label = f"{node.manufacturer} {node.model}"

job_id = None
for attempt in range(1, 4):
    try:
        job_id = submit_to_ragscallion(
            pdf_path=node.pdf_path,
            corpus_id=corpus_id,
            source_label=source_label,
            on_conflict="error"  # Reject accidental re-submissions
        )
        break  # Success
    except RagscallionCollisionError:
        # Duplicate source_label in existing corpus — human decision needed
        node.add_failure(3, FailureCategory.RAGDB_COLLISION, 
                        f"source_label '{source_label}' already in corpus '{corpus_id}'")
        queue_4.add(node)
        return
    except (RagscallionUnavailable, RagscallionTimeout) as e:
        if attempt < 3:
            wait_time = [1, 4, 16][attempt - 1]  # Exponential backoff
            logger.info(f"Ragscallion submission attempt {attempt}/3 failed. Retrying in {wait_time}s...")
            await asyncio.sleep(wait_time)
        else:
            node.add_failure(3, FailureCategory.RAGSCALLION_UNAVAILABLE,
                           f"Failed to submit after 3 retries: {e}")
            queue_4.add(node)
            return

if not job_id:
    node.add_failure(3, FailureCategory.RAGDB_INDEXING_FAILED, "Unexpected: no job_id returned")
    queue_4.add(node)
    return

node.ragscallion_job_id = job_id
node.corpus_id = corpus_id
node.ragscallion_submitted_at = datetime.now()
node.stage_convert_marker = StageStatus.IN_PROGRESS  # Marker running on Linux
node.stage_index_rag = StageStatus.IN_PROGRESS  # Indexing queued

# ENTER POLLING QUEUE (non-blocking)
queue_2.add(node)
manifest.persist()
```

**API call:**
```bash
curl -X POST http://localhost:8086/ingest \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/r08d.pdf" \
  -F "corpus_id=yamaha-r08d" \
  -F "source_label=YAMAHA R08D" \
  -F "on_conflict=error"
```

**Response:**
```json
{
  "job_id": "a1b2c3d4-...",
  "corpus_id": "yamaha-r08d",
  "status": "queued"
}
```

**Error responses:**
```json
{
  "error": "collision",
  "message": "source_label 'YAMAHA R08D' already exists in corpus 'yamaha-r08d'"
}
```

---

## Polling Loop (NEW)

Harness runs an async polling task that checks Ragscallion periodically:

```python
async def poll_ragscallion_jobs():
    """Poll Ragscallion for completed jobs every 3 seconds."""
    last_check = datetime.now() - timedelta(hours=1)  # Query last hour on startup
    consecutive_failures = 0
    
    while True:
        try:
            # CRITICAL: Capture timestamp BEFORE request to avoid race condition
            request_start = datetime.now()
            
            response = requests.get(
                "http://localhost:8086/jobs",
                params={
                    "since": last_check.isoformat(),
                    "status": "ready,failed",
                    "limit": 100
                },
                timeout=10
            )
            response.raise_for_status()
            
            data = response.json()
            jobs = data["jobs"]
            # Use Ragscallion's server time, not Mac's clock, to avoid skew
            last_check = datetime.fromisoformat(data["server_now"].replace('Z', '+00:00'))
            consecutive_failures = 0  # Reset on success
            
            for job in jobs:
                node = manifest.get_node(job["corpus_id"])
                if not node:
                    continue
                
                if job["status"] == "ready":
                    # Move from queue_2 (INGESTING) → queue_3 (READY_TO_EXTRACT)
                    node.ragscallion_job_id = job["job_id"]
                    node.stage_convert_marker = StageStatus.COMPLETED
                    node.stage_index_rag = StageStatus.COMPLETED
                    node.ragscallion_completed_at = datetime.now()
                    queue_3.add(node)
                    manifest.persist()
                
                elif job["status"] == "failed":
                    # Move from queue_2 → queue_4 (FAILED)
                    node.add_failure(4, FailureCategory.RAGDB_INDEXING_FAILED, job.get("error"))
                    queue_4.add(node)
                    manifest.persist()
        
        except (requests.Timeout, requests.ConnectionError, requests.HTTPError) as e:
            consecutive_failures += 1
            
            if consecutive_failures == 3:
                logger.warning(f"Polling failed 3 times: {e}. Continuing to retry.")
            elif consecutive_failures == 10:
                logger.error(f"Polling failed 10 times. Ragscallion may be unavailable. Error: {e}")
            else:
                logger.debug(f"Polling error (attempt {consecutive_failures}): {e}")
        
        except Exception as e:
            logger.error(f"Unexpected polling error: {e}")
        
        # Check for stale jobs in queue_2 (nodes waiting >15 min with no update)
        for node in queue_2.all():
            age = datetime.now() - node.ragscallion_submitted_at
            if age > timedelta(minutes=15):
                logger.warning(f"Node {node.device_id} in queue_2 for {age.total_seconds()/60:.1f} min. Check Ragscallion health.")
                node.marked_suspicious = True
        
        await asyncio.sleep(3)  # Poll every 3 seconds
```

---

### Stage 5: Extract Specs (UPDATED)
**Input:** Device metadata + Ragscallion corpus_id  
**Output:** DeviceSpec JSON  
**Duration:** ~15-30 seconds per device (run in pool of 5 concurrent)

```python
# UPDATED: Query Ragscallion instead of local RAG
# Run with asyncio.Semaphore(5) to allow concurrent extractions
# Reduces 30+ hour serial time to ~10 hours

extraction_semaphore = asyncio.Semaphore(5)

async def extract_specs_for_node(node):
    async with extraction_semaphore:  # Max 5 concurrent Haiku calls
        spec_json = extract_specs_via_agent(
            manufacturer=node.manufacturer,
            model=node.model,
            corpus_id=node.corpus_id,  # References Ragscallion corpus
            rag_search=lambda q: ragscallion_search(q, corpus=node.corpus_id)
        )
        
        if not spec_json:
            node.add_failure(5, FailureCategory.EXTRACTION_FAILED, 
                           "Agent couldn't extract specs")
            node.failure_stage = 5
            node.failure_retryable = True
            queue_4.add(node)
            return False
        
        node.specs_json = spec_json
        node.stage_extract_specs = StageStatus.COMPLETED
        manifest.persist()
        return True

# Batch process queue_3 nodes
tasks = [extract_specs_for_node(node) for node in queue_3.all()]
await asyncio.gather(*tasks)
```

**Haiku agent now calls:**
```bash
curl "http://localhost:8086/search?q=signal+routing&corpus=yamaha-r08d&n=5"
```

**Concurrency rationale:** I/O-bound calls, don't compete with GPU. At 5 concurrent Haiku calls, well under Anthropic API rate limits (4000 RPM for Haiku tier 1). Anthropic's rate limit headers will be binding constraint. If hitting 429s, reduce to 3.

---

### Stage 6: Generate PatchLang (UNCHANGED)
**Input:** DeviceSpec JSON  
**Output:** `.patch` source text  
**Duration:** ~5 seconds

```python
patch_source = generate_patch_from_spec(node.specs_json)
node.patch_source = patch_source
node.stage_generate_patch = StageStatus.COMPLETED
```

---

### Stage 7: Validate Patch (UNCHANGED)
**Input:** `.patch` source text  
**Output:** Valid patch or error  
**Duration:** ~2 seconds

```python
try:
    patchlang_python.check(node.patch_source)
    node.is_valid = True
    node.stage_validate_patch = StageStatus.COMPLETED
    
    # Write to output
    output_path = settings.stdlib_output / f"{node.device_id}.patch"
    output_path.write_text(node.patch_source)
except PatchlangError as e:
    node.add_failure(7, FailureCategory.PATCH_VALIDATION_FAILED, str(e))
    node.validation_errors = e.diagnostics
    queue_4.add(node)
```

---

## Ragscallion API Contract

**New endpoints the harness uses:**

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/ingest` | POST | Submit PDF for async conversion |
| `/jobs` | GET | Poll for completed jobs (with `since` filter) |
| `/search` | GET | Query indexed corpus during extraction |
| `/resolve` | GET | Resolve device name to corpus_id (optional) |

**No changes to existing endpoints** (`/stats`, `/sources`, `/health` remain unchanged).

---

## State Transitions

```
Device Input
    ↓
Stage 1 (Find PDF)
    ├─ SUCCESS → Stage 2
    └─ FAILURE → Queue 1 (escalate to human)
    ↓
Stage 2 (Download PDF)
    ├─ SUCCESS → Stage 3-4
    └─ FAILURE → Queue 1
    ↓
Stage 3-4 (Submit to Ragscallion)
    ├─ SUCCESS → Queue 2 (polling)
    └─ FAILURE → Queue 4 (manual)
    
Queue 2 (Polling Loop, non-blocking)
    ↓
(Every 3s: check Ragscallion /jobs endpoint)
    ├─ job.status=ready → Queue 3 (ready to extract)
    └─ job.status=failed → Queue 4 (manual)
    
Queue 3 (Ready to Extract)
    ↓
Stage 5 (Extract Specs via Agent)
    ├─ SUCCESS → Stage 6
    └─ FAILURE → Queue 4
    ↓
Stage 6 (Generate Patch)
    ├─ SUCCESS → Stage 7
    └─ FAILURE → Queue 4
    ↓
Stage 7 (Validate Patch)
    ├─ VALID → Write to output (✅ COMPLETE)
    └─ INVALID → Queue 4 (manual)
```

---

## Implementation Checklist

### Priority 0 (Must-have for Phase 0)

**Ragscallion (Linux box) — Phase A:**
- [ ] Refactor `server.py` to FastAPI + uvicorn
- [ ] Add `POST /ingest` multipart upload endpoint
  - [ ] Accept `corpus_id`, `source_label`, `on_conflict` parameters
  - [ ] Return `{ "job_id", "corpus_id", "status", "server_now" }`
- [ ] Implement SQLite metadata.db with jobs table
- [ ] Add job lifecycle state machine (queued → awaiting_marker → converting → ingesting → ready/failed)
- [ ] Implement MARKER_LOCK and INGEST_LOCK (asyncio.Lock)
- [ ] Add Marker timeout handling (600s, kill subprocess, mark failed, release lock)
- [ ] **CRITICAL: Implement `GET /jobs?since=X&status=ready,failed`**
  - [ ] Return all jobs since timestamp (RFC3339)
  - [ ] **Echo back `server_now` field in response** (solves clock skew)
  - [ ] Filter by status, limit to 100
  - [ ] Order by updated_at descending
- [ ] Add corpus_id validation (regex: `^[a-z0-9][a-z0-9_-]{0,63}$`)
- [ ] Handle collision: `on_conflict=error|append|replace`
- [ ] Add `/health` endpoint
- [ ] Update README (FastAPI explanation, dependency list)

**Harness (Mac) — Core Pipeline:**
- [ ] Create RagscallionClient class with retry logic (3 attempts, backoff: 1s/4s/16s)
- [ ] **Implement SQLite manifest.db with crash recovery**
  - [ ] Device node schema with all failure metadata
  - [ ] Startup recovery: re-poll Ragscallion for queue_2 nodes, restart other stages
- [ ] Implement async polling loop (NEW)
  - [ ] **Capture `last_check` BEFORE request** (race condition fix)
  - [ ] Use Ragscallion's `server_now` for next poll's `since` parameter
  - [ ] Consecutive failure thresholds (3 = WARN, 10 = ERROR)
  - [ ] Per-node age check: 15min in queue_2 → log suspicious
- [ ] Update stages 1-2 (unchanged, just persist to manifest)
- [ ] **Update stages 3-4: retry-with-backoff on submission**
  - [ ] Catch collision errors (source_label exists) → queue_4
  - [ ] Catch unavailable (5xx/timeout) → retry, then queue_4
- [ ] Update stage 5: add `asyncio.Semaphore(5)` for concurrent extraction
- [ ] Update stages 6-7 (generate, validate — unchanged, just store metadata)
- [ ] Refactor queue system (0-4 with rich metadata)
- [ ] Implement failure triage metadata (stage, category, retryable, attempts)

### Priority 1 (Week 1 completion)

**Ragscallion Phase B:**
- [ ] Migrate existing single-corpus data to `legacy` corpus
- [ ] Add `/storage` endpoint for accounting

**Harness Testing:**
- [ ] Phase 0 test harness (3 ground truth devices, end-to-end)
  - [ ] Verify submission, polling, completion, extraction, validation
  - [ ] Test collision detection and error handling
  - [ ] Simulate Ragscallion failures → verify error thresholds
  - [ ] Simulate Mac crash → verify manifest recovery

### Priority 2 (Week 2+, defer if tight)

**Ragscallion Phase C:**
- [ ] Server-Sent Events (SSE) for notifications
- [ ] Webhooks

**Harness Observability:**
- [ ] Cost tracking (per-stage token accounting)
- [ ] Queue UI (triage queue_4 by failure category/stage)

---

## Backward Compatibility

**Old Ragscallion (single-corpus):** Deprecated. Will be replaced by multi-corpus version.

**Migration:** One-time migration script converts existing `vectordb/papers` table to `legacy` corpus.

---

## Manifest Persistence & Crash Recovery (NEW)

**Storage:** SQLite manifest file at `./manifest.db`

**Schema:**
```sql
CREATE TABLE device_nodes (
    device_id TEXT PRIMARY KEY,
    manufacturer TEXT,
    model TEXT,
    corpus_id TEXT,
    
    -- Stages (numeric: 0=pending, 1=in_progress, 2=completed)
    stage_find_pdf INTEGER,
    stage_download_pdf INTEGER,
    stage_convert_marker INTEGER,
    stage_index_rag INTEGER,
    stage_extract_specs INTEGER,
    stage_generate_patch INTEGER,
    stage_validate_patch INTEGER,
    
    -- Ragscallion state
    pdf_url TEXT,
    pdf_path TEXT,
    ragscallion_job_id TEXT,
    ragscallion_submitted_at TIMESTAMP,
    ragscallion_completed_at TIMESTAMP,
    marked_suspicious BOOLEAN DEFAULT FALSE,
    
    -- Output
    specs_json TEXT,
    patch_source TEXT,
    
    -- Failure tracking
    failure_stage INTEGER,
    failure_category TEXT,
    failure_message TEXT,
    failure_retryable BOOLEAN,
    failure_attempts INTEGER DEFAULT 0,
    failure_at TIMESTAMP,
    
    -- Metadata
    queue INTEGER,  -- 0=initial, 1=cannot_find, 2=ingesting, 3=ready, 4=failed
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);
```

**Crash recovery on startup:**
- Nodes in stages 0-4 (find PDF, download, submit, polling) → Resume from current stage
- Nodes in polling queue (queue_2) → Re-query Ragscallion with stored `ragscallion_job_id`:
  - If Ragscallion says `ready` → Advance to queue_3
  - If Ragscallion says `failed` → Move to queue_4
  - If job not found (Ragscallion was wiped) → Restart from stage 3 (re-submit)
- Nodes in mid-stage (extracting, generating, validating) → Restart that stage (stages should be idempotent)

**Persistence pattern:**
- Call `manifest.persist()` after every state change
- Use SQLite WAL mode for concurrent reads during polling writes
- ~50 lines of code total

---

## Failure Metadata (Enhanced)

Add rich fields to every device node for triage and observability:

```python
node.failure_stage: int          # Which stage failed (3, 5, 6, 7)
node.failure_category: enum      # PDF_NOT_FOUND, RAGDB_COLLISION, EXTRACTION_FAILED, etc.
node.failure_message: str        # Error details for human review
node.failure_retryable: bool     # Can this be auto-retried? (stage 5 yes, stage 7 maybe)
node.failure_attempts: int       # How many times we've tried this device
node.failure_at: datetime        # When the failure occurred
```

**Queue 4 (manual review) processing:**
- UI filters by `failure_stage` or `failure_category` to route to responsible team
- Stage 3 failures (PDF download) → Try different URL
- Stage 5 failures (extraction) → Refine agent prompt and retry
- Stage 7 failures (validation) → Review PatchLang errors, fix generation logic

---

## Testing

**Phase 0 (Harness validation):**
1. Spin up new multi-corpus Ragscallion
2. Test job submission for 3 ground truth devices (with retry/collision cases)
3. Monitor polling loop (verify timestamp ordering, server_now handling)
4. Extract specs for all 3 devices (verify concurrency with Semaphore(5))
5. Verify all 3 produce valid .patch files
6. Simulate Ragscallion failure → verify error thresholds (3, 10) log correctly
7. Simulate Mac crash mid-pipeline → verify manifest recovery works

**Success criteria:**
- All 3 ground truth patches parse and validate
- Polling loop correctly handles concurrent job transitions
- Crash recovery resumes pipeline without losing state
- Error logging surfaces problems clearly to operator

**Phase 1:** Scale to 50 devices, measure polling loop latency and job completion times.

---

## Timeline

**Week 1:**
- Implement Ragscallion multi-corpus API (Phase A)
- Implement harness Ragscallion client + polling loop
- Run Phase 0 test harness

**Week 2:**
- Implement Ragscallion Phase B (lifecycle, storage accounting)
- Run Phase 1 test harness (50 devices)
- Measure and optimize

**Week 3+:**
- Scale to full device set
- Add Phase C features (SSE, webhooks) if needed
