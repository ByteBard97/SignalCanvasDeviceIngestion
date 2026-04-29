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
**Duration:** ~1 second (async submission)

```python
# NEW: Submit PDF to Ragscallion instead of converting locally
corpus_id = node.device_id  # e.g., "yamaha-r08d"
job_id = submit_to_ragscallion(
    pdf_path=node.pdf_path,
    corpus_id=corpus_id,
    source_label=f"{node.manufacturer} {node.model}"
)

if not job_id:
    node.add_failure(3, FailureCategory.RAGDB_INDEXING_FAILED, "Ragscallion submission failed")
    queue_4.add(node)
    return

node.ragscallion_job_id = job_id
node.corpus_id = corpus_id
node.stage_convert_marker = StageStatus.IN_PROGRESS  # Marker running on Linux
node.stage_index_rag = StageStatus.IN_PROGRESS  # Indexing queued

# ENTER POLLING QUEUE (non-blocking)
queue_2.add(node)
```

**API call:**
```bash
curl -X POST http://192.168.0.200:8086/ingest \
  -H "Content-Type: multipart/form-data" \
  -F "file=@/path/to/r08d.pdf" \
  -F "corpus_id=yamaha-r08d" \
  -F "source_label=YAMAHA R08D"
```

**Response:**
```json
{
  "job_id": "a1b2c3d4-...",
  "corpus_id": "yamaha-r08d",
  "status": "queued"
}
```

---

## Polling Loop (NEW)

Harness runs an async polling task that checks Ragscallion periodically:

```python
async def poll_ragscallion_jobs():
    """Poll Ragscallion for completed jobs every 3 seconds."""
    last_check = datetime.now() - timedelta(hours=1)  # Query last hour on startup
    
    while True:
        try:
            # Get all jobs completed since last check
            response = requests.get(
                "http://192.168.0.200:8086/jobs",
                params={
                    "since": last_check.isoformat(),
                    "status": "ready,failed",
                    "limit": 100
                }
            )
            jobs = response.json()["jobs"]
            last_check = datetime.now()
            
            for job in jobs:
                node = manifest.get_node(job["corpus_id"])
                if not node:
                    continue
                
                if job["status"] == "ready":
                    # Move from queue_2 (INGESTING) → queue_3 (READY_TO_EXTRACT)
                    node.ragscallion_job_id = job["job_id"]
                    node.stage_convert_marker = StageStatus.COMPLETED
                    node.stage_index_rag = StageStatus.COMPLETED
                    queue_3.add(node)
                
                elif job["status"] == "failed":
                    # Move from queue_2 → queue_4 (FAILED)
                    node.add_failure(4, FailureCategory.RAGDB_INDEXING_FAILED, job.get("error"))
                    queue_4.add(node)
        
        except Exception as e:
            logger.error(f"Polling error: {e}")
        
        await asyncio.sleep(3)  # Poll every 3 seconds
```

---

### Stage 5: Extract Specs (UPDATED)
**Input:** Device metadata + Ragscallion corpus_id  
**Output:** DeviceSpec JSON  
**Duration:** ~15-30 seconds per device

```python
# UPDATED: Query Ragscallion instead of local RAG
spec_json = extract_specs_via_agent(
    manufacturer=node.manufacturer,
    model=node.model,
    corpus_id=node.corpus_id,  # Now references Ragscallion corpus
    rag_search=lambda q: ragscallion_search(q, corpus=node.corpus_id)
)

if not spec_json:
    node.add_failure(5, FailureCategory.EXTRACTION_FAILED, "Agent couldn't extract specs")
    queue_4.add(node)
    return

node.specs_json = spec_json
node.stage_extract_specs = StageStatus.COMPLETED
```

**Haiku agent now calls:**
```bash
curl "http://192.168.0.200:8086/search?q=signal+routing&corpus=yamaha-r08d&n=5"
```

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

**Ragscallion (Linux box):**
- [ ] Refactor `server.py` to FastAPI
- [ ] Add multipart upload endpoint (`POST /ingest`)
- [ ] Implement SQLite metadata.db (jobs table)
- [ ] Add job lifecycle state machine
- [ ] Implement MARKER_LOCK and INGEST_LOCK
- [ ] Add Marker timeout handling (600s)
- [ ] Implement `GET /jobs?since=X&status=ready,failed`
- [ ] Migrate existing single-corpus data to `legacy`
- [ ] Add corpus_id validation (regex)
- [ ] Update README (FastAPI explanation)

**Harness (Mac):**
- [ ] Add Ragscallion client class (`RagscallionClient`)
- [ ] Implement async polling loop
- [ ] Update stages 3-4 to use Ragscallion API
- [ ] Update stage 5 to query Ragscallion instead of local RAG
- [ ] Refactor queue system to match new state machine
- [ ] Update manifest to track `ragscallion_job_id` and `corpus_id`
- [ ] Add polling timeout and failure handling

---

## Backward Compatibility

**Old Ragscallion (single-corpus):** Deprecated. Will be replaced by multi-corpus version.

**Migration:** One-time migration script converts existing `vectordb/papers` table to `legacy` corpus.

---

## Testing

**Phase 0 (Harness validation):**
1. Spin up new multi-corpus Ragscallion
2. Test job submission for 3 ground truth devices
3. Monitor polling loop
4. Extract specs for all 3 devices
5. Verify all 3 produce valid .patch files

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
