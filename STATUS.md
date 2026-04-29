# SignalCanvas Device Ingestion Pipeline — Development Status

**As of 2026-04-29**

## ✅ Completed

### Infrastructure
- [x] Ragscallion RAG server running on 192.168.0.200:8086 (resurrected)
- [x] SSH access to Linux box verified (your-username@localhost)
- [x] Repository created and shared with Reid
- [x] REQUIREMENTS.md — Complete 7-stage pipeline specification
- [x] IMPLEMENTATION.md — Architecture and integration design

### Core Harness
- [x] `src/harness/manifest.py` — SQLite-backed device state persistence
  - IngestionNode: Tracks single device through all 7 stages
  - IngestionManifest: SQLite persistence with full CRUD operations
  - StageStatus enum: NOT_STARTED, IN_PROGRESS, COMPLETED, FAILED, SKIPPED
  - FailureCategory enum: 11 failure types for categorized retry logic

- [x] `src/harness/state.py` — Execution state tracking
  - IngestionState: Current device, phase, progress, checkpoints

### Data Models
- [x] `src/models/device.py` — Device specification models
  - TemplateMetadata: Manufacturer, model, category, Dante chipset, RF config
  - PortDefinition: Port arrays, connectors, protocols, attributes
  - BridgeRule: Signal routing rules
  - DeviceSpec: Complete device specification with bridges, slots, streams
  - DeviceInput: Raw device metadata before extraction

### Pipeline Orchestrator
- [x] `src/pipeline.py` — LangGraph-based orchestrator
  - DeviceIngestionPipeline: Graph construction with 10 nodes
  - 7 pipeline stages (stub implementations)
  - Conditional routing from complete_device → next_device or metrics
  - Checkpoint support for resumability

### Configuration
- [x] `src/config.py` — Settings with defaults
  - Ragscallion endpoints (host, port, SSH user, script path)
  - Output directories (stdlib, manifests, PDF cache)
  - Phase parameters (3, 50, 1500, remaining)
  - Timeouts and retry settings

### Phase 0 Testing
- [x] `tests/fixtures/phase0_ground_truth.json` — 3 ground truth devices
  - YAMAHA R08D (8-ch Dante→XLR)
  - Audinate AVIO-AI2 (2-ch analog→Dante)
  - YAMAHA CL5 (console)

- [x] `tests/test_phase0.py` — Unit and integration tests
  - Manifest persistence and retrieval
  - Device state tracking
  - Pipeline initialization
  - Ragscallion connectivity check
  - Phase 0 fixture validation

- [x] `tests/README.md` — Test documentation
- [x] `pytest.ini` — Pytest configuration
- [x] `.env.example` — Environment template
- [x] `pyproject.toml` — Python dependencies
- [x] `requirements.txt` — Pip requirements

## 🔄 Next Steps (In Priority Order)

### Phase 0: Harness Validation (Week 1)

**Goal:** Validate the pipeline works end-to-end with 3 known devices before scaling.

#### 1. Implement Stage 1: Find PDF
- [ ] Create `src/stages/find_pdf.py`
- [ ] Use Claude's WebSearch tool (free)
- [ ] Haiku validation of PDF URLs
- [ ] Expected result: PDF URLs for all 3 devices
- **Target success:** 3/3 (100%)

#### 2. Implement Stage 2: Download PDF
- [ ] Create `src/stages/download_pdf.py`
- [ ] HTTP download with timeout
- [ ] File validation (magic bytes check for PDF)
- [ ] Handle redirects, auth errors, dead links
- **Target success:** 3/3 (100%)

#### 3. Implement Stage 3: Convert with Marker
- [ ] Create `src/stages/convert_marker.py`
- [ ] SSH subprocess call to Ragscallion's Marker wrapper
- [ ] Store markdown locally in pdf_cache_dir
- [ ] Handle timeouts, malformed PDFs
- **Target success:** 3/3 (100%)

#### 4. Implement Stage 4: Index in RAG
- [ ] Create `src/stages/index_rag.py`
- [ ] Call Ragscallion's add-paper.sh script via SSH
- [ ] Verify indexed via /stats endpoint
- **Target success:** 3/3 (100%)

#### 5. Implement Stage 5: Extract Specs
- [ ] Create `src/stages/extract_specs.py`
- [ ] Haiku agent with RAG semantic search
- [ ] Extract: ports, bridges, bridges, internal routing
- [ ] Return structured DeviceSpec JSON
- **Target success:** 3/3 (100%)

#### 6. Implement Stage 6: Generate Patch
- [ ] Create `src/stages/generate_patch.py`
- [ ] Import patchlang_python from SignalCanvasLang
- [ ] Use PatchBuilder to construct template
- [ ] Write canonical .patch text
- **Target success:** 3/3 (100%)

#### 7. Implement Stage 7: Validate Patch
- [ ] Create `src/stages/validate_patch.py`
- [ ] Call patchlang_python.check() on generated .patch
- [ ] Parse diagnostics on failure
- [ ] Write valid patches to stdlib_output
- **Target success:** 3/3 valid (100%)

### Phase 1: Test Harness Refinement (Week 2)

**Goal:** Validate process on 50 known devices, refine based on Phase 0 learnings.

- [ ] Select 50 ground truth devices from EasySchematic + Reid's library
- [ ] Run Phase 1 with full metrics
- [ ] Analyze failure categories
- [ ] Refine extraction logic based on failure patterns

### Phase 2: Mid-Tier Scaling (Week 3)

**Goal:** Process 1,500 devices with refined extraction logic.

### Phase 3: Remaining Devices (Week 4)

**Goal:** Process remaining 2,000+ devices.

### QA Pipeline (After Phase 1)

- [ ] Implement `src/qa/sampler.py` — Random sampling
- [ ] Implement `src/qa/validator.py` — RAG-based validation
- [ ] Compare extracted specs against manual content
- [ ] Flag discrepancies for review

## File Structure (Completed)

```
SignalCanvasDeviceIngestion/
├── src/
│   ├── __init__.py                 ✅
│   ├── config.py                   ✅ (Settings with Ragscallion config)
│   ├── pipeline.py                 ✅ (LangGraph orchestrator)
│   ├── harness/
│   │   ├── __init__.py             ✅
│   │   ├── manifest.py             ✅ (SQLite persistence)
│   │   └── state.py                ✅ (Execution state)
│   ├── models/
│   │   ├── __init__.py             ✅
│   │   └── device.py               ✅ (Data models)
│   ├── stages/                     🔄 (Empty, ready for implementation)
│   │   ├── __init__.py             (TODO)
│   │   ├── find_pdf.py             (TODO)
│   │   ├── download_pdf.py         (TODO)
│   │   ├── convert_marker.py       (TODO)
│   │   ├── index_rag.py            (TODO)
│   │   ├── extract_specs.py        (TODO)
│   │   ├── generate_patch.py       (TODO)
│   │   └── validate_patch.py       (TODO)
│   ├── ragdb/                      (TODO)
│   ├── compiler/                   (TODO)
│   └── agents/                     (TODO)
├── tests/
│   ├── __init__.py                 (TODO)
│   ├── test_phase0.py              ✅
│   ├── README.md                   ✅
│   └── fixtures/
│       └── phase0_ground_truth.json ✅
├── REQUIREMENTS.md                 ✅
├── IMPLEMENTATION.md               ✅
├── README.md                       ✅
├── pyproject.toml                  ✅
├── requirements.txt                ✅
├── pytest.ini                      ✅
├── .env.example                    ✅
├── .gitignore                      ✅
└── STATUS.md                       (this file)
```

## Recent Commits

```
773239c feat: add Phase 0 test harness with ground truth fixtures and pytest infrastructure
aece7e2 feat: add core harness, models, and LangGraph pipeline orchestrator
```

## Key Design Decisions

1. **LangGraph for orchestration** — Provides checkpointing, resumability, and clear state management
2. **SQLite manifest** — Persistent state tracking across restarts
3. **Ragscallion delegation** — RAG indexing and search via existing microservice (no new dependencies)
4. **SSH for Marker** — Marker runs on Linux box, invoked via SSH from pipeline
5. **PatchBuilder over string concat** — Validates PatchLang eagerly during generation
6. **Haiku for extraction** — Cost-effective (~$0.005/device), good enough for initial specs
7. **Phase 0-3 strategy** — Validate process with 3 devices before scaling to thousands

## Known Issues / Blockers

None currently. All infrastructure is in place to begin implementation of pipeline stages.

## Testing Checklist

Before each phase:
- [ ] All unit tests pass
- [ ] Ragscallion connectivity verified
- [ ] Phase devices initialized in manifest
- [ ] LangGraph graph constructs without errors
- [ ] Output directories created

## Contact

ByteBard97 (ByteBard97) — Project lead
Reid — Domain expert (AV engineer, specs validation)
