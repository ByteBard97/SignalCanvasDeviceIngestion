# SignalCanvas Device Ingestion — Requirements Document

## Overview

**Goal:** Build an automated pipeline to convert AV device manufacturer manuals into rich SignalCanvas device templates (PatchLang `.patch` files) with complete signal routing schema.

**Scope:** 4,000+ devices from EasySchematic and Patchify, enriched with signal flow information extracted from manufacturer PDFs via RAG-based agent queries.

**Output:** Valid, compilable `.patch` files suitable for the SignalCanvas stdlib, validated against the PatchLang Rust compiler.

---

## What We're Building

### 1. Multi-Stage Ingestion Pipeline

**Stage 1: Find PDF**
- Input: Device (manufacturer + model)
- Process: Web search + Haiku validation to locate manufacturer manual/datasheet PDFs
- Output: Verified PDF URL
- Success metric: 80%+ of major devices, 40%+ of niche devices

**Stage 2: Download PDF**
- Input: PDF URL
- Process: Download, validate it's a real PDF, store locally
- Output: Device manual PDF file
- Success metric: 95%+ of found URLs download successfully

**Stage 3: Convert to Markdown**
- Input: PDF file
- Process: Use Marker (PDF → Markdown conversion) to extract structured content
- Output: Device manual as markdown with preserved structure (tables, lists, headings)
- Success metric: Marker handles 90%+ of manuals without crashes

**Stage 4: Index in RAG DB**
- Input: Converted markdown + device metadata
- Process: Build vector embeddings, store in queryable index
- Output: Searchable knowledge base of all device manuals
- Success metric: RAG retrieval returns relevant sections 80%+ of the time

**Stage 5: Extract Specs via Agent**
- Input: Device name + manual from RAG
- Process: Agent queries RAG for signal routing info, extracts:
  - Internal signal routing patterns
  - Expansion slot definitions (what cards fit, what they add)
  - Bus definitions (what buses exist, what channels feed them)
  - Stream/virtual channel capabilities
  - Port signal type constraints
- Output: Structured device spec JSON
- Success metric: Agent extracts correct specs 75%+ of the time

**Stage 6: Generate PatchLang Template**
- Input: Device spec JSON
- Process: Build canonical `.patch` template using PatchLang Rust builder (Python binding)
- Output: Device template in PatchLang format
- Success metric: All output templates parse without error

**Stage 7: Validate Against Compiler**
- Input: Generated `.patch` file
- Process: Run through PatchLang `check()` function (Python binding), verify no errors
- Output: Validation report (valid/invalid + diagnostics)
- Success metric: 95%+ of generated templates pass validation

---

## Core Requirements

### R1: Pipeline Harness
- **Manifest system** to track each device through all stages (NOT_STARTED → DONE/FAILED)
- **State tracking**: Current device, attempt count, failure history, categorized failures
- **Persistence**: Resume pipeline from last checkpoint on restart
- **Logging**: All operations logged for debugging and metrics

### R2: Web Search + Validation
- Use Claude Code's built-in `WebSearch` tool (free)
- Use Claude Haiku for smart search term generation + URL validation
- **Cost**: ~$4 for 4,000 devices

### R3: PDF Processing
- Download and validate PDFs are real files
- Handle redirects, authentication errors, dead links gracefully
- Log failures categorized by type (not found, download failed, invalid PDF)

### R4: Marker Integration
- Call Marker (subprocess) to convert PDFs → Markdown
- Handle Marker crashes gracefully (timeout, memory limits)
- Validate output is readable markdown
- **Cost**: Free (local tool)

### R5: RAG Database
- Index device manuals with vector embeddings
- Support semantic search ("what signals does this device route?")
- Local-first (SQLite + embeddings, no external service)
- Fast enough to query during agent extraction

### R6: Agent Extraction
- Deploy lightweight agent (Claude Haiku with function calling)
- Agent searches RAG for specific device specs
- Structured extraction (JSON schema for specs)
- Retry on failures with previous error context
- **Cost**: ~$15-20 for 4,000 devices (Haiku @ $0.80/1M tokens)

### R7: PatchLang Generation
- Use Python binding of SignalCanvasLang Rust compiler to build templates
- Validate all required fields are present
- Generate canonical format (use `format_source()`)
- Deterministic IDs from device names, not UUIDs

### R8: Compiler Validation
- Call `patchlang_python.validate()` and `check()` on every generated template
- Reject invalid templates, log diagnostics
- Output only valid `.patch` files to stdlib
- **Must pass before any file is written**

### R9: Test Harness
- Ground truth dataset of 50-100 known devices
- Automated test pipeline runs on known devices
- Metrics: PDF found %, download success %, marker success %, extraction accuracy %, validation pass %
- Failure categorization and iteration loop

### R10: Phased Execution
- **Phase 0**: Harness validation on 1-3 known devices → verify pipeline works
- **Phase 1**: Test harness on 50 known devices → refine process
- **Phase 2**: 1,500 mid-tier devices → apply Phase 1 learnings
- **Phase 3**: Remaining devices → accept lower hit rate
- **Goal**: Complete in 1 week (overnight/evening runs)

### R11: QA Pipeline (Sampling + Validation)
- After initial generation, sample N generated `.patch` files randomly
- Query RAG DB for each sample with device name + extracted specs
- Compare: Does RAG manual content match generated specs?
- Flag discrepancies for manual review
- **Goal**: Validate accuracy before merging to stdlib
- **Note**: Design/implementation deferred until initial import works

---

## Data Flow

```
EasySchematic + Patchify
  ↓
Device List
  ↓ (Stage 1-2)
PDF URLs + Downloaded Files
  ↓ (Stage 3)
Markdown Manuals
  ↓ (Stage 4)
RAG Index (Vector DB)
  ↓ (Stage 5)
Extracted Device Specs (JSON)
  ↓ (Stage 6)
Generated .patch Templates
  ↓ (Stage 7)
✓ Validated .patch Files → output/stdlib/devices/
✗ Invalid → validation_report.json (for review)
```

---

## Success Criteria

| Metric | Target | Rationale |
|--------|--------|-----------|
| **PDF found rate** | 80% major, 40% niche | EasySchematic/Patchify skew toward established brands |
| **Download success** | 95% of found URLs | Some links may be dead, auth-protected |
| **Marker success** | 90% without crashes | Some PDFs scanned, corrupted, or unusual format |
| **Extraction accuracy** | 75% correct specs | Agent may misinterpret complex manuals |
| **Validation pass** | 95%+ generated templates | PatchLang builder validates eagerly |
| **Total devices processed** | 3,000+ | 4,000 input, expect 75% to complete pipeline |
| **Pipeline duration** | 1 week | Overnight/evening runs, debuggable in phases |
| **Cost** | <$50 total | Haiku ($20) + web search (free) + no external APIs |

---

## Deliverables

1. ✅ `requirements.md` (this document)
2. ⬜ `IMPLEMENTATION.md` — Architecture, API design, module breakdown
3. ⬜ `src/` — Python + TypeScript implementation
4. ⬜ `tests/` — Harness + ground truth fixtures
5. ⬜ `output/stdlib/` — Generated `.patch` files (only valid ones)
6. ⬜ `output/validation_report.json` — Per-device success/failure summary

---

## Assumptions

- Manufacturer PDFs are publicly available for 60-80% of major brand devices
- Marker successfully converts 90%+ of PDFs to readable markdown
- Claude Haiku is capable enough to extract signal specs from manuals
- PatchLang Rust compiler is accessible via Python binding (confirmed)
- Network access available for PDF downloads and web searches
- Signal routing information is available in manufacturer documentation for most devices

---

## Constraints

- **Timeline**: Must be debuggable in 1 week (not 1 month)
- **Cost**: Total < $50
- **Output quality**: Only valid `.patch` files in stdlib (zero tolerance for invalid syntax)
- **Model constraints**: Use Haiku for cost, Sonnet only for complex extraction failures
- **Dependencies**: Must not break SignalCanvas frontend; output compatible with PatchLang v0.2.2 spec

---

## Open Questions

1. **Marker setup**: How is Marker installed/run? Subprocess, Docker, pip package?
2. **RAG DB choice**: Chroma, FAISS, or custom SQLite + embeddings?
3. **Agent deployment**: Direct API calls or Agent wrapper?
4. **Output location**: Where do generated `.patch` files go? (SignalCanvasFrontend/src/data/stdlib/devices/ ?)
5. **Failure handling**: Which devices get escalated to manual review vs. skipped?
