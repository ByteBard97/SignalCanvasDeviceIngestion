# SignalCanvas Device Ingestion Pipeline

Automated pipeline to convert AV device manufacturer manuals into SignalCanvas device templates (PatchLang `.patch` files) with complete signal routing schema.

## Quick Start

```bash
# Setup
git clone <repo>
cd SignalCanvasDeviceIngestion
pip install -r requirements.txt

# Build compiler (one-time)
cd ../SignalCanvasLang/crates/patchlang-python
maturin develop

# Run Phase 1 (test harness on 50 known devices)
python src/pipeline.py --phase 1 --max-devices 50

# Check report
cat output/validation_report.json
```

## What It Does

1. **Finds PDF manuals** via web search + Haiku validation (~$0.001 per device)
2. **Downloads + validates** PDFs as real files
3. **Converts with Marker** to structured markdown
4. **Indexes in RAG DB** for semantic search
5. **Extracts signal routing specs** via Haiku agent querying RAG
6. **Generates PatchLang templates** using Rust compiler Python binding
7. **Validates against compiler** — only valid `.patch` files written to output

## Output

- `output/stdlib/devices/*.patch` — Valid device templates, ready for SignalCanvas stdlib
- `output/validation_report.json` — Per-device success/failure with diagnostics

## Cost

- **Total**: ~$50 for 4,000 devices
  - Haiku agents: ~$20
  - Web search: Free (Claude Code built-in)
  - Local tools: Free

## Timeline

- **Phase 1** (Week 1): Test harness on 50 known devices → refine process
- **Phase 2** (Week 2-3): 1,500 mid-tier devices → apply learnings
- **Phase 3** (Week 4): Remaining devices → accept lower hit rate

Total runtime: 1 week (overnight/evening runs), fully debuggable.

## Architecture

See `IMPLEMENTATION.md` for:
- Module structure and APIs
- Compiler integration details
- RAG database design
- Failure handling strategies
- Test harness design

## Requirements

See `REQUIREMENTS.md` for:
- What we're building (7-stage pipeline)
- Core requirements (R1-R10)
- Success criteria
- Open questions

## Setup

### Prerequisites

**Ragscallion RAG microservice must be running.** The pipeline delegates PDF indexing and semantic search to Ragscallion instead of building its own RAG system.

```bash
# Clone and start Ragscallion (runs on 192.168.0.200:8086)
git clone https://github.com/ByteBard97/ragscallion
cd ragscallion
python server.py 8086

# Verify it's running
curl http://localhost:8086/health  # Should return "ok"
```

**PatchLang compiler** must be built from the sibling SignalCanvasLang repo:

```bash
cd ../SignalCanvasLang/crates/patchlang-python
pip install maturin
maturin develop
```

### Dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt

# Or manually:
pip install pydantic click pydantic-settings requests langgraph langchain sentence-transformers
```

### Environment

```bash
# Create .env from template
cp .env.example .env

# Edit .env and set:
CLAUDE_API_KEY=sk-ant-...
RAGSCALLION_HOST=192.168.0.200      # Where your Ragscallion server is running
RAGSCALLION_PORT=8086
RAGSCALLION_SSH_USER=your-username
RAGSCALLION_SSH_HOST=192.168.0.200
```

### Architecture

```
Device Input
    ↓
[Pipeline Harness] ← SQLite manifest (persistent state)
    ↓
Stage 1-4: Find → Download → Convert → Index
    ↓
[Ragscallion RAG] ← Indexed device manuals + vector embeddings
    ↓
Stage 5-7: Extract specs → Generate patch → Validate
    ↓
Valid .patch files → output/stdlib/devices/
Invalid → output/validation_report.json
```

**Key dependency:** Ragscallion is responsible for:
- PDF → Markdown conversion via Marker (GPU-accelerated on Linux box)
- Vector embedding and semantic search for spec extraction
- Device manual indexing and retrieval

The pipeline runs on your local machine and delegates these operations to Ragscallion via HTTP + SSH.

## Development

```bash
# Run tests
pytest tests/

# Run with debug logging
RUST_LOG=debug python src/pipeline.py --phase 1

# Check compiler validation works
python -c "import patchlang_python; print(patchlang_python.validate('template Foo {}'))"
```

## Status

**Phase 0: Harness Validation** (Current)
- [x] Ragscallion RAG microservice running
- [x] Core harness + manifest persistence
- [x] LangGraph pipeline orchestrator
- [x] Phase 0 ground truth fixtures (3 devices)
- [x] Test suite + connectivity checks
- [ ] Stage implementations (7 stages to build)

**Phase 1: Test Harness** (Next)
- [ ] Implement all 7 pipeline stages
- [ ] Validate on 50 known devices
- [ ] Refine extraction logic based on failures

**Phase 2-3: Scaling** (Follow-up)
- [ ] 1,500 mid-tier devices
- [ ] Remaining 2,000+ devices
- [ ] QA pipeline + sampling validation

---

**Repo:** https://github.com/ByteBard97/SignalCanvasDeviceIngestion  
**Owner:** ByteBard97 + Reid (reidwwall)  
**Dependency:** https://github.com/ByteBard97/ragscallion (must be running)  
**Next:** Implement Stage 1-7 pipeline modules
