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

### Dependencies

```bash
# Python
pip install pydantic click pydantic-settings

# RAG embeddings
pip install sentence-transformers

# PDF conversion (requires system deps)
pip install marker-ai

# PatchLang compiler (build from SignalCanvasLang)
cd ../SignalCanvasLang/crates/patchlang-python
pip install maturin
maturin develop
```

### Environment

```bash
# .env
CLAUDE_API_KEY=<your key>
MANIFESTS_DB=output/ingestion.db
RAG_DB=output/rag.db
STDLIB_OUTPUT=output/stdlib/devices
```

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

- [ ] Harness (manifest, state, fixtures)
- [ ] Stage implementations
- [ ] RAG database
- [ ] Compiler bridge
- [ ] Agent extraction
- [ ] Phase 1 test run
- [ ] Phase 2 + refinement
- [ ] Phase 3 + final report

---

**Owner:** ByteBard97 + Reid
**Status:** Design phase (REQUIREMENTS.md + IMPLEMENTATION.md complete)
**Next:** Implement harness + stages
