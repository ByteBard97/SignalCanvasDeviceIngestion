# SignalCanvas Device Ingestion Pipeline

Automated pipeline to convert AV device manufacturer manuals into SignalCanvas device templates
(PatchLang `.patch` files) with complete signal routing schema.

4,000+ devices. ~$20–25 in LLM costs. Runs overnight.

## What It Does

```
Device list (manufacturer + model)
    │
    ▼
Stage 1  Find PDF      — Web search + Haiku validation to locate datasheets
Stage 2  Download      — Fetch PDFs, validate as real files, cache locally
Stage 3  Index RAG     — Submit to Ragscallion for GPU-accelerated indexing
Stage 4  Poll          — Wait for Ragscallion to finish embedding
Stage 5  Extract specs — Kimi agent queries the indexed manual, returns structured JSON
Stage 6  Generate      — Build PatchLang template from spec JSON
Stage 7  Validate      — Compile through PatchLang Rust checker; only valid files written
    │
    ▼
output/stdlib/devices/*.patch  — ready for SignalCanvas stdlib
```

Each stage is independently retryable. A SQLite manifest tracks every device through the pipeline,
so overnight runs survive crashes and resume from the last checkpoint.

See [ARCHITECTURE.md](ARCHITECTURE.md) for why the pipeline is structured this way.

## Quick Start

```bash
git clone https://github.com/SignalCanvas/SignalCanvasDeviceIngestion
cd SignalCanvasDeviceIngestion
pip install -r requirements.txt
```

Set up dependencies (see below), then run the pipeline:

```bash
# Stages 5–7: extract specs, generate + validate PatchLang templates
python scripts/run_pipeline.py

# Multiple extraction shots per device (improves accuracy, increases cost)
python scripts/run_pipeline.py --n-shot 3

# Check results
cat output/validation_report.json
```

## Dependencies

### 1. Ragscallion

[Ragscallion](https://github.com/ByteBard97/ragscallion) is a local-first RAG server that handles
PDF ingestion, GPU-accelerated embedding, and hybrid vector+BM25 search. The pipeline delegates
all document indexing and semantic search to it over HTTP.

```bash
git clone https://github.com/ByteBard97/ragscallion
cd ragscallion
uv sync
uv run python server.py 8086

# Verify
curl http://localhost:8086/health  # → "ok"
```

Ragscallion requires an NVIDIA GPU with CUDA. It can run on the same machine as the pipeline
or on a separate box — set `RAGSCALLION_HOST` in your `.env` accordingly.

### 2. PatchLang compiler

The PatchLang compiler validates generated `.patch` files. Built from the sibling
[SignalCanvasLang](https://github.com/SignalCanvas/SignalCanvasLang) repo:

```bash
cd ../SignalCanvasLang/crates/patchlang-python
pip install maturin
maturin develop
```

### 3. Environment

```bash
cp .env.example .env
# Edit .env — set CLAUDE_API_KEY, MOONSHOT_API_KEY, and RAGSCALLION_HOST
```

Required keys:
- `CLAUDE_API_KEY` — Anthropic API key (for Stage 1 PDF discovery via Claude Haiku)
- `MOONSHOT_API_KEY` — Moonshot/Kimi API key (for Stage 5 spec extraction)

If Ragscallion is running on the same machine, the defaults in `.env.example` work without changes.

## Cost

| Stage | Model | Cost for 4,000 devices |
|-------|-------|------------------------|
| 1 — Find PDF | Claude Haiku | ~$4 |
| 5 — Extract specs | Kimi (Moonshot) 128K | ~$15–20 |
| 2–4, 6–7 | Local tools / Ragscallion | Free |
| **Total** | | **~$20–25** |

## Output

- `output/stdlib/devices/*.patch` — valid device templates, ready for the SignalCanvas stdlib
- `output/ingestion.db` — SQLite manifest with per-device stage history
- `output/validation_report.json` — per-device success/failure with diagnostics

## Development

```bash
# Run tests
pytest tests/

# Check compiler works
python -c "import patchlang_python; print(patchlang_python.validate('template Foo {}'))"
```

## Status

**Phase 0 — Harness Validation** (done)
- [x] Ragscallion integration (multi-doc: spec sheet + user manual + install guide)
- [x] SQLite manifest with checkpoint/resume
- [x] Pipeline orchestrator
- [x] Ground truth fixtures + test suite

**Phase 1 — Test Harness** (next)
- [ ] Validate on 50 known devices
- [ ] Tune extraction prompts based on failure analysis

**Phase 2–3 — Scale**
- [ ] 1,500 mid-tier devices
- [ ] Remaining 2,000+ devices

## Related Projects

- [SignalCanvasLang](https://github.com/SignalCanvas/SignalCanvasLang) — the PatchLang DSL and Rust compiler this pipeline targets
- [Ragscallion](https://github.com/ByteBard97/ragscallion) — the local RAG server powering document indexing and search
- [EasySchematic](https://easyschematic.live) — browser-based AV signal flow diagram tool with its own device library (2,000+ templates). Both tools are building structured device databases for AV system design; there's natural overlap and interest in format interop.

---

**Repo:** https://github.com/SignalCanvas/SignalCanvasDeviceIngestion
