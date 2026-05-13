# Architecture

Why the pipeline is built the way it is.

## The problem

Building a device library for AV signal-flow design at scale means processing 4,000+ manufacturer
PDFs — datasheets, user manuals, install guides — to extract structured specs: port names, signal
types, connector formats, expansion slots, bus topologies. The data is locked in unstructured PDFs
with no consistent format across manufacturers.

We need to do this:
- Without sending thousands of proprietary PDFs to a cloud service
- Without paying cloud API costs for GPU-intensive PDF conversion
- With enough reliability to run overnight and resume if something crashes
- Cheaply enough that iterating on extraction quality isn't cost-prohibitive

## Why Ragscallion

[Ragscallion](https://github.com/ByteBard97/ragscallion) is a local-first RAG server that runs
on your own hardware. The pipeline delegates PDF ingestion, embedding, and search entirely to it.

**Why not do RAG inline in the pipeline?**

PDF → Markdown conversion via [Marker](https://github.com/VikParuchuri/marker) needs a CUDA GPU
to be fast enough at scale. A 300-page mixer manual takes ~20 seconds on GPU; it would be minutes
on CPU. Running the pipeline on a laptop and offloading GPU work to a dedicated machine over HTTP
is cleaner than requiring the pipeline runner to have a GPU.

**Why not a managed vector DB (Pinecone, Weaviate, etc.)?**

Device manuals contain manufacturer specifications that we'd rather not upload to third-party cloud
services. Ragscallion runs entirely on your own infrastructure — no API keys, no data leaves your
network.

**Why hybrid search?**

Vector-only search misses exact matches. When extracting specs for a "Yamaha CL5", you want both
semantic understanding ("what are the input channels") and exact term matching ("CL5", "dante",
"96kHz"). Ragscallion uses Reciprocal Rank Fusion to merge vector results with BM25 keyword
results, which consistently outperforms either alone for datasheet queries.

**Why just HTTP?**

The pipeline runs on any machine that can reach the Ragscallion host. No Python SDK to import,
no MCP server to configure, no vendor lock-in. The same Ragscallion instance can serve Claude
Code, Cursor, or any other agent that can make HTTP requests.

## Why a staged pipeline

The pipeline splits ingestion into 7 discrete stages rather than one big function per device.

**Stages are independently retryable.** If Stage 5 (spec extraction) fails on a batch of 50
devices due to a prompt bug, you fix the prompt and re-run Stage 5 only — you don't re-download
and re-index 50 PDFs. Each stage writes its output to the SQLite manifest before moving on, so
failures are always checkpointed at stage boundaries.

**The SQLite manifest is the source of truth.** Every device has a row in `device_nodes` tracking
which stage it's in, how many retries it has left, and what queue it belongs to. The pipeline can
be killed at any point and will resume exactly where it left off. This matters for overnight runs
across 4,000 devices.

**Stages 3–4 are async by design.** Ragscallion ingestion is GPU-bound and takes ~30 seconds per
device. The pipeline submits a batch of PDFs (Stage 3), then polls for completion (Stage 4) while
simultaneously running Stage 1–2 on the next batch. This keeps the GPU busy without blocking the
pipeline on each device.

**Cost isolation.** Stages 1–4 (find, download, convert, index) are cheap: Haiku at ~$0.001/device
plus local GPU work. Stage 5 (extraction) is where the cost lives: Kimi 128K at ~$0.004/device.
Running them separately means you can build and QA the full corpus first, then tune extraction
without re-incurring indexing costs.

## Model routing

**Stage 1 — Claude Haiku:** URL discovery and validation. We search for PDFs using web search, then
ask Haiku to evaluate whether a URL looks like a real manufacturer datasheet vs. a distributor
page or brochure. Haiku is fast and cheap (~$0.001/device); accuracy only needs to be "good enough"
because Stage 2 validates the actual file.

**Stage 5 — Kimi (Moonshot) 128K:** Spec extraction. A full device manual can be 200+ pages.
After Ragscallion indexes it, Stage 5 issues a series of targeted queries (one per spec target:
analog inputs, digital outputs, expansion slots, etc.) and asks Kimi to synthesize the results
into structured JSON. Kimi's 128K context window handles the retrieved chunks without truncation.
It's also cost-effective: the Moonshot API prices at ~$0.002/1K input tokens at the 8K tier,
vs. Claude Sonnet at ~$0.003/1K.

**Stage 6–7 — PatchLang Rust compiler:** Zero cost, deterministic. The compiler validates that the
generated `.patch` template is syntactically and semantically correct. Only files that pass the
compiler are written to `output/stdlib/devices/`.

## Multi-doc per device

The original pipeline ingested one PDF per device (the spec sheet). Spec sheets are dense with
port counts and connector types, but thin on wiring diagrams, bus topologies, and routing
capabilities — that information lives in user manuals and install guides.

The pipeline now collects up to three documents per device:
- **Spec sheet** — port counts, connector types, physical specs
- **User manual** — signal flow, bus architecture, routing options
- **Install guide** — rack diagrams, wiring examples, network setup

All three are indexed in Ragscallion under the same corpus ID. Stage 5 queries across the full
corpus so extraction can pull from whichever document has the relevant content. This improved
extraction accuracy on complex devices (large-format consoles, DSP processors, modular systems)
where the spec sheet alone didn't contain routing depth.

## Device format landscape

This pipeline targets [PatchLang](https://github.com/SignalCanvas/SignalCanvasLang), a DSL for
describing AV device signal routing. A `.patch` file declares a device's ports, signal types,
routing constraints, and expansion slots in a compiler-verified format.

[EasySchematic](https://easyschematic.live) is a browser-based AV signal flow diagram tool with
its own device template format and a library of 2,000+ devices. Both tools are building structured
device databases for the AV industry, with different representations of the same underlying
information. There is interest in exploring format interoperability — a converter between
EasySchematic templates and PatchLang `.patch` files would let the two libraries cross-pollinate.
