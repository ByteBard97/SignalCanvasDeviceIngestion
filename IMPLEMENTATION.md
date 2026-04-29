# SignalCanvas Device Ingestion — Implementation Plan

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ Device List (EasySchematic + Patchify consolidated JSON)    │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┴────────────────┐
        │                                 │
   ┌────▼────────┐              ┌────────▼──────┐
   │ Stage 1: F  │              │ Stage 1: F    │
   │ Find PDF    │◄─────────────┤ (Haiku        │
   │ (WebSearch) │              │  validation)  │
   └────┬────────┘              └───────────────┘
        │
   ┌────▼────────────────────────────────────────┐
   │ Stage 2: Download PDF                       │
   │ (HTTP + validate file)                      │
   └────┬───────────────────────────────────────┘
        │
   ┌────▼────────────────────────────────────────┐
   │ Stage 3: Convert with Marker                │
   │ (PDF → Markdown subprocess)                 │
   └────┬───────────────────────────────────────┘
        │
   ┌────▼────────────────────────────────────────┐
   │ Stage 4: Index in RAG DB                    │
   │ (Vector embeddings + SQLite)                │
   └────┬───────────────────────────────────────┘
        │
   ┌────▼───────────────────────────────────────────────┐
   │ Stage 5: Extract Specs via Agent                   │
   │ (Haiku + RAG search + function calling)            │
   └────┬──────────────────────────────────────────────┘
        │
   ┌────▼───────────────────────────────────────────────┐
   │ Stage 6: Generate PatchLang Template               │
   │ (Python binding → ProgramBuilder.format())         │
   └────┬──────────────────────────────────────────────┘
        │
   ┌────▼───────────────────────────────────────────────┐
   │ Stage 7: Validate with Compiler                    │
   │ (patchlang_python.check() + parse diagnostics)     │
   └────┬──────────────────────────────────────────────┘
        │
    ┌───┴────────────────────────┬───────────────────┐
    │                            │                   │
✓ Valid                  ✗ Invalid            (Manifest tracks both)
    │                            │
    ▼                            ▼
output/stdlib/         validation_report.json
devices/*.patch        + detailed error logs
```

---

## Module Structure

```
SignalCanvasDeviceIngestion/
├── pyproject.toml              # Python dependencies (marker, pyo3, etc)
├── package.json                # Node/TS dependencies
├── .envrc                       # direnv for local dev
│
├── src/
│   │
│   ├── harness/                # Manifest + state management
│   │   ├── manifest.py         # IngestionNode, IngestionManifest, SQLite persistence
│   │   ├── state.py            # IngestionState (current device, attempt tracking)
│   │   ├── failure_analysis.py # Categorize failures, retry strategies
│   │   └── __init__.py
│   │
│   ├── stages/                 # Pipeline stages (one per file)
│   │   ├── find_pdf.py         # WebSearch + Haiku validation
│   │   ├── download_pdf.py     # HTTP + file validation
│   │   ├── convert_marker.py   # Marker subprocess integration
│   │   ├── index_rag.py        # Build vector index
│   │   ├── extract_specs.py    # Agent extraction with RAG search
│   │   ├── generate_patch.py   # ProgramBuilder + formatting
│   │   ├── validate_patch.py   # Compiler validation
│   │   └── __init__.py
│   │
│   ├── ragdb/                  # Vector DB + search
│   │   ├── builder.py          # Initialize embeddings, build index
│   │   ├── search.py           # Semantic search + retrieval
│   │   └── __init__.py
│   │
│   ├── compiler/               # PatchLang integration
│   │   ├── bridge.py           # Python binding wrapper
│   │   └── __init__.py
│   │
│   ├── models/                 # Data structures
│   │   ├── device.py           # DeviceInput, DeviceSpec
│   │   ├── ingestion.py        # IngestionNode, IngestionStatus
│   │   └── __init__.py
│   │
│   ├── agents/                 # Agentic patterns
│   │   ├── extractor.py        # Spec extraction agent
│   │   ├── validator.py        # Haiku URL validation
│   │   └── __init__.py
│   │
│   ├── config.py               # Configuration (model names, paths, etc)
│   ├── logging.py              # Structured logging
│   └── pipeline.py             # Main orchestration
│
├── tests/
│   ├── conftest.py             # Pytest fixtures
│   ├── harness_test.py         # Manifest + state tests
│   ├── stage_integration_test.py # End-to-end stage tests
│   ├── compiler_bridge_test.py # Compiler validation tests
│   ├── failure_handling_test.py # Categorization + retry
│   │
│   └── fixtures/
│       ├── ground_truth_devices.json     # 50 known devices with verified URLs
│       ├── sample_manuals/               # Small PDF/markdown samples
│       └── expected_specs/               # Expected extracted specs
│
├── docs/
│   ├── MARKERS_SETUP.md         # How to install Marker locally
│   ├── RAG_DB_DESIGN.md         # RAG architecture details
│   └── AGENT_PROMPTS.md         # Extraction prompt templates
│
├── .gitignore
└── README.md                   # Quick start + architecture summary
```

---

## Key Algorithms & APIs

### 1. Manifest System (src/harness/manifest.py)

```python
class IngestionStatus(Enum):
    NOT_STARTED = "not_started"
    FINDING_PDF = "finding_pdf"
    DOWNLOADING = "downloading"
    CONVERTING = "converting"
    INDEXING = "indexing"
    EXTRACTING = "extracting"
    GENERATING = "generating"
    VALIDATING = "validating"
    DONE = "done"
    FAILED = "failed"

class FailureCategory(Enum):
    PDF_NOT_FOUND = "pdf_not_found"
    DOWNLOAD_FAILED = "download_failed"
    MARKER_FAILED = "marker_failed"
    EXTRACTION_FAILED = "extraction_failed"
    PATCH_INVALID = "patch_invalid"
    UNKNOWN = "unknown"

@dataclass
class IngestionNode:
    device_id: str                    # "{manufacturer}_{model}"
    device_input: DeviceInput         # From EasySchematic/Patchify
    
    # Stage tracking
    status: IngestionStatus
    current_stage: int                # 0-7
    attempt_count: int
    
    # Outputs
    pdf_url: Optional[str]
    pdf_path: Optional[str]
    markdown_path: Optional[str]
    device_spec: Optional[DeviceSpec]
    patch_source: Optional[str]       # Generated .patch file content
    validation_result: Optional[dict] # { valid: bool, errors: [...] }
    
    # Error tracking
    last_error: Optional[str]
    failure_category: Optional[FailureCategory]
    error_history: List[str]          # All errors encountered
    retry_count: int
    
    # Timestamps
    created_at: datetime
    updated_at: datetime

class IngestionManifest:
    """SQLite-backed persistent manifest."""
    
    def __init__(self, db_path: str):
        self.db = sqlite3.connect(db_path)
        self._init_schema()
    
    def create_node(self, device_input: DeviceInput) -> IngestionNode:
        """Create and persist a new node."""
        ...
    
    def load_node(self, device_id: str) -> Optional[IngestionNode]:
        """Load from DB."""
        ...
    
    def update_node(self, node: IngestionNode) -> None:
        """Persist changes."""
        ...
    
    def next_nodes_for_stage(self, stage: int, limit: int) -> List[IngestionNode]:
        """Get next N nodes ready for a given stage."""
        ...
    
    def nodes_by_status(self, status: IngestionStatus) -> List[IngestionNode]:
        """Query nodes by status."""
        ...
    
    def nodes_by_failure_category(self, category: FailureCategory) -> List[IngestionNode]:
        """Find devices that failed in a specific way."""
        ...
```

### 2. Pipeline Orchestration (src/pipeline.py)

```python
class DeviceIngestionPipeline:
    """Main orchestrator."""
    
    def __init__(self, config: Config):
        self.manifest = IngestionManifest(config.manifest_db)
        self.stages = {
            1: FindPDFStage(self.manifest),
            2: DownloadPDFStage(self.manifest),
            3: ConvertMarkerStage(self.manifest),
            4: IndexRAGStage(self.manifest),
            5: ExtractSpecsStage(self.manifest),
            6: GeneratePatchStage(self.manifest),
            7: ValidatePatchStage(self.manifest),
        }
    
    def run_phase(self, phase: int, max_devices: int = 500):
        """
        Run one phase:
        - Phase 1: Known devices (50)
        - Phase 2: Mid-tier (1,500)
        - Phase 3: Remaining (remaining)
        """
        devices = self.manifest.load_phase(phase)
        
        for stage_num in range(1, 8):
            stage = self.stages[stage_num]
            ready = self.manifest.next_nodes_for_stage(stage_num, max_devices)
            
            logger.info(f"Phase {phase}: Stage {stage_num}: {len(ready)} devices")
            
            for node in ready:
                try:
                    result = stage.process(node)
                    node.status = IngestionStatus.DONE if stage_num == 7 else ...
                    self.manifest.update_node(node)
                except StageFailure as e:
                    node.last_error = str(e)
                    node.failure_category = e.category
                    node.retry_count += 1
                    self.manifest.update_node(node)
                    logger.error(f"Device {node.device_id} failed at stage {stage_num}: {e}")
    
    def generate_report(self) -> dict:
        """Summary statistics."""
        return {
            "total_devices": len(self.manifest.all_nodes()),
            "completed": len(self.manifest.nodes_by_status(IngestionStatus.DONE)),
            "failed": len(self.manifest.nodes_by_status(IngestionStatus.FAILED)),
            "by_failure_category": {
                cat: len(self.manifest.nodes_by_failure_category(cat))
                for cat in FailureCategory
            },
        }
```

### 3. Stage Interface (base class)

```python
class IngestionStage(ABC):
    """Base class for all stages."""
    
    def __init__(self, manifest: IngestionManifest):
        self.manifest = manifest
    
    @abstractmethod
    def process(self, node: IngestionNode) -> StageResult:
        """Execute stage. Raise StageFailure on error."""
        ...

class StageFailure(Exception):
    """Raised when a stage cannot complete."""
    def __init__(self, message: str, category: FailureCategory):
        self.message = message
        self.category = category
```

### 4. Compiler Bridge (src/compiler/bridge.py)

```python
import patchlang_python

def validate_patch(patch_source: str) -> dict:
    """Validate generated .patch file against compiler."""
    try:
        # Try to parse
        is_valid = patchlang_python.validate(patch_source)
        
        if not is_valid:
            # Get detailed diagnostics
            check_result = json.loads(patchlang_python.check(patch_source))
            return {
                "valid": False,
                "errors": check_result.get("errors", []),
                "diagnostics": check_result.get("diagnostics", []),
            }
        
        return {"valid": True, "errors": []}
    
    except Exception as e:
        return {
            "valid": False,
            "errors": [str(e)],
            "diagnostics": [],
        }

def build_device_template(spec: DeviceSpec) -> str:
    """Build .patch template from spec using Python binding."""
    builder = patchlang_python.ProgramBuilder()
    
    # Build template with all metadata
    template_json = json.dumps({
        "name": spec.template_name,
        "meta": {
            "manufacturer": spec.manufacturer,
            "model": spec.model,
            "category": spec.category,
            # Signal flow metadata
            "signal_routing_sources": spec.sources,  # Where signals come from
            "signal_routing_targets": spec.targets,  # Where signals go
        },
        "ports": [port.to_json() for port in spec.ports],
        # If expansion cards detected
        "slots": [slot.to_json() for slot in spec.slots] if spec.slots else None,
    })
    
    builder.add_template(template_json)
    
    # Format and validate
    patch_source = builder.format()
    validation = validate_patch(patch_source)
    
    if not validation["valid"]:
        raise ValueError(f"Generated patch invalid: {validation['errors']}")
    
    return patch_source
```

### 5. Spec Extraction Agent (src/agents/extractor.py)

```python
class SpecExtractorAgent:
    """Haiku-based agent to extract signal routing specs from manuals."""
    
    def __init__(self, rag_db: RAGDatabase):
        self.rag = rag_db
    
    def extract_specs(self, device: DeviceInput) -> DeviceSpec:
        """
        Search RAG for device manual, extract signal specs.
        Returns DeviceSpec with routing information.
        """
        
        # 1. Search RAG for this device's manual
        manual_sections = self.rag.search(
            query=f"{device.manufacturer} {device.model} signal routing",
            top_k=5
        )
        
        if not manual_sections:
            raise ValueError(f"No manual found for {device.device_id}")
        
        # 2. Build extraction prompt
        prompt = self._build_extraction_prompt(device, manual_sections)
        
        # 3. Call Haiku with structured extraction
        response = self._call_agent(prompt)
        
        # 4. Parse response into DeviceSpec
        spec = self._parse_response(response, device)
        
        return spec
    
    def _build_extraction_prompt(self, device: DeviceInput, sections: List[str]) -> str:
        return f"""
        Extract signal routing specs from the manual for:
        Manufacturer: {device.manufacturer}
        Model: {device.model}
        
        Manual excerpts:
        {chr(10).join(sections)}
        
        Extract and return JSON:
        {{
            "internal_routing": [
                {{"source_port": "...", "target_port": "...", "channels": [1..N]}}
            ],
            "buses": [
                {{"name": "...", "input_channels": N, "output_channels": N}}
            ],
            "expansion_slots": [
                {{"name": "...", "format": "...", "max_cards": N}}
            ],
            "port_constraints": [
                {{"port_name": "...", "allowed_signals": ["Dante", "AES3"]}}
            ],
            "streams": [
                {{"name": "...", "channels": N, "protocol": "Dante"}}
            ]
        }}
        
        Return ONLY valid JSON, no markdown or explanation.
        """
```

---

## Compiler Integration Details

### Python Binding Installation

The SignalCanvasLang repo includes a Python wheel (via PyO3). Integration steps:

1. **Build the wheel** (one-time, in SignalCanvasLang):
   ```bash
   cd SignalCanvasLang/crates/patchlang-python
   pip install maturin
   maturin develop  # Builds wheel and installs locally
   ```

2. **Import in our pipeline**:
   ```python
   import patchlang_python
   
   # Validate source
   is_valid = patchlang_python.validate(patch_source)
   
   # Get diagnostics
   check_json = patchlang_python.check(patch_source)
   
   # Format to canonical style
   formatted = patchlang_python.format_source(patch_source)
   ```

3. **Error handling**:
   - `validate()` returns bool (fast check)
   - `check()` returns JSON with diagnostics (detailed errors)
   - Any parsing/compilation error is caught before output

---

## RAG Database Design

### Option 1: Local SQLite + Sentence Transformers

```python
class RAGDatabase:
    def __init__(self, db_path: str):
        self.db = sqlite3.connect(db_path)
        self.embedder = SentenceTransformer('all-MiniLM-L6-v2')  # Small, fast
        self._init_schema()
    
    def index_manual(self, device_id: str, markdown: str) -> None:
        """
        Split markdown into sections, embed each, store in DB.
        """
        sections = self._split_sections(markdown)
        
        for section in sections:
            embedding = self.embedder.encode(section["text"])
            self.db.execute(
                """INSERT INTO embeddings 
                   (device_id, section_title, section_text, embedding)
                   VALUES (?, ?, ?, ?)""",
                (device_id, section["title"], section["text"], 
                 json.dumps(embedding.tolist()))
            )
    
    def search(self, query: str, top_k: int = 5) -> List[str]:
        """Semantic search across all manuals."""
        query_embedding = self.embedder.encode(query)
        
        # Cosine similarity search in SQLite
        results = self.db.execute(
            """SELECT section_text FROM embeddings
               ORDER BY embedding <-> ?
               LIMIT ?""",
            (json.dumps(query_embedding.tolist()), top_k)
        ).fetchall()
        
        return [r[0] for r in results]
```

### Why SQLite over Chroma/FAISS?

- **Local-first**: No external services, no API keys
- **Simplicity**: Single database file, easy backup
- **Cost**: Free
- **Performance**: Fast enough for 4,000 manuals
- **Portability**: Runs anywhere

---

## Testing Strategy

### Test Harness (tests/conftest.py)

```python
@pytest.fixture
def ground_truth_devices():
    """50 known devices with verified PDF URLs."""
    return json.load(open("tests/fixtures/ground_truth_devices.json"))

@pytest.fixture
def manifest_db(tmp_path):
    """Fresh DB for each test."""
    return IngestionManifest(str(tmp_path / "test.db"))

@pytest.fixture
def sample_markdown():
    """Sample converted manual."""
    return open("tests/fixtures/sample_manuals/yamaha_cl5.md").read()
```

### Test Phases

**Phase 1: Unit tests** (src/stage)
- Each stage processes sample input, verifies output structure

**Phase 2: Integration tests** (ground truth devices)
- Run harness on 50 known devices
- Measure success rate per stage
- Categorize failures
- Refine for Phase 2

**Phase 3: Validation tests** (compiler)
- Generate 100 patches
- All must pass `patchlang_python.validate()`
- No invalid files in output

---

## Configuration (src/config.py)

```python
@dataclass
class Config:
    # Pipeline
    manifest_db: str = "output/ingestion.db"
    
    # Stages
    pdf_download_timeout: int = 30  # seconds
    marker_timeout: int = 60
    
    # RAG
    rag_db: str = "output/rag.db"
    embedder_model: str = "all-MiniLM-L6-v2"
    
    # Agents
    haiku_model: str = "claude-3-5-haiku-20241022"
    sonnet_model: str = "claude-3-5-sonnet-20241022"
    
    # Output
    output_dir: str = "output"
    stdlib_output: str = "output/stdlib/devices"
    
    # Phases
    phase_1_devices: int = 50
    phase_2_devices: int = 1500
    phase_3_limit: int = None  # All remaining
```

---

## Failure Handling Strategy

### Categorization

| Category | Root Cause | Strategy |
|----------|-----------|----------|
| `pdf_not_found` | Web search failed | Try different search terms (Phase 2) |
| `download_failed` | URL dead, auth blocked | Manual review queue |
| `marker_failed` | PDF corrupted, scanned image | Try OCR (future), skip for now |
| `extraction_failed` | Manual too complex, agent confused | Escalate to Sonnet, then manual |
| `patch_invalid` | Generated template syntax error | Debug template generation logic |

### Retry Phases

- **Phase 1**: No retries, log all failures
- **Phase 2**: Retry Phase 1 failures with adjusted strategies
- **Phase 3**: Accept failures, generate report for manual triage

---

## Output

### 1. Valid `.patch` Files

Location: `output/stdlib/devices/*.patch`

Only files that pass `patchlang_python.validate()` are written here.

### 2. Validation Report

Location: `output/validation_report.json`

```json
{
  "summary": {
    "total": 4000,
    "completed": 3247,
    "failed": 753,
    "by_failure_category": {
      "pdf_not_found": 400,
      "download_failed": 150,
      "marker_failed": 100,
      "extraction_failed": 75,
      "patch_invalid": 28
    }
  },
  "devices": [
    {
      "device_id": "Yamaha_CL5",
      "status": "done",
      "stages": {
        "find_pdf": { "success": true, "url": "..." },
        "download": { "success": true, "size_bytes": 5242880 },
        "convert": { "success": true, "sections": 147 },
        "extract": { "success": true, "specs_extracted": [...] },
        "generate": { "success": true, "patch_lines": 234 },
        "validate": { "success": true, "errors": [] }
      }
    },
    ...
  ]
}
```

---

## Timeline & Milestones

| Week | Milestone | Tasks |
|------|-----------|-------|
| Week 1 | **Harness + Phase 1** | Manifest system, ground truth fixtures, run on 50 devices |
| Week 2 | **Stages + Integration** | Implement all 7 stages, fix Phase 1 learnings |
| Week 3 | **Phase 2 + Refinement** | Run on 1,500 devices, iterate on failures |
| Week 4 | **Phase 3 + Report** | Run on remaining, generate final report, deliver `.patch` files |

---

## Dependencies

### Python
- `patchlang_python` (from SignalCanvasLang build)
- `sentence-transformers` (RAG embeddings)
- `sqlite3` (manifest DB)
- `pydantic` (data models)
- `click` (CLI)

### External
- **Marker**: PDF → Markdown (pip install marker-ai)
- **Claude API**: Haiku/Sonnet for agents
- **Web search**: Claude Code's built-in WebSearch tool

### No external services required
- No Chroma, Pinecone, or cloud APIs
- No database infrastructure
- Everything runs locally

