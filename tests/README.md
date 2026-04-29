# Tests — Device Ingestion Pipeline Test Suite

## Structure

```
tests/
├── test_phase0.py           # Phase 0 ground truth validation tests
├── fixtures/
│   └── phase0_ground_truth.json  # 3 known good devices for validation
└── README.md
```

## Phase 0 Test Devices

Three well-documented devices used to validate the entire pipeline end-to-end:

1. **YAMAHA R08D** — 8-channel Dante to XLR converter
   - Simple signal flow (1:1 bridging)
   - Tests basic port extraction and bridge rules

2. **Audinate AVIO-AI2** — 2-channel analog input Dante converter
   - Inverse of R08D (analog in, Dante out)
   - Tests both input and output directions

3. **YAMAHA CL5** — Digital mixing console with Dante
   - More complex device (64 channels)
   - Tests multi-port extraction

## Running Tests

```bash
# Run all Phase 0 tests
pytest tests/test_phase0.py -v

# Run specific test
pytest tests/test_phase0.py::test_manifest_persistence -v

# With coverage
pytest tests/test_phase0.py --cov=src --cov-report=html
```

## Prerequisites

Before running tests:

```bash
# Install dependencies
pip install -e ".[dev]"

# Ensure Ragscallion server is running on 192.168.0.200:8086
# Check health: curl http://192.168.0.200:8086/health
```

## Test Categories

### Unit Tests
- `test_manifest_persistence` — SQLite storage and retrieval
- `test_manifest_stats` — Aggregated statistics
- `test_device_node_stage_tracking` — Device state progression
- `test_pipeline_initialization` — LangGraph setup

### Integration Tests
- `test_ragscallion_connectivity` — RAG server health
- `test_phase0_fixture_structure` — Ground truth device validation

## Success Criteria

All Phase 0 tests must pass before proceeding to Phase 1. Key metrics:

- ✓ All 3 devices in manifest after initialization
- ✓ Ragscallion server reachable and healthy
- ✓ Pipeline graph constructs without errors
- ✓ Device state tracking through 7 stages works correctly
