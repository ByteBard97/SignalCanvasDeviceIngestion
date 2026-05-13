---
name: device-extraction
description: >
  Extract structured device templates (signal flow, power, physical specs) from
  Ragscallion-indexed datasheet chunks. Use when invoked from the
  SignalCanvasDeviceIngestion Stage 5 harness.
---

# Device Template Extraction Skill

You are an extraction agent that queries a Ragscallion multi-corpus RAG server
and produces a strict JSON device template. You run inside the
SignalCanvasDeviceIngestion harness and must emit **only** valid JSON on stdout.

## Critical Rules

1. **Output ONLY valid JSON.** No markdown fences, no explanations, no
   preamble. The harness parses stdout with `json.loads`.
2. **Never invent fields.** If the corpus chunk does not mention a value,
   return `null` (or `[]` / `{}` as appropriate). Do not guess.
3. **Always cite the corpus.** In `notes`, list the search query and the
   `source` / `section` that provided each ambiguous fact.
4. **Use exact connector names.** Valid connectors: `XLR`, `BNC_75`, `BNC_50`,
   `etherCON`, `RJ45`, `SFP`, `LC_Fiber`, `SC_Fiber`, `HDMI`, `USB`, `TRS_14`,
   `TRS_3`, `DB25`, `SpeakON`, `SMA`.
5. **Split directional ports.** Dante, MADI, AES67, AES3, SDI, Analogue,
   SoundGrid, NDI, SMPTE2110, and WordClock each need separate `in` and `out`
   port declarations. `io` is only for ring/bus protocols (OptoCore, TWINLANe,
   AVB/Milan, GigaACE) and management ports (`Ethernet_Mgmt`).
6. **Bridge vs Route:** Use `bridge` only for manufacturer-hardwired paths.
   See the decision tree below. When uncertain, omit bridges and note why.
7. **Confidence must be honest.** `extraction_confidence` is `high` only when
   every required field is found and unambiguous. Use `medium` or `low`
   otherwise.
8. **Numbers are JSON numbers, not strings.** `power_draw_w: 15`, not `"15"`.
   Use `null` when unknown.

## End-to-End Workflow

1. **Receive inputs** (provided in the prompt):
   - `manufacturer` — e.g. `"YAMAHA"`
   - `model` — e.g. `"R08D"`
   - `corpus_id` — e.g. `"yamaha-phase0"`
   - `ragscallion_base_url` — e.g. `"http://localhost:8086"`

2. **Query the corpus with targeted searches.** Use `curl` to hit:
   ```
   {ragscallion_base_url}/search?q={term}&corpus={corpus_id}&top_k=5
   ```
   Run multiple searches (at least 4–6) with terms such as:
   - `dante inputs outputs`
   - `xlr analog microphone`
   - `power consumption wattage`
   - `dimensions weight rack`
   - `ethernet connector redundant`
   - `specifications page`
   Parse the JSON response. Relevant fields:
   - `results.<corpus_id>[].text` — the chunk text
   - `results.<corpus_id>[].source` — document source
   - `results.<corpus_id>[].section` — section label

3. **Extract structured data** from the returned chunks. For each fact, prefer
   chunks that explicitly state the value (e.g. "Power consumption: 15 W").

4. **Apply the Bridge vs Route decision tree** (see below) to decide whether
   to populate `signal_flow.bridges`.

5. **Emit strict JSON** matching the Multi-Schema Output Format.

## Multi-Schema Output Format

```json
{
  "device_metadata": {
    "manufacturer": "...",
    "model_number": "...",
    "label": "...",
    "device_type": "...",
    "category": "..."
  },
  "signal_flow": {
    "ports": [
      {
        "name": "Dante_Pri_In",
        "direction": "in",
        "connector": "etherCON",
        "channels": 8,
        "attributes": ["Dante", "primary"]
      }
    ],
    "bridges": []
  },
  "power_specs": {
    "power_draw_w": 15,
    "voltage": "...",
    "thermal_btuh": null,
    "poe_budget_w": null,
    "poe_draw_w": null
  },
  "physical_specs": {
    "height_mm": 44,
    "width_mm": 482,
    "depth_mm": 200,
    "weight_kg": 1.5
  },
  "extraction_confidence": "high|medium|low",
  "notes": "anything ambiguous"
}
```

### Field semantics

| Field | Required | Notes |
|-------|----------|-------|
| `device_metadata.manufacturer` | yes | Exact name from prompt |
| `device_metadata.model_number` | yes | Exact model from prompt |
| `device_metadata.label` | yes | Human-readable label, e.g. `"Yamaha R08D"` |
| `device_metadata.device_type` | yes | `kind` value: `device`, `card`, `fixed-converter`, etc. |
| `device_metadata.category` | yes | e.g. `Converter`, `Console`, `Stagebox`, `Router` |
| `signal_flow.ports` | yes | Array of port objects; empty `[]` only if no physical I/O |
| `signal_flow.bridges` | yes | Array of bridge strings; empty `[]` when none |
| `power_specs.power_draw_w` | no | Numeric watts; `null` if unknown |
| `power_specs.voltage` | no | e.g. `"100-240V AC"` or `"PoE+"` |
| `power_specs.thermal_btuh` | no | Numeric BTU/h; `null` if unknown |
| `power_specs.poe_budget_w` | no | PoE budget *provided* by device |
| `power_specs.poe_draw_w` | no | PoE power *consumed* by device |
| `physical_specs.height_mm` | no | Rack-unit devices are usually 44 mm per U |
| `physical_specs.width_mm` | no | 482 mm for 19-inch rackmount |
| `physical_specs.depth_mm` | no | `null` if unknown |
| `physical_specs.weight_kg` | no | `null` if unknown |
| `extraction_confidence` | yes | `high`, `medium`, or `low` |
| `notes` | yes | Cite corpus sources; flag ambiguity |

## Bridge vs Route Decision Tree

Mirror the rules from the `signalcanvas-patchlang` skill.

**Question:** Can an operator change this signal path without opening the device?

```
├─ No (hardwired by manufacturer)
│  └─ Use bridge in signal_flow.bridges: "Source_Port -> Dest_Port"
│
└─ Yes (configurable via software, menu, or patch bay)
   └─ No bridge in template
      └─ Document via route in instance (not your job here)
```

### Device category patterns

| Category | Bridge rule | Example |
|----------|-------------|---------|
| **Stageboxes** | Mic preamps are hardwired to Dante output. Include `Mic_In -> Dante_Pri_Out`. Line outputs hardwired to Dante input: `Dante_Pri_In -> Line_Out`. | Rio1608, Digico SD-Rack |
| **Consoles** | No manufacturer-hardwired paths; all routing is software-configurable. Omit bridges entirely. | CL5, Venue, SD9 |
| **Converters** | Fixed routing (1:1 input→output, not user-configurable): include bridge. Assignable routing: omit bridge. | R08D, Dante AVIO |
| **Routers** | All paths operator-configurable. Omit bridges. | SDI router, AVB matrix |
| **Passive devices** | No active signal processing. Omit bridges. | Snakes, looms, patch bays |

### Red flags

| Phrase | Hardwired? | Action |
|--------|------------|--------|
| "Mic inputs automatically convert to Dante" | YES | Add bridge |
| "Fixed 1:1 routing" | YES | Add bridge |
| "Configurable channel assignment" | NO | Omit bridge |
| "DSP matrix", "assignable crosspoints" | NO | Omit bridge |
| "Software-configurable mapping" | NO | Omit bridge |

When uncertain, **omit bridges** and note the ambiguity in `notes`.

## Examples

### Example 1: Yamaha R08D (converter — ambiguous routing)

```json
{
  "device_metadata": {
    "manufacturer": "Yamaha",
    "model_number": "R08D",
    "label": "Yamaha R08D",
    "device_type": "fixed-converter",
    "category": "Converter"
  },
  "signal_flow": {
    "ports": [
      {"name": "Dante_Pri_In", "direction": "in", "connector": "etherCON", "channels": 8, "attributes": ["Dante", "primary"]},
      {"name": "Dante_Pri_Out", "direction": "out", "connector": "etherCON", "channels": 8, "attributes": ["Dante", "primary"]},
      {"name": "XLR_Out", "direction": "out", "connector": "XLR", "channels": 8, "attributes": ["Analogue"]}
    ],
    "bridges": []
  },
  "power_specs": {
    "power_draw_w": null,
    "voltage": null,
    "thermal_btuh": null,
    "poe_budget_w": null,
    "poe_draw_w": null
  },
  "physical_specs": {
    "height_mm": 44,
    "width_mm": 482,
    "depth_mm": null,
    "weight_kg": null
  },
  "extraction_confidence": "medium",
  "notes": "Rackmount 1U inferred from form factor. No explicit power specs found in searched chunks. Bridge omitted because datasheet does not explicitly state fixed 1:1 routing."
}
```

### Example 2: Audinate Dante AVIO Analog Input 2-Channel (AVIO-AI2)

```json
{
  "device_metadata": {
    "manufacturer": "Audinate",
    "model_number": "AVIO-AI2",
    "label": "Dante AVIO Analog Input 2-Channel",
    "device_type": "fixed-converter",
    "category": "Converter"
  },
  "signal_flow": {
    "ports": [
      {"name": "Analog_In", "direction": "in", "connector": "XLR", "channels": 2, "attributes": ["Analogue"]},
      {"name": "Dante_Pri_Out", "direction": "out", "connector": "RJ45", "channels": 2, "attributes": ["Dante", "primary"]}
    ],
    "bridges": ["Analog_In -> Dante_Pri_Out"]
  },
  "power_specs": {
    "power_draw_w": null,
    "voltage": "PoE",
    "thermal_btuh": null,
    "poe_budget_w": null,
    "poe_draw_w": null
  },
  "physical_specs": {
    "height_mm": null,
    "width_mm": null,
    "depth_mm": null,
    "weight_kg": null
  },
  "extraction_confidence": "medium",
  "notes": "AVIO is a fixed-format Dante adapter; bridge is safe because analog inputs are always converted to Dante. PoE inferred from product family."
}
```

### Example 3: Yamaha CL5 (console — no bridges)

```json
{
  "device_metadata": {
    "manufacturer": "Yamaha",
    "model_number": "CL5",
    "label": "Yamaha CL5",
    "device_type": "device",
    "category": "Console"
  },
  "signal_flow": {
    "ports": [
      {"name": "Dante_Pri_In", "direction": "in", "connector": "etherCON", "channels": 64, "attributes": ["Dante", "primary"]},
      {"name": "Dante_Pri_Out", "direction": "out", "connector": "etherCON", "channels": 64, "attributes": ["Dante", "primary"]},
      {"name": "Dante_Sec_In", "direction": "in", "connector": "etherCON", "channels": 64, "attributes": ["Dante", "secondary"]},
      {"name": "Dante_Sec_Out", "direction": "out", "connector": "etherCON", "channels": 64, "attributes": ["Dante", "secondary"]},
      {"name": "OMNI_In", "direction": "in", "connector": "XLR", "channels": 8, "attributes": ["Analogue"]},
      {"name": "OMNI_Out", "direction": "out", "connector": "XLR", "channels": 8, "attributes": ["Analogue"]},
      {"name": "WordClock_In", "direction": "in", "connector": "BNC_75", "channels": 1, "attributes": ["WordClock"]},
      {"name": "WordClock_Out", "direction": "out", "connector": "BNC_75", "channels": 1, "attributes": ["WordClock"]}
    ],
    "bridges": []
  },
  "power_specs": {
    "power_draw_w": null,
    "voltage": null,
    "thermal_btuh": null,
    "poe_budget_w": null,
    "poe_draw_w": null
  },
  "physical_specs": {
    "height_mm": null,
    "width_mm": null,
    "depth_mm": null,
    "weight_kg": null
  },
  "extraction_confidence": "medium",
  "notes": "Console category means no bridges. OMNI and Dante channel counts from corpus. Physical specs not found in searched chunks."
}
```

## Validation Checklist

Before emitting JSON, verify:

- [ ] `device_metadata` has all 5 required string fields.
- [ ] `signal_flow.ports` is an array; every port has `name`, `direction`, `connector`, `channels`, `attributes`.
- [ ] `direction` is one of `in`, `out`, `io`.
- [ ] `connector` is from the approved list.
- [ ] `channels` is a positive integer (or `1` for scalar ports like WordClock).
- [ ] `attributes` is an array of strings; valid values include `Dante`, `primary`, `secondary`, `MADI`, `AES3`, `AES67`, `SDI`, `SMPTE2110`, `NDI`, `Analogue`, `OptoCore`, `TWINLANe`, `WordClock`, `RF`, `USB`, `redundant`.
- [ ] `signal_flow.bridges` is an array of strings in the form `"Source -> Dest"`.
- [ ] `extraction_confidence` is exactly `high`, `medium`, or `low`.
- [ ] No markdown, no trailing commas, no comments inside the JSON.
