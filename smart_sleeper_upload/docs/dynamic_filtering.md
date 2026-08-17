# Dynamic Filtering

This filtering framework consumes already-decoded canonical SMART Sleeper records. The pipeline fetches and decodes MeshNET source rows before calling it; the filter itself does not split packets, infer byte offsets, redesign storage, or perform operating-state, train-event, environmental-regression, ADWIN, conformal, neural-network, or ML fault classification.

## Layers

1. Hard/static filtering runs the existing deterministic record rules. It validates payload shape, numeric conversion, impossible physical values, invalid timestamps or sentinel values, invalid enums, function/family mismatches, and the existing environment, gas, GPS, network, device telemetry, microphone FFT, and AE checks. This remains the only layer that normally assigns `quality_state = "invalid"`.

2. Context-aware filtering compares related decoded values without a long baseline. Current mechanisms include peer-sensor groups, configured cross-field comparisons, and recent or same-timestamp cross-family context. Context findings include `affected_fields` so only affected adaptive baselines are protected.

3. Historical/adaptive filtering keeps causal state by `(device_id, family, field_name)`. It supports rolling median/MAD outliers, step changes, rate changes, stuck-sensor detection, reporting gaps, family silence, quarantine, and baseline re-entry.

The system annotates original observations with structured evidence instead of deleting records or replacing values. Hard/static rules may still null individual impossible fields as before. Dynamic runs normalize evidence to:

```json
{
  "filter_evidence": {
    "hard": [],
    "context": [],
    "adaptive": [],
    "sequence": []
  }
}
```

Legacy hard-only output remains unchanged in `off` mode.

## Modes

`off` runs hard/static filtering only. It does not add dynamic evidence, load state, or save state.

`shadow` runs dynamic logic and writes evidence/report/state, but does not merge candidate dynamic flags into `quality_flags` and does not upgrade quality states.

`enforce` runs dynamic logic and may upgrade decoded records to suspect. It never downgrades existing states and never changes invalid to suspect.

Public API and CLI mode defaults are aligned:

- no dynamic config and no explicit mode: `off`
- dynamic config supplied and no explicit mode: `shadow`
- explicit `off`, `shadow`, or `enforce`: use that mode

Example:

```powershell
python filter_rules.py --records decoded.jsonl --outdir out --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow
```

Persistent state:

```powershell
python filter_rules.py --records decoded.jsonl --outdir out --dynamic-config config/dynamic_filtering.json --dynamic-state out/dynamic_state.json --dynamic-mode shadow
```

## Configuration

Static hard-rule thresholds remain in `config/filter_thresholds.json`.

Dynamic rules are configured in `config/dynamic_filtering.json`. Example values are provisional. Production thresholds still require calibration from real decoded SMART Sleeper data. Rules without explicit thresholds remain disabled.

Global controls:

- `enabled`: disables all dynamic behavior when `false`
- `enabled_families`: optional family allow-list
- `enabled_fields`: supports both `temperature_c` and `environment.temperature_c`
- `allow_suspect_context`: defaults to `false`, so suspect records are not trusted cross-family context unless explicitly allowed
- `allow_candidate_context`: defaults to `false`, so records with dynamic context, adaptive, or sequence candidates are not written into trusted context in shadow mode
- `deduplication_capacity`: bounds the persisted fingerprint ledger

## Rolling Median / MAD

Adaptive outlier detection is causal: the current value is evaluated only against earlier accepted values. The current sample is never included in its own baseline.

Preferred scale calculation:

```text
rolling_median = median(history)
mad = median(abs(history_value - rolling_median))
robust_scale = max(1.4826 * mad, minimum_scale)
score = abs(current_value - rolling_median) / robust_scale
```

`minimum_scale` is field-specific and uses the same units as the field. It prevents zero or tiny MAD from producing unreasonable scores. The older `epsilon` field is still accepted for backward compatibility when `minimum_scale` is not supplied.

## Cold Start

Before `min_samples` accepted historical values exist, adaptive baseline rules emit non-candidate cold-start evidence and do not make anomaly decisions. Hard-invalid records and context-affected fields do not update accepted adaptive histories.

## Quarantine and Re-Entry

Point-level adaptive candidates are not immediately added to the accepted baseline. When `baseline_reentry.enabled` is true for a field, candidates are held in a quarantine buffer. A stable new cluster can confirm a baseline shift when it satisfies configured consecutive-sample, value-tolerance, and duration requirements.

Supported action:

```text
reset
```

On confirmation, the accepted history is replaced with the stable quarantined cluster, a baseline version is incremented, and an `adaptive.baseline_shift` finding is emitted. Single outliers followed by normal values clear quarantine without migrating the baseline.

Re-entry never uses hard-invalid records, missing-timestamp records, context-affected fields, non-numeric values, or values from another device/family/field.

## Persistent State

`--dynamic-state` loads and saves dynamic state using a versioned JSON structure:

```json
{
  "state_schema_version": 3,
  "updated_at_utc": "...",
  "adaptive": {},
  "context": {},
  "events": {},
  "deduplication": {}
}
```

The state includes rolling histories, previous values, stuck runs, last-seen timestamps, latest trusted context records, family-silence markers, quarantine buffers, baseline versions, event aggregation state, and duplicate-processing fingerprints.

State writes are atomic: a temporary file is written in the same directory and then replaces the target. Unsupported future state versions fail clearly instead of being discarded.

Schema v2 stores deduplication entries with both `fingerprint` and `device_id`, so `reset_device(device_id)` can clear only one device's ledger. Schema v3 stores tuple map keys as compact JSON array strings, so platform-derived device IDs may contain `|` safely. Schema v1 files are migrated by preserving adaptive, context and event state while clearing the legacy unmappable fingerprint-only ledger. Schema v2 files keep dedup, histories, context, events, quarantine and baseline versions while legacy pipe-delimited keys are decoded with right-splitting.

The normal Supabase pipeline uses staged state: it loads previous state, computes a candidate next state, and persists records/report/events/state through the protected RPC. It does not write filtered results or dynamic state locally. Local-only mode with `--skip-supabase` writes the debug output and state files instead.

Duplicate record fingerprints skip state updates and add non-candidate evidence:

```json
{
  "rule_id": "dynamic.duplicate_state_update_skipped",
  "state": "not_updated"
}
```

## Timestamp Policy

Records without a parseable timestamp still pass through hard filtering, but they do not update time-ordered adaptive history, previous-value state, stuck-duration state, recent cross-family context, reporting-gap state, family-silence state, event confirmation, or deduplication. They emit non-candidate evidence:

```json
{
  "rule_id": "dynamic.timestamp_unavailable",
  "state": "not_evaluated"
}
```

Same-timestamp cross-family context is handled with a deterministic group snapshot, so results do not depend on input order or alphabetical family order. Future timestamps are never used as context.

## Event Confirmation

Event aggregation is optional and only combines repeated point-level data-quality candidates. It is not operating-state classification or fault diagnosis.

Aggregation key:

```text
(device_id, family, field_name, candidate_flag)
```

Supported enforcement policies:

- `point`: preserve existing enforce behavior
- `confirmed_event`: upgrade to suspect only when the configured event condition is met

Event state tracks continuous anomalies as open events with stable IDs. Recent contributing record IDs are bounded by `window_points`, but total point count and the original start time are retained, so long anomalies do not receive new IDs when the recent window slides. Confirmed, updated and closed summaries are reported under `anomaly_events` with deterministic event IDs, time range, point count, contributing record IDs, field, device, family, candidate flag and status.

## Supabase Persistence

Dynamic ingestion uses `supabase_uploader.py` and the protected Edge Function in `supabase/functions/ingest-dynamic-v2/index.ts`. The handler is wrapped with `withSupabase({ auth: "secret" })`, requires the `apikey` header, and uses `ctx.supabaseAdmin.rpc("ingest_dynamic_v2", ...)`.

See `supabase_dynamic.md` for environment variables, deployment commands, retry behavior, deterministic run keys, and the local-only fallback.

## Future Records

Future decoded SMART Sleeper records can enter the same filter if they provide the canonical record fields used here, such as `record_id`, device identifier, `family`, timestamp fields, `payload`, `quality_state`, `quality_flags`, `analysis_tags`, `platform_meta`, and `raw_unmapped`.
