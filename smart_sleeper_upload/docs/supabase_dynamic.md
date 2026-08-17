# Supabase Dynamic Integration

This project writes dynamic filtering output through the existing Supabase RPC:

```text
public.ingest_dynamic_v2
```

It uses the existing upgraded tables:

```text
public.ingest_runs
public.processed_records
public.anomaly_events
public.dynamic_filter_state
```

No duplicate dynamic-ingestion tables are created by application code.

## Dynamic Record Views

The typed read views for analytics and application reads are:

```text
public.v_environment_records
public.v_smart_sleeper_environment_records
```

These views project family-specific rows from `public.processed_records`.
`v_smart_sleeper_environment_records` additionally restricts rows to
`source_type = 'smart_sleeper'` and projects the decoded RTD, sleeper,
moisture, flood, rain, sensor-location, and sensor-time fields as typed columns.
They keep the original static columns first, then append dynamic-v2 metadata:

```text
family
device_id
sleeper_id
source_time_utc
ingest_time_utc
schema_version
batch_id
function_id
pipeline_version
filtering_mode
dynamic_config_hash
record_fingerprint
payload_raw
platform_meta
filter_evidence
record_json
created_at
updated_at
```

Numeric payload projections use the database `ss_try_double_precision(...)`
helper so malformed payload values return `null` instead of failing the whole
view query.

The views are configured with `security_invoker = true`, `SELECT` is granted to
`authenticated` and `service_role`, and direct `anon` access is revoked. This
keeps view reads aligned with the underlying `processed_records` RLS policies.

## Environment

Copy `.env.example` and set values outside source control:

```text
SMART_SLEEPER_URL=
SMART_SLEEPER_USERNAME=
SMART_SLEEPER_PASSWORD=
SUPABASE_URL=
SUPABASE_PUBLISHABLE_KEY=
SUPABASE_SECRET_KEY=
SUPABASE_JWKS_URL=
SUPABASE_INGEST_URL=
```

`SUPABASE_SECRET_KEY` is used only server-to-server. Do not pass it in CLI arguments and do not expose it to browser code.

## Edge Function

The protected handler is:

```text
supabase/functions/ingest-dynamic-v2/index.ts
```

It uses:

```ts
withSupabase({ auth: "secret" }, handler)
```

Secret mode requires callers to send the secret in the `apikey` header. The handler uses `ctx.supabaseAdmin`, not `ctx.supabase`, and writes only by calling:

```text
ctx.supabaseAdmin.rpc("ingest_dynamic_v2", ...)
```

`supabase/config.toml` contains:

```toml
[functions.ingest-dynamic-v2]
verify_jwt = false
```

This disables platform JWT verification for this function because `@supabase/server` handles secret-key authentication.

## Atomic Ingestion

The Python pipeline builds one request containing:

- run metadata and deterministic `run_key`
- only processed records whose stable fingerprints are not already in remote state
- quality report
- anomaly-event summaries
- candidate dynamic state

The RPC atomically persists the run, records, report, anomaly events and dynamic state. The RPC is still called with an empty records array when the entire source snapshot is already known, so that every 15-minute execution has an `ingest_runs` row without rewriting `processed_records`.

## State Flow

When Supabase persistence is enabled:

1. The pipeline reads remote state from `GET /functions/v1/ingest-dynamic-v2?state_key=...`.
2. It fetches and filters the complete SMART Sleeper snapshot.
3. Fingerprints already present in the remote state ledger are omitted from the records array.
4. The candidate state, quality report and remaining records are uploaded through the RPC.

There is no local progress checkpoint. If upload fails, the next scheduled execution reads the complete source again and retries safely. Normal Supabase mode does not write filtered records, reports, or dynamic state to local files.

When `--skip-supabase` is used, the pipeline runs in explicit local-only debug mode and writes the records, report, and dynamic state locally.

## Determinism

The uploader computes:

- `dynamic_config_hash` with JSON sorted keys, stable separators and SHA-256
- `run_key` from the 15-minute execution slot, source metadata, filtering configuration, state key and the full logical snapshot fingerprints

Retries in the same execution slot reuse the same key, including when a retry omits records that were persisted by the first attempt. The next 15-minute slot receives a new key and therefore a new `ingest_runs` row.

## Commands

Install TypeScript dependencies:

```bash
npm install
```

Run Python tests:

```bash
pytest -q
```

Run handler tests and type checking:

```bash
npm test
npm run typecheck
```

Fetch SMART Sleeper data and run the pipeline with Supabase ingestion:

```bash
python run_pipeline.py --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow
```

Run an offline SMART Sleeper sample without Supabase:

```bash
python run_pipeline.py --input-json data/smart_sleeper_data.json --skip-supabase --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow
```

Deploy the function with the Supabase CLI:

```bash
supabase functions deploy ingest-dynamic-v2
```
