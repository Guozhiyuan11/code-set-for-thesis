# SMART Sleeper Data Filter

This project fetches SMART Sleeper MeshNET JSON data, decodes environment sensor packets, applies static and dynamic quality filtering, and writes the results to Supabase.

## Project layout

```text
config/                     Static and dynamic filter configuration
data/smart_sleeper_data.json Redacted offline SMART Sleeper sample
decoder/                     SMART Sleeper packet decoder
filters/                     Dynamic filtering and persistent state
scripts/                     Windows local scheduler
supabase/                    Protected Supabase Edge Function
tests/                       Python and TypeScript tests
run_pipeline.py              Main pipeline entry point
smart_sleeper_source.py      Source fetch and decoding adapter
filter_rules.py              Static and dynamic filtering facade
supabase_uploader.py         Supabase upload boundary
```

## Configuration

Copy the environment template and set values only in your local `.env` file:

```powershell
Copy-Item .env.example .env
```

Required values:

```text
SMART_SLEEPER_USERNAME
SMART_SLEEPER_PASSWORD
SUPABASE_URL
SUPABASE_SECRET_KEY
```

Never commit `.env` or `.env.local`.

## Run

Run against the redacted offline sample without Supabase:

```powershell
python run_pipeline.py --input-json data/smart_sleeper_data.json --skip-supabase --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow
```

Fetch live SMART Sleeper data and write to Supabase:

```powershell
python run_pipeline.py --dynamic-config config/dynamic_filtering.json --dynamic-mode shadow
```

The normal pipeline fetches the full source snapshot every 15 minutes. Duplicate records are not rewritten to `processed_records`, while every scheduled interval still produces a Supabase `ingest_runs` record.

Install the Windows scheduler:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/install_local_schedule.ps1
```

## Checks

```powershell
python -m pytest -q
npm ci
npm test
npm run typecheck
```

See [dynamic filtering](docs/dynamic_filtering.md), [Supabase integration](docs/supabase_dynamic.md), and the [local automation manual](docs/local_automation_manual.md) for details.
