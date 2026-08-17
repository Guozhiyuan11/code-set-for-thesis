"""Run the SMART Sleeper source, filtering, and Supabase pipeline once."""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from filter_rules import (
    filter_decoded_records_with_state,
    save_cleaned_records,
    save_quality_report,
)
from filters.config import load_dynamic_filter_config, normalize_dynamic_mode
from filters.models import parse_record_timestamp, record_fingerprint
from filters.state import DynamicState, load_dynamic_state, save_dynamic_state
from smart_sleeper_source import (
    SOURCE_NAME,
    SOURCE_TYPE,
    SmartSleeperSourceError,
    decode_smart_sleeper_rows,
    fetch_smart_sleeper_json,
    load_smart_sleeper_json,
    rows_from_smart_sleeper_export,
)
from supabase_uploader import SupabaseIngestError, fetch_dynamic_state, upload_dynamic


DEFAULT_PIPELINE_VERSION = "dynamic-v2"
DEFAULT_STATE_KEY = "smart_sleeper:default"
DEFAULT_SOURCE_NAME = "SmartSleeper/influx/json"


def build_paths(workdir: str | Path | None = None) -> dict[str, Path]:
    """Build the working paths for all pipeline stages."""

    resolved_workdir = (
        Path(workdir).expanduser().resolve()
        if workdir
        else (Path.cwd() / "smart_sleeper_local_output").resolve()
    )
    filtered_output_dir = resolved_workdir / "filtered_output"
    return {
        "workdir": resolved_workdir,
        "filtered_output_dir": filtered_output_dir,
        "cleaned_records_jsonl": filtered_output_dir / "cleaned_records.jsonl",
        "quality_report_json": filtered_output_dir / "quality_report.json",
        "dynamic_state_json": filtered_output_dir / "dynamic_state.json",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-json",
        help="Optional saved SMART Sleeper response; otherwise fetch the configured HTTP endpoint",
    )
    parser.add_argument("--source-url", help="Optional SMART Sleeper JSON endpoint override")
    parser.add_argument("--workdir", help="Working directory used only with --skip-supabase")
    parser.add_argument("--env-file", default=".env", help="Local KEY=VALUE secrets file")
    parser.add_argument("--source-file-name", help="Source label stored in Supabase ingest metadata")
    parser.add_argument(
        "--execution-slot",
        help="Optional UTC run slot override; defaults to the current 15-minute interval",
    )
    parser.add_argument(
        "--skip-supabase",
        action="store_true",
        help="Write filtering results locally instead of uploading to Supabase",
    )
    parser.add_argument("--supabase-ingest-url", help="Protected ingest handler URL override")
    parser.add_argument(
        "--pipeline-version",
        default=DEFAULT_PIPELINE_VERSION,
        help=f"Pipeline version stored in ingest metadata (default: {DEFAULT_PIPELINE_VERSION})",
    )
    parser.add_argument(
        "--dynamic-state-key",
        default=DEFAULT_STATE_KEY,
        help=f"Supabase dynamic state key (default: {DEFAULT_STATE_KEY})",
    )
    parser.add_argument("--dynamic-config", help="Optional dynamic filtering JSON config")
    parser.add_argument("--dynamic-state", help="Optional local-only dynamic state JSON")
    parser.add_argument(
        "--dynamic-mode",
        choices=("off", "shadow", "auto", "enforce"),
        help="Dynamic filtering mode",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env_file(args.env_file)
    try:
        execution_slot = resolve_execution_slot(args.execution_slot)
    except ValueError as exc:
        print(f"Invalid execution slot: {exc}", file=sys.stderr)
        return 1

    paths = build_paths(args.workdir)
    if args.skip_supabase:
        paths["workdir"].mkdir(parents=True, exist_ok=True)
        paths["filtered_output_dir"].mkdir(parents=True, exist_ok=True)

    try:
        if args.input_json:
            input_path = Path(args.input_json).expanduser().resolve()
            if not input_path.is_file():
                raise FileNotFoundError(f"SMART Sleeper input JSON not found: {input_path}")
            source_payload = load_smart_sleeper_json(input_path)
            source_file_name = args.source_file_name or input_path.name
        else:
            source_payload = fetch_smart_sleeper_json(source_url=args.source_url)
            source_file_name = args.source_file_name or DEFAULT_SOURCE_NAME
        source_rows = rows_from_smart_sleeper_export(source_payload)
        decoded_records = decode_smart_sleeper_rows(source_rows)
    except (FileNotFoundError, SmartSleeperSourceError, ValueError) as exc:
        print(f"SMART Sleeper source failed: {exc}", file=sys.stderr)
        return 1

    dynamic_state_path = (
        Path(args.dynamic_state).expanduser().resolve()
        if args.dynamic_state
        else paths["dynamic_state_json"]
    )
    filtering_mode = normalize_dynamic_mode(
        args.dynamic_mode,
        dynamic_config_supplied=args.dynamic_config is not None,
    )
    dynamic_config = _load_dynamic_config_for_mode(args.dynamic_config, filtering_mode)

    try:
        previous_state = _load_previous_state(
            skip_supabase=args.skip_supabase,
            filtering_mode=filtering_mode,
            state_key=args.dynamic_state_key,
            ingest_url=args.supabase_ingest_url,
            local_state_path=dynamic_state_path,
        )
    except SupabaseIngestError as exc:
        print(f"Supabase state load failed: {exc}", file=sys.stderr)
        return 1
    known_fingerprints = {
        entry.fingerprint for entry in previous_state.deduplication
    } if previous_state is not None else set()

    cleaned_records, record_report, candidate_state = filter_decoded_records_with_state(
        decoded_records,
        dynamic_config=dynamic_config,
        dynamic_mode=filtering_mode,
        dynamic_state=previous_state,
        dynamic_state_path=None if previous_state is not None else dynamic_state_path,
    )
    records_for_upload = (
        cleaned_records
        if args.skip_supabase
        else records_not_seen(cleaned_records, known_fingerprints)
    )
    record_report.update(
        {
            "state_key": args.dynamic_state_key,
            "pipeline_version": args.pipeline_version,
            "execution_slot": execution_slot,
            "source_rows_fetched": len(source_rows),
            "source_rows_selected": len(source_rows),
            "records_submitted": len(records_for_upload),
            "duplicate_records_omitted": len(cleaned_records) - len(records_for_upload),
        }
    )

    persistence_result: dict[str, Any] | None = None
    if args.skip_supabase:
        if candidate_state is not None:
            save_dynamic_state(candidate_state, dynamic_state_path)
            record_report["state_saved"] = True
        save_cleaned_records(cleaned_records, paths["cleaned_records_jsonl"])
        save_quality_report(record_report, paths["quality_report_json"])
    else:
        try:
            persistence_result = upload_dynamic(
                records=records_for_upload,
                quality_report=record_report,
                candidate_state=candidate_state,
                source_file_name=source_file_name,
                filtering_mode=filtering_mode,
                pipeline_version=args.pipeline_version,
                state_key=args.dynamic_state_key,
                execution_slot=execution_slot,
                run_record_fingerprints=[
                    record_fingerprint(record) for record in cleaned_records
                ],
                dynamic_config=dynamic_config,
                source_name=SOURCE_NAME,
                source_type=SOURCE_TYPE,
                ingest_url=args.supabase_ingest_url,
            )
        except SupabaseIngestError as exc:
            print(f"Supabase ingestion failed: {exc}", file=sys.stderr)
            return 1

    _print_summary(
        paths=paths,
        filtering_mode=filtering_mode,
        execution_slot=execution_slot,
        source_rows=len(source_rows),
        selected_rows=len(source_rows),
        records=len(cleaned_records),
        submitted_records=len(records_for_upload),
        supabase_load_performed=not args.skip_supabase,
        local_output=args.skip_supabase,
        persistence_result=persistence_result,
    )
    return 0


def load_env_file(path: str | Path | None) -> None:
    """Load simple KEY=VALUE entries without adding a runtime dependency."""

    if not path:
        return
    env_path = Path(path).expanduser()
    if not env_path.is_file():
        return
    for line_number, raw_line in enumerate(env_path.read_text(encoding="utf-8-sig").splitlines(), 1):
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid env entry on line {line_number} of {env_path}")
        key, value = line.split("=", 1)
        key = key.strip()
        if not key.isidentifier():
            raise ValueError(f"Invalid env key on line {line_number} of {env_path}")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def resolve_execution_slot(value: str | None, *, now: datetime | None = None) -> str:
    """Return a stable UTC key for one 15-minute scheduled interval."""

    if value is not None:
        parsed = parse_record_timestamp(value)
        if parsed is None:
            raise ValueError("must be an ISO-8601 timestamp")
        return parsed.isoformat()
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return current.replace(minute=(current.minute // 15) * 15, second=0, microsecond=0).isoformat()


def records_not_seen(
    records: list[dict[str, Any]],
    known_fingerprints: set[str],
) -> list[dict[str, Any]]:
    """Return records absent from the atomically persisted dynamic-state ledger."""

    # ponytail: the bounded state ledger is the duplicate index; query
    # processed_records if state and record persistence are ever decoupled.
    return [record for record in records if record_fingerprint(record) not in known_fingerprints]


def _load_dynamic_config_for_mode(path: str | None, filtering_mode: str) -> dict[str, Any] | None:
    if filtering_mode == "off":
        return None
    return load_dynamic_filter_config(path)


def _load_previous_state(
    *,
    skip_supabase: bool,
    filtering_mode: str,
    state_key: str,
    ingest_url: str | None,
    local_state_path: Path,
) -> DynamicState | None:
    if filtering_mode == "off":
        return None
    if skip_supabase:
        state, _ = load_dynamic_state(local_state_path)
        return state
    remote_state = fetch_dynamic_state(state_key=state_key, ingest_url=ingest_url)
    if remote_state.get("found") is True:
        state_json = remote_state.get("state_json")
        if not isinstance(state_json, dict):
            raise SupabaseIngestError("remote dynamic state payload is malformed")
        return DynamicState.restore(state_json)
    return DynamicState.empty()


def _print_summary(
    *,
    paths: dict[str, Path],
    filtering_mode: str,
    execution_slot: str,
    source_rows: int,
    selected_rows: int,
    records: int,
    submitted_records: int,
    supabase_load_performed: bool,
    local_output: bool,
    persistence_result: dict[str, Any] | None,
) -> None:
    print("\nPipeline Summary")
    print("result_destination: local" if local_output else "result_destination: supabase")
    if local_output:
        print(f"workdir: {paths['workdir']}")
        print(f"cleaned_records_jsonl: {paths['cleaned_records_jsonl']}")
        print(f"quality_report_json: {paths['quality_report_json']}")
    print(f"filtering_mode: {filtering_mode}")
    print(f"execution_slot: {execution_slot}")
    print(f"source_type: {SOURCE_TYPE}")
    print(f"source_rows_fetched: {source_rows}")
    print(f"source_rows_selected: {selected_rows}")
    print(f"processed_records: {records}")
    print(f"records_submitted: {submitted_records}")
    print(f"duplicate_records_omitted: {records - submitted_records}")
    print(f"supabase_load_performed: {supabase_load_performed}")
    if persistence_result is not None:
        for key in (
            "ingest_run_id",
            "run_key",
            "inserted_records",
            "updated_records",
            "skipped_records",
            "anomaly_events",
            "state_saved",
        ):
            print(f"{key}: {persistence_result[key]}")


if __name__ == "__main__":
    raise SystemExit(main())
