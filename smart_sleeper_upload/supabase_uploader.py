"""Supabase dynamic ingestion client."""

from __future__ import annotations

import hashlib
import json
import os
import socket
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from filters.config import VALID_DYNAMIC_MODES
from filters.models import canonical_timestamp, record_fingerprint
from filters.state import DynamicState, STATE_SCHEMA_VERSION


SOURCE_NAME = "smart_sleeper"
SOURCE_TYPE = "smart_sleeper"
DEFAULT_TIMEOUT_SECONDS = 30.0

JsonObject = dict[str, Any]
Record = dict[str, Any]


class SupabaseIngestError(RuntimeError):
    """Raised for safe-to-display Supabase ingestion failures."""


def load_jsonl_records(path: str | Path) -> list[Record]:
    """Load JSONL records for upload."""

    records: list[Record] = []
    jsonl_path = Path(path)
    with jsonl_path.open("r", encoding="utf-8-sig") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Invalid JSON in {jsonl_path} on line {line_number}: {exc.msg}"
                ) from exc
            if not isinstance(parsed, dict):
                raise ValueError(f"JSONL line {line_number} in {jsonl_path} must contain an object")
            records.append(parsed)
    return records


def load_json_object(path: str | Path) -> JsonObject:
    """Load a JSON object."""

    json_path = Path(path)
    try:
        with json_path.open("r", encoding="utf-8-sig") as handle:
            parsed = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON in {json_path}: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"{json_path} must contain a JSON object")
    return parsed


def canonical_config_hash(config: Any) -> str | None:
    """Return a deterministic SHA-256 hash for dynamic configuration."""

    if config is None:
        return None
    encoded = json.dumps(config, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def deterministic_run_key(
    *,
    source_type: str,
    source_file_name: str,
    pipeline_version: str,
    filtering_mode: str,
    dynamic_config_hash: str | None,
    record_fingerprints: list[str],
    state_key: str,
    execution_slot: str,
) -> str:
    """Build a stable idempotency key for one scheduled ingest run."""

    body = {
        "source_type": source_type,
        "source_file_name": source_file_name,
        "pipeline_version": pipeline_version,
        "filtering_mode": filtering_mode,
        "dynamic_config_hash": dynamic_config_hash,
        "record_fingerprints": sorted(record_fingerprints),
        "state_key": state_key,
        "execution_slot": execution_slot,
    }
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "dynamic-v2:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def extract_anomaly_events(quality_report: JsonObject) -> list[JsonObject]:
    """Return anomaly-event summaries from a quality report."""

    events = quality_report.get("anomaly_events", [])
    if not isinstance(events, list):
        raise SupabaseIngestError("quality_report.anomaly_events must be an array")
    extracted: list[JsonObject] = []
    for index, event in enumerate(events):
        if not isinstance(event, dict):
            raise SupabaseIngestError(f"anomaly event {index} must be an object")
        extracted.append(dict(event))
    return extracted


def prepare_records_for_upload(records: list[Record], *, source_type: str) -> list[Record]:
    """Validate and normalize complete processed-record payloads."""

    seen_record_ids: set[str] = set()
    prepared: list[Record] = []
    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise SupabaseIngestError(f"record {index} must be an object")
        item = dict(record)
        record_id = _required_text(item, "record_id", index)
        if record_id in seen_record_ids:
            raise SupabaseIngestError(f"duplicate record_id in upload payload: {record_id}")
        seen_record_ids.add(record_id)
        _required_text(item, "family", index)
        timestamp = canonical_timestamp(item)
        if timestamp is None:
            raise SupabaseIngestError(f"record {record_id} is missing a valid canonical timestamp")
        item.setdefault("timestamp", timestamp)
        item.setdefault("source_time_utc", timestamp)
        item.setdefault("source_type", source_type)
        item.setdefault("quality_state", "decoded")
        item.setdefault("quality_flags", [])
        item.setdefault("analysis_tags", [])
        item.setdefault("payload", {})
        item.setdefault("platform_meta", {})
        item.setdefault("raw_unmapped", {})
        item.setdefault("filter_evidence", {})
        item["record_fingerprint"] = str(item.get("record_fingerprint") or record_fingerprint(item))
        prepared.append(item)
    return prepared


def build_ingest_payload(
    *,
    records: list[Record],
    quality_report: JsonObject,
    candidate_state: DynamicState | JsonObject | None,
    source_file_name: str,
    filtering_mode: str,
    pipeline_version: str,
    state_key: str,
    execution_slot: str,
    run_record_fingerprints: list[str] | None = None,
    dynamic_config: Any = None,
    source_name: str = SOURCE_NAME,
    source_type: str = SOURCE_TYPE,
) -> JsonObject:
    """Build the fixed RPC payload accepted by the protected handler."""

    if filtering_mode not in VALID_DYNAMIC_MODES:
        raise SupabaseIngestError(f"unsupported filtering mode: {filtering_mode}")
    prepared_records = prepare_records_for_upload(records, source_type=source_type)
    config_hash = canonical_config_hash(dynamic_config)
    fingerprints = (
        run_record_fingerprints
        if run_record_fingerprints is not None
        else [str(record["record_fingerprint"]) for record in prepared_records]
    )
    run_key = deterministic_run_key(
        source_type=source_type,
        source_file_name=source_file_name,
        pipeline_version=pipeline_version,
        filtering_mode=filtering_mode,
        dynamic_config_hash=config_hash,
        record_fingerprints=fingerprints,
        state_key=state_key,
        execution_slot=execution_slot,
    )
    if isinstance(candidate_state, DynamicState):
        state_json: JsonObject | None = candidate_state.serialize()
    elif isinstance(candidate_state, dict):
        state_json = dict(candidate_state)
    elif candidate_state is None:
        state_json = None
    else:
        raise SupabaseIngestError("candidate dynamic state must be an object")

    return {
        "run": {
            "run_key": run_key,
            "source_name": source_name,
            "source_file_name": source_file_name,
            "source_type": source_type,
            "pipeline_version": pipeline_version,
            "filtering_mode": filtering_mode,
            "dynamic_config_hash": config_hash,
            "state_schema_version": STATE_SCHEMA_VERSION,
            "execution_slot": execution_slot,
        },
        "quality_report": quality_report,
        "records": prepared_records,
        "anomaly_events": extract_anomaly_events(quality_report),
        "state": {
            "state_key": state_key,
            "state_json": state_json or {},
        },
    }


def upload_dynamic(
    *,
    records: list[Record],
    quality_report: JsonObject,
    candidate_state: DynamicState | JsonObject | None,
    source_file_name: str,
    filtering_mode: str,
    pipeline_version: str,
    state_key: str,
    execution_slot: str,
    run_record_fingerprints: list[str] | None = None,
    dynamic_config: Any = None,
    source_name: str = SOURCE_NAME,
    source_type: str = SOURCE_TYPE,
    ingest_url: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> JsonObject:
    """Upload records/report/events/state through the secret-authenticated handler."""

    endpoint, secret_key = _resolve_endpoint_and_secret(ingest_url)
    payload = build_ingest_payload(
        records=records,
        quality_report=quality_report,
        candidate_state=candidate_state,
        source_file_name=source_file_name,
        filtering_mode=filtering_mode,
        pipeline_version=pipeline_version,
        state_key=state_key,
        execution_slot=execution_slot,
        run_record_fingerprints=run_record_fingerprints,
        dynamic_config=dynamic_config,
        source_name=source_name,
        source_type=source_type,
    )
    response = _send_json("POST", endpoint, secret_key, payload, timeout_seconds=timeout_seconds)
    return _validate_ingest_response(response, payload)


def fetch_dynamic_state(
    *,
    state_key: str,
    ingest_url: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> JsonObject:
    """Fetch a remote dynamic-state snapshot from the protected handler."""

    if not state_key.strip():
        raise SupabaseIngestError("state_key is required")
    endpoint, secret_key = _resolve_endpoint_and_secret(ingest_url)
    separator = "&" if "?" in endpoint else "?"
    url = f"{endpoint}{separator}{urllib.parse.urlencode({'state_key': state_key})}"
    response = _send_json("GET", url, secret_key, None, timeout_seconds=timeout_seconds)
    if not isinstance(response, dict):
        raise SupabaseIngestError("state GET response must be a JSON object")
    if response.get("found") is True:
        state_json = response.get("state_json")
        if not isinstance(state_json, dict):
            raise SupabaseIngestError("state GET response is missing state_json")
    elif response.get("found") is not False:
        raise SupabaseIngestError("state GET response is malformed")
    return response


def _resolve_endpoint_and_secret(ingest_url: str | None) -> tuple[str, str]:
    secret_key = os.environ.get("SUPABASE_SECRET_KEY")
    if not secret_key:
        raise SupabaseIngestError("SUPABASE_SECRET_KEY is not set")

    endpoint = ingest_url or os.environ.get("SUPABASE_INGEST_URL")
    if not endpoint:
        supabase_url = os.environ.get("SUPABASE_URL")
        if not supabase_url:
            raise SupabaseIngestError("SUPABASE_URL is not set")
        endpoint = supabase_url.rstrip("/") + "/functions/v1/ingest-dynamic-v2"
    return endpoint, secret_key


def _send_json(
    method: str,
    url: str,
    secret_key: str,
    payload: JsonObject | None,
    *,
    timeout_seconds: float,
) -> Any:
    encoded = None
    if payload is not None:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        method=method,
        headers={
            "content-type": "application/json",
            "accept": "application/json",
            "apikey": secret_key,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        safe_body = _safe_error_body(exc)
        raise SupabaseIngestError(f"Supabase handler returned HTTP {exc.code}: {safe_body}") from exc
    except urllib.error.URLError as exc:
        reason = exc.reason
        if isinstance(reason, socket.timeout):
            raise SupabaseIngestError("Supabase handler request timed out") from exc
        raise SupabaseIngestError(f"Supabase handler connection failed: {reason}") from exc
    except TimeoutError as exc:
        raise SupabaseIngestError("Supabase handler request timed out") from exc

    if status < 200 or status >= 300:
        raise SupabaseIngestError(f"Supabase handler returned HTTP {status}")
    try:
        return json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SupabaseIngestError("Supabase handler returned non-JSON response") from exc


def _validate_ingest_response(response: Any, request_payload: JsonObject) -> JsonObject:
    if not isinstance(response, dict):
        raise SupabaseIngestError("Supabase handler response must be a JSON object")
    if response.get("success") is not True:
        raise SupabaseIngestError("Supabase handler reported ingestion failure")
    if not isinstance(response.get("ingest_run_id"), str) or not response["ingest_run_id"].strip():
        raise SupabaseIngestError("Supabase handler response is missing ingest_run_id")
    if response.get("run_key") != request_payload["run"]["run_key"]:
        raise SupabaseIngestError("Supabase handler response run_key does not match request")
    for key in ("inserted_records", "updated_records", "skipped_records", "anomaly_events"):
        if not isinstance(response.get(key), int) or response[key] < 0:
            raise SupabaseIngestError(f"Supabase handler response has invalid {key}")
    total_returned = response["inserted_records"] + response["updated_records"] + response["skipped_records"]
    if total_returned != len(request_payload["records"]):
        raise SupabaseIngestError("Supabase handler returned record counts inconsistent with request")
    if response.get("state_saved") is not True:
        raise SupabaseIngestError("Supabase handler did not confirm dynamic state persistence")
    return {
        "success": True,
        "ingest_run_id": response["ingest_run_id"],
        "run_key": response["run_key"],
        "inserted_records": response["inserted_records"],
        "updated_records": response["updated_records"],
        "skipped_records": response["skipped_records"],
        "anomaly_events": response["anomaly_events"],
        "state_saved": response["state_saved"],
    }


def _required_text(record: Record, field_name: str, index: int) -> str:
    value = record.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise SupabaseIngestError(f"record {index} is missing required field {field_name}")
    return value.strip()


def _safe_error_body(exc: urllib.error.HTTPError) -> str:
    try:
        body = exc.read(4096).decode("utf-8", errors="replace")
    except Exception:
        return "response body unavailable"
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        return body[:500]
    if isinstance(parsed, dict):
        return json.dumps(
            {key: value for key, value in parsed.items() if "secret" not in key.lower()},
            sort_keys=True,
        )[:500]
    return str(parsed)[:500]
