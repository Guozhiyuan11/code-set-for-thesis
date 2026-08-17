"""Fetch and decode SMART Sleeper records from the MeshNET JSON endpoint."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import urllib.error
import urllib.request
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable

from decoder.decoder import PACKET_FIELDS, SMARTSleeperFrame


SOURCE_NAME = "smart_sleeper"
SOURCE_TYPE = "smart_sleeper"
DEFAULT_SOURCE_URL = (
    "https://meshnetdev.thearcsgroup.com/smartsleepertest2/"
    "app/DeviceGroups/SmartSleeper/influx/json"
)
DEFAULT_TIMEOUT_SECONDS = 30.0
PLATFORM_META_FIELDS = (
    "ControllerName",
    "Area",
    "Site",
    "Location",
    "Latitude",
    "Longitude",
)

JsonObject = dict[str, Any]
Row = dict[str, Any]
Record = dict[str, Any]


class SmartSleeperSourceError(RuntimeError):
    """Raised for safe-to-display source fetch failures."""


def fetch_smart_sleeper_json(
    *,
    source_url: str | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> JsonObject:
    """Fetch the current SMART Sleeper Influx export using HTTP Basic Auth."""

    username = os.environ.get("SMART_SLEEPER_USERNAME")
    password = os.environ.get("SMART_SLEEPER_PASSWORD")
    if not username or not password:
        raise SmartSleeperSourceError(
            "SMART_SLEEPER_USERNAME and SMART_SLEEPER_PASSWORD must be set"
        )

    endpoint = source_url or os.environ.get("SMART_SLEEPER_URL") or DEFAULT_SOURCE_URL
    auth = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    request = urllib.request.Request(
        endpoint,
        headers={
            "accept": "application/json",
            "authorization": f"Basic {auth}",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            body = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 401:
            message = "source authentication failed"
        else:
            message = f"source returned HTTP {exc.code}"
        raise SmartSleeperSourceError(message) from exc
    except urllib.error.URLError as exc:
        reason = "request timed out" if isinstance(exc.reason, socket.timeout) else str(exc.reason)
        raise SmartSleeperSourceError(f"source connection failed: {reason}") from exc
    except TimeoutError as exc:
        raise SmartSleeperSourceError("source request timed out") from exc

    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SmartSleeperSourceError("source returned invalid JSON") from exc
    return validate_smart_sleeper_export(payload)


def load_smart_sleeper_json(path: str | Path) -> JsonObject:
    """Load a saved SMART Sleeper JSON response for offline testing."""

    try:
        with Path(path).open("r", encoding="utf-8-sig") as handle:
            payload = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid SMART Sleeper JSON: {exc.msg}") from exc
    return validate_smart_sleeper_export(payload)


def validate_smart_sleeper_export(payload: Any) -> JsonObject:
    """Validate the source response envelope."""

    if not isinstance(payload, dict):
        raise ValueError("SMART Sleeper response must be a JSON object")
    if not isinstance(payload.get("results"), list):
        raise ValueError("SMART Sleeper response must contain a top-level 'results' list")
    return payload


def rows_from_smart_sleeper_export(payload: JsonObject) -> list[Row]:
    """Expand Influx columns and values into row objects."""

    rows: list[Row] = []
    for result in payload["results"]:
        if not isinstance(result, dict):
            continue
        series_list = result.get("series")
        if not isinstance(series_list, list):
            continue
        for series in series_list:
            rows.extend(_load_series_rows(series))
    return rows


def decode_smart_sleeper_rows(rows: Iterable[Row]) -> list[Record]:
    """Decode filter-ready environment records from source rows."""

    records: list[Record] = []
    for row_index, row in enumerate(rows):
        timestamp = row.get("time")
        if not _has_value(timestamp):
            raise ValueError(f"SMART Sleeper row {row_index} is missing required 'time'")

        frame = SMARTSleeperFrame.decode(row)
        if frame.env_frame is None:
            continue

        values = asdict(frame.env_frame)
        payload = {
            "rtd1_t_x100": values["rtd1_t"],
            "rtd2_t_x100": values["rtd2_t"],
            "rtd3_t_x100": values["rtd3_t"],
            "rtd4_t_x100": values["rtd4_t"],
            "tmp102_t_x100": values["tmp102_t"],
            **{
                key: values[key]
                for key in (
                    "moist_pc",
                    "flood_flag",
                    "rain_mm",
                    "sleeper_rh",
                    "lat",
                    "lon",
                    "year",
                    "month",
                    "day",
                    "hour",
                    "minute",
                    "second",
                )
            },
        }
        platform_meta = _pick_present_fields(row, PLATFORM_META_FIELDS)
        records.append(
            {
                "record_id": _stable_record_id(row, frame.device_id, str(timestamp)),
                "family": "environment",
                "timestamp": timestamp,
                "source_time_utc": timestamp,
                "source_type": SOURCE_TYPE,
                "schema_version": "decoded_record/v1",
                "sleeper_id": frame.device_id,
                "device_id": frame.device_id,
                "payload": payload,
                "platform_meta": platform_meta,
                "raw_unmapped": _pick_raw_unmapped(row, set(platform_meta) | {"time"}),
                "quality_state": "decoded",
                "quality_flags": [],
            }
        )
    return records


def _load_series_rows(series: Any) -> list[Row]:
    if not isinstance(series, dict):
        return []
    columns = series.get("columns")
    values = series.get("values")
    if not isinstance(columns, list) or not isinstance(values, list):
        return []
    if "FirstPacket" not in columns:
        return []

    rows: list[Row] = []
    for value_row in values:
        if isinstance(value_row, list):
            rows.append(dict(zip(columns, value_row)))
    return rows


def _stable_record_id(row: Row, device_id: str, timestamp: str) -> str:
    identity = {
        "device_id": device_id,
        "timestamp": timestamp,
        "packets": {
            field_name: row.get(field_name)
            for field_name, _ in PACKET_FIELDS
            if _has_value(row.get(field_name))
        },
    }
    encoded = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return "smart_sleeper:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _has_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip()) and value.strip().lower() != "null"
    return True


def _pick_present_fields(row: Row, fields: tuple[str, ...]) -> JsonObject:
    return {field: row[field] for field in fields if _has_value(row.get(field))}


def _pick_raw_unmapped(row: Row, used_fields: set[str]) -> JsonObject:
    return {
        field: value
        for field, value in row.items()
        if field not in used_fields and _has_value(value)
    }
