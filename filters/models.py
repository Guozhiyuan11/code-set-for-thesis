"""Shared helpers for record filtering layers."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


QUALITY_SEVERITY = {"decoded": 0, "suspect": 1, "invalid": 2}
TIME_FIELDS = ("timestamp", "source_time_utc", "ingest_time_utc")


def encode_tuple_key(parts: tuple[str, ...]) -> str:
    """Encode tuple keys without delimiter collisions."""

    return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))


def decode_tuple_key(raw_key: str, *, length: int, name: str) -> tuple[str, ...]:
    """Decode JSON-array keys, falling back to the legacy pipe format."""

    if not isinstance(raw_key, str):
        raise ValueError(f"{name} key must be a string")
    try:
        parsed = json.loads(raw_key)
    except json.JSONDecodeError:
        parsed = None
    if isinstance(parsed, list) and len(parsed) == length and all(
        isinstance(item, str) for item in parsed
    ):
        return tuple(parsed)
    parts = raw_key.rsplit("|", length - 1)
    if len(parts) != length or any(part == "" for part in parts):
        raise ValueError(f"{name} key is invalid")
    return tuple(parts)


def normalize_quality_state(value: Any) -> str:
    """Normalize legacy and current state labels."""

    text = str(value or "").strip().lower()
    if text in {"invalid", "reject"}:
        return "invalid"
    if text == "suspect":
        return "suspect"
    return "decoded"


def merge_quality_state(existing: Any, derived: str) -> str:
    """Return the higher-severity quality state."""

    existing_state = normalize_quality_state(existing)
    derived_state = normalize_quality_state(derived)
    if QUALITY_SEVERITY[existing_state] >= QUALITY_SEVERITY[derived_state]:
        return existing_state
    return derived_state


def dedupe_sorted_labels(values: list[Any]) -> list[str]:
    """Return sorted unique non-empty string labels."""

    labels: set[str] = set()
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            labels.add(text)
    return sorted(labels)


def as_number(value: Any) -> float | None:
    """Return a finite numeric value or None."""

    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number in {float("inf"), float("-inf")}:
        return None
    return number


def parse_record_timestamp(value: Any) -> datetime | None:
    """Parse common decoded-record timestamps, trimming nanoseconds if needed."""

    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    sign_index = max(text.rfind("+"), text.rfind("-"))
    if sign_index <= 10:
        sign_index = len(text)
    main = text[:sign_index]
    suffix = text[sign_index:]
    if "." in main:
        prefix, fraction = main.split(".", 1)
        main = f"{prefix}.{fraction[:6]}"

    try:
        parsed = datetime.fromisoformat(f"{main}{suffix}")
    except ValueError:
        return None

    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def record_timestamp(record: dict[str, Any]) -> datetime | None:
    """Return the best available canonical timestamp for a decoded record."""

    for field_name in TIME_FIELDS:
        parsed = parse_record_timestamp(record.get(field_name))
        if parsed is not None:
            return parsed
    return None


def timestamp_sort_value(record: dict[str, Any]) -> tuple[int, str]:
    """Return a deterministic sort value for record time."""

    parsed = record_timestamp(record)
    if parsed is None:
        return (1, "")
    return (0, parsed.isoformat())


def canonical_timestamp(record: dict[str, Any]) -> str | None:
    """Return the canonical UTC timestamp string for a record."""

    parsed = record_timestamp(record)
    if parsed is None:
        return None
    return parsed.isoformat()


def device_id_for_record(record: dict[str, Any]) -> str:
    """Return a stable device key without depending on an upstream decoder."""

    for field_name in ("sleeper_id", "device_id", "gateway_id"):
        value = record.get(field_name)
        if isinstance(value, str) and value.strip():
            return value.strip()
        if value is not None and not isinstance(value, (dict, list)):
            return str(value)

    platform_meta = record.get("platform_meta")
    if isinstance(platform_meta, dict):
        parts: list[str] = []
        for field_name in ("ControllerName", "Site", "Location", "Area"):
            value = platform_meta.get(field_name)
            if isinstance(value, str) and value.strip():
                parts.append(value.strip())
            elif value is not None and not isinstance(value, (dict, list)):
                parts.append(str(value))
        if parts:
            return "platform:" + "|".join(parts)

    source_type = record.get("source_type")
    if isinstance(source_type, str) and source_type.strip():
        return f"source:{source_type.strip()}"

    return "unknown"


def ensure_filter_evidence(record: dict[str, Any], mode: str) -> dict[str, Any]:
    """Return the structured filter evidence object, creating it when needed."""

    existing = record.get("filter_evidence")
    if not isinstance(existing, dict):
        existing = {}
    existing["mode"] = mode
    for layer in ("hard", "context", "adaptive", "model", "sequence"):
        if not isinstance(existing.get(layer), list):
            existing[layer] = []
    record["filter_evidence"] = existing
    return existing


def family_enabled(family: str, config: dict[str, Any]) -> bool:
    """Return whether a family passes global dynamic-family filtering."""

    enabled_families = config.get("enabled_families", [])
    return not enabled_families or family in enabled_families


def field_enabled(family: str, field_name: str, config: dict[str, Any]) -> bool:
    """Return whether a field passes global dynamic-field filtering."""

    enabled_fields = config.get("enabled_fields", [])
    if not enabled_fields:
        return True
    return field_name in enabled_fields or f"{family}.{field_name}" in enabled_fields


def finding_is_candidate(finding: dict[str, Any]) -> bool:
    """Return whether a dynamic finding should be treated as a candidate."""

    return bool(finding.get("candidate_flag"))


def record_fingerprint(record: dict[str, Any]) -> str:
    """Return a deterministic fingerprint for state update deduplication."""

    payload = record.get("payload")
    if not isinstance(payload, dict):
        payload = {}
    body = {
        "device_id": device_id_for_record(record),
        "family": str(record.get("family") or ""),
        "record_id": str(record.get("record_id") or ""),
        "timestamp": canonical_timestamp(record),
        "payload": payload,
    }
    encoded = json.dumps(body, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def final_quality_counts(records: list[dict[str, Any]]) -> dict[str, int]:
    """Calculate final quality-state counts from returned records."""

    decoded = sum(normalize_quality_state(record.get("quality_state")) == "decoded" for record in records)
    suspect = sum(normalize_quality_state(record.get("quality_state")) == "suspect" for record in records)
    invalid = sum(normalize_quality_state(record.get("quality_state")) == "invalid" for record in records)
    return {
        "total_decoded_records": len(records),
        "decoded_records": decoded,
        "suspect_records": suspect,
        "invalid_records": invalid,
    }


def sorted_count_dict(counts: dict[str, int]) -> dict[str, int]:
    """Return counts sorted by key for deterministic JSON output."""

    return {key: int(counts[key]) for key in sorted(counts)}
