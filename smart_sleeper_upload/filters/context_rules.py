"""Context-aware filtering rules for decoded records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Any

from .models import (
    as_number,
    decode_tuple_key,
    device_id_for_record,
    encode_tuple_key,
    family_enabled,
    field_enabled,
    normalize_quality_state,
    parse_record_timestamp,
    record_timestamp,
)


@dataclass
class LatestRecord:
    """Latest valid record context for one device/family pair."""

    record: dict[str, Any]
    timestamp: datetime | None


class ContextState:
    """Small in-memory context store for recent valid records."""

    def __init__(self) -> None:
        self._latest_by_device_family: dict[tuple[str, str], LatestRecord] = {}

    def update_latest_valid(self, record: dict[str, Any], *, allow_suspect_context: bool = False) -> None:
        """Store the latest non-invalid record for cross-family lookup."""

        state = normalize_quality_state(record.get("quality_state"))
        if state == "invalid":
            return
        if state == "suspect" and not allow_suspect_context:
            return
        family = str(record.get("family") or "")
        if not family:
            return
        device_id = device_id_for_record(record)
        timestamp = record_timestamp(record)
        if timestamp is None:
            return
        key = (device_id, family)
        current = self._latest_by_device_family.get(key)
        if current is not None and current.timestamp is not None and timestamp is not None:
            if timestamp < current.timestamp:
                return
        self._latest_by_device_family[key] = LatestRecord(record=dict(record), timestamp=timestamp)

    def copy(self) -> "ContextState":
        """Return a shallow copy suitable for same-timestamp snapshots."""

        copied = ContextState()
        copied._latest_by_device_family = dict(self._latest_by_device_family)
        return copied

    def serialize(self) -> dict[str, Any]:
        """Serialize trusted latest context records."""

        records: dict[str, dict[str, Any]] = {}
        for key in sorted(self._latest_by_device_family):
            latest = self._latest_by_device_family[key]
            records[encode_tuple_key(key)] = {
                "timestamp": latest.timestamp.isoformat() if latest.timestamp else None,
                "record": latest.record,
            }
        return {"latest_by_device_family": records}

    @classmethod
    def restore(cls, payload: dict[str, Any]) -> "ContextState":
        """Restore context state from serialized data."""

        state = cls()
        latest_raw = payload.get("latest_by_device_family", {})
        if not isinstance(latest_raw, dict):
            raise ValueError("context.latest_by_device_family must be an object")
        for raw_key, value in latest_raw.items():
            if not isinstance(value, dict) or not isinstance(value.get("record"), dict):
                raise ValueError("context latest value must contain a record object")
            timestamp = parse_record_timestamp(value.get("timestamp"))
            device_id, family = decode_tuple_key(
                raw_key, length=2, name="context latest"
            )
            state._latest_by_device_family[(device_id, family)] = LatestRecord(
                record=dict(value["record"]),
                timestamp=timestamp,
            )
        return state

    def latest_valid_record(
        self,
        *,
        device_id: str,
        family: str,
        before: datetime | None,
        tolerance_seconds: float | None,
    ) -> LatestRecord | None:
        """Return the latest valid record for a device/family within tolerance."""

        latest = self._latest_by_device_family.get((device_id, family))
        if latest is None:
            return None
        if before is None or latest.timestamp is None:
            return None
        elapsed = (before - latest.timestamp).total_seconds()
        if elapsed < 0:
            return None
        if tolerance_seconds is not None and elapsed > tolerance_seconds:
            return None
        return latest

    def reset_device(self, device_id: str) -> None:
        """Remove all latest-record context for one device."""

        for key in list(self._latest_by_device_family):
            if key[0] == device_id:
                del self._latest_by_device_family[key]


def evaluate_context_rules(
    record: dict[str, Any],
    config: dict[str, Any],
    state: ContextState,
) -> list[dict[str, Any]]:
    """Evaluate configured context rules against one hard-filtered record."""

    if not config.get("enabled", True):
        return []
    if normalize_quality_state(record.get("quality_state")) == "invalid":
        return []

    family = str(record.get("family") or "")
    if not family_enabled(family, config):
        return []

    findings: list[dict[str, Any]] = []
    findings.extend(_evaluate_peer_sensor_groups(record, config))
    findings.extend(_evaluate_cross_field_rules(record, config))
    findings.extend(_evaluate_cross_family_rules(record, config, state))
    return findings


def _evaluate_peer_sensor_groups(record: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []

    family = str(record.get("family") or "")
    findings: list[dict[str, Any]] = []
    for group in config.get("peer_sensor_groups", []):
        if group["family"] != family:
            continue
        threshold = group.get("spread_threshold")
        if threshold is None:
            continue

        values = _collect_peer_values(
            payload,
            family,
            group["fields"],
            group.get("field_scales", {}),
            group.get("aliases", []),
            config,
        )
        if len(values) < group["min_available"]:
            continue

        numeric_values = [item["value"] for item in values]
        center = float(median(numeric_values))
        spread = max(abs(value - center) for value in numeric_values)
        if spread <= threshold:
            continue

        findings.append(
            {
                "rule_id": group["rule_id"],
                "fields": [item["field"] for item in values],
                "affected_fields": [item["field"] for item in values],
                "observed_values": {item["field"]: item["value"] for item in values},
                "median": center,
                "spread": spread,
                "threshold": threshold,
                "candidate_flag": group["candidate_flag"],
            }
        )
    return findings


def _evaluate_cross_field_rules(record: dict[str, Any], config: dict[str, Any]) -> list[dict[str, Any]]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []

    family = str(record.get("family") or "")
    findings: list[dict[str, Any]] = []
    for rule in config.get("cross_field_rules", []):
        if rule["family"] != family:
            continue
        threshold = rule.get("max_abs_difference")
        if threshold is None:
            continue

        left_field, right_field = rule["fields"]
        if not field_enabled(family, left_field, config) or not field_enabled(family, right_field, config):
            continue
        left = _scaled_payload_number(payload, left_field, rule.get("field_scales", {}))
        right = _scaled_payload_number(payload, right_field, rule.get("field_scales", {}))
        if left is None or right is None:
            continue

        difference = abs(left - right)
        if difference <= threshold:
            continue
        findings.append(
            {
                "rule_id": rule["rule_id"],
                "fields": [left_field, right_field],
                "affected_fields": [left_field, right_field],
                "observed_values": {left_field: left, right_field: right},
                "difference": difference,
                "threshold": threshold,
                "candidate_flag": rule["candidate_flag"],
            }
        )
    return findings


def _evaluate_cross_family_rules(
    record: dict[str, Any],
    config: dict[str, Any],
    state: ContextState,
) -> list[dict[str, Any]]:
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []

    family = str(record.get("family") or "")
    timestamp = record_timestamp(record)
    device_id = device_id_for_record(record)
    default_tolerance = config.get("recent_context_tolerance_seconds")
    findings: list[dict[str, Any]] = []

    for rule in config.get("cross_family_rules", []):
        if rule["family"] != family:
            continue
        if not field_enabled(family, rule["field"], config):
            continue
        threshold = rule.get("max_abs_difference")
        if threshold is None:
            continue
        tolerance = rule.get("tolerance_seconds")
        if tolerance is None:
            tolerance = default_tolerance

        latest = state.latest_valid_record(
            device_id=device_id,
            family=rule["context_family"],
            before=timestamp,
            tolerance_seconds=tolerance,
        )
        if latest is None:
            continue
        context_payload = latest.record.get("payload")
        if not isinstance(context_payload, dict):
            continue

        current = _scaled_payload_number(payload, rule["field"], rule.get("field_scales", {}))
        context = _scaled_payload_number(
            context_payload,
            rule["context_field"],
            rule.get("field_scales", {}),
        )
        if current is None or context is None:
            continue

        difference = abs(current - context)
        if difference <= threshold:
            continue
        findings.append(
            {
                "rule_id": rule["rule_id"],
                "field": rule["field"],
                "affected_fields": [rule["field"]],
                "context_family": rule["context_family"],
                "context_field": rule["context_field"],
                "observed": current,
                "context_observed": context,
                "difference": difference,
                "threshold": threshold,
                "tolerance_seconds": tolerance,
                "candidate_flag": rule["candidate_flag"],
            }
        )
    return findings


def _collect_peer_values(
    payload: dict[str, Any],
    family: str,
    fields: list[str],
    scales: dict[str, float],
    aliases: list[list[str]],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    alias_by_field: dict[str, list[str]] = {}
    for alias_group in aliases:
        for field in alias_group:
            alias_by_field[field] = alias_group

    values: list[dict[str, Any]] = []
    consumed: set[str] = set()
    for field in fields:
        if field in consumed:
            continue

        alias_group = alias_by_field.get(field)
        if alias_group:
            candidates = [
                alias
                for alias in alias_group
                if alias in fields and alias not in consumed and field_enabled(family, alias, config)
            ]
            consumed.update(alias_group)
            present = [
                (alias, _scaled_payload_number(payload, alias, scales))
                for alias in candidates
            ]
            present = [(alias, value) for alias, value in present if value is not None]
            if present:
                alias, value = present[0]
                values.append({"field": alias, "value": value})
            continue

        consumed.add(field)
        if not field_enabled(family, field, config):
            continue
        value = _scaled_payload_number(payload, field, scales)
        if value is not None:
            values.append({"field": field, "value": value})
    return values


def _scaled_payload_number(
    payload: dict[str, Any],
    field: str,
    scales: dict[str, float],
) -> float | None:
    value = as_number(payload.get(field))
    if value is None:
        return None
    return value * float(scales.get(field, 1.0))
