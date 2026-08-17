"""Point-to-event aggregation for dynamic data-quality candidates."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .models import (
    decode_tuple_key,
    device_id_for_record,
    encode_tuple_key,
    parse_record_timestamp,
    record_timestamp,
)


EventKey = tuple[str, str, str, str]


@dataclass
class ActiveEvent:
    """One continuous anomaly-event lifecycle."""

    event_id: str
    device_id: str
    family: str
    field: str
    candidate_flag: str
    start_timestamp: datetime
    last_timestamp: datetime
    point_count: int
    contributing_record_ids: list[str]
    confirmed: bool = False
    closed: bool = False


class EventState:
    """In-memory event aggregation state."""

    def __init__(self) -> None:
        self.active_events: dict[EventKey, ActiveEvent] = {}
        self.closed_events: dict[str, dict[str, Any]] = {}

    def add_candidates(
        self,
        record: dict[str, Any],
        findings: list[dict[str, Any]],
        config: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], set[str]]:
        """Aggregate point candidates and return event summaries plus enforced flags."""

        event_config = config.get("event_confirmation", {})
        policy = event_config.get("enforcement_policy", "point")
        if not event_config.get("enabled", False):
            return [], {
                str(finding["candidate_flag"])
                for finding in findings
                if finding.get("candidate_flag") and policy == "point"
            }

        timestamp = record_timestamp(record)
        if timestamp is None:
            return [], set()
        device_id = device_id_for_record(record)
        family = str(record.get("family") or "")
        record_id = str(record.get("record_id") or "")
        summaries: list[dict[str, Any]] = []
        enforce_flags: set[str] = set()

        for finding in findings:
            candidate_flag = finding.get("candidate_flag")
            if not candidate_flag:
                continue
            for field in _finding_fields(finding):
                key = (device_id, family, field, str(candidate_flag))
                active = self.active_events.get(key)
                if active is not None and _gap_exceeded(active.last_timestamp, timestamp, event_config):
                    summaries.append(_event_summary(active, status="closed"))
                    self.closed_events[active.event_id] = _event_summary(active, status="closed")
                    del self.active_events[key]
                    active = None

                if active is None:
                    active = _new_event(key, timestamp, record_id)
                    self.active_events[key] = active
                else:
                    active.last_timestamp = timestamp
                    active.point_count += 1
                    active.contributing_record_ids.append(record_id)
                    active.contributing_record_ids = active.contributing_record_ids[
                        -int(event_config["window_points"]) :
                    ]

                if _event_confirmed(active, event_config):
                    finding["event_id"] = active.event_id
                    if not active.confirmed:
                        active.confirmed = True
                        summaries.append(_event_summary(active, status="confirmed"))
                    elif policy == "confirmed_event":
                        summaries.append(_event_summary(active, status="updated"))
                    if policy == "confirmed_event":
                        enforce_flags.add(str(candidate_flag))
                elif policy == "point":
                    enforce_flags.add(str(candidate_flag))

        return summaries, enforce_flags

    def serialize(self) -> dict[str, Any]:
        """Serialize event aggregation state."""

        return {
            "active_events": {
                _encode_key(key): _active_event_to_json(event)
                for key, event in sorted(self.active_events.items())
            },
            "closed_events": {
                key: self.closed_events[key]
                for key in sorted(self.closed_events)
            },
        }

    @classmethod
    def restore(cls, payload: dict[str, Any]) -> "EventState":
        """Restore event state from serialized data."""

        if not isinstance(payload, dict):
            raise ValueError("event state must be an object")
        state = cls()
        active_events = payload.get("active_events")
        if isinstance(active_events, dict):
            for raw_key, raw_event in active_events.items():
                if not isinstance(raw_event, dict):
                    raise ValueError("event.active_events values must be objects")
                state.active_events[_decode_key(raw_key)] = _active_event_from_json(raw_event)
        else:
            state._restore_v1_active_points(payload)

        closed = payload.get("closed_events", payload.get("confirmed_events", {}))
        if not isinstance(closed, dict):
            raise ValueError("event.closed_events must be an object")
        state.closed_events = {
            str(key): dict(value)
            for key, value in closed.items()
            if isinstance(value, dict)
        }
        return state

    def reset_device(self, device_id: str) -> None:
        """Reset event state for one device."""

        for key in list(self.active_events):
            if key[0] == device_id:
                del self.active_events[key]
        self.closed_events = {
            event_id: event
            for event_id, event in self.closed_events.items()
            if event.get("device_id") != device_id
        }

    def _restore_v1_active_points(self, payload: dict[str, Any]) -> None:
        active = payload.get("active_points", {})
        if not isinstance(active, dict):
            raise ValueError("event.active_points must be an object")
        for raw_key, raw_points in active.items():
            if not isinstance(raw_points, list) or not raw_points:
                continue
            key = _decode_key(raw_key)
            points = [
                {
                    "record_id": str(point.get("record_id") or ""),
                    "timestamp": _require_timestamp(point.get("timestamp")),
                }
                for point in raw_points
                if isinstance(point, dict)
            ]
            if not points:
                continue
            points.sort(key=lambda item: item["timestamp"])
            event = _new_event(key, points[0]["timestamp"], points[0]["record_id"])
            for point in points[1:]:
                event.last_timestamp = point["timestamp"]
                event.point_count += 1
                event.contributing_record_ids.append(point["record_id"])
            self.active_events[key] = event


def event_enforcement_policy(config: dict[str, Any]) -> str:
    """Return the configured event enforcement policy."""

    return config.get("event_confirmation", {}).get("enforcement_policy", "point")


def _finding_fields(finding: dict[str, Any]) -> list[str]:
    affected = finding.get("affected_fields")
    if isinstance(affected, list) and affected:
        return sorted({str(field) for field in affected if str(field)})
    field = finding.get("field")
    if field is None:
        return ["record"]
    return [str(field)]


def _new_event(key: EventKey, timestamp: datetime, record_id: str) -> ActiveEvent:
    device_id, family, field, candidate_flag = key
    body = "|".join([device_id, family, field, candidate_flag, timestamp.isoformat()])
    event_id = "event:" + hashlib.sha256(body.encode("utf-8")).hexdigest()[:16]
    return ActiveEvent(
        event_id=event_id,
        device_id=device_id,
        family=family,
        field=field,
        candidate_flag=candidate_flag,
        start_timestamp=timestamp,
        last_timestamp=timestamp,
        point_count=1,
        contributing_record_ids=[record_id],
    )


def _gap_exceeded(previous_time: datetime, current_time: datetime, config: dict[str, Any]) -> bool:
    maximum_gap = config.get("maximum_gap_seconds")
    if maximum_gap is None:
        return False
    return (current_time - previous_time).total_seconds() > maximum_gap


def _event_confirmed(event: ActiveEvent, config: dict[str, Any]) -> bool:
    if event.point_count < config["minimum_points"]:
        return False
    minimum_duration = config.get("minimum_duration_seconds")
    if minimum_duration is None:
        return True
    elapsed = (event.last_timestamp - event.start_timestamp).total_seconds()
    return elapsed >= minimum_duration


def _event_summary(event: ActiveEvent, *, status: str) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "device_id": event.device_id,
        "family": event.family,
        "field": event.field,
        "candidate_flag": event.candidate_flag,
        "start_time_utc": event.start_timestamp.isoformat(),
        "end_time_utc": event.last_timestamp.isoformat(),
        "point_count": event.point_count,
        "contributing_record_ids": list(event.contributing_record_ids),
        "status": status,
    }


def _active_event_to_json(event: ActiveEvent) -> dict[str, Any]:
    return {
        "event_id": event.event_id,
        "device_id": event.device_id,
        "family": event.family,
        "field": event.field,
        "candidate_flag": event.candidate_flag,
        "start_time_utc": event.start_timestamp.isoformat(),
        "end_time_utc": event.last_timestamp.isoformat(),
        "point_count": event.point_count,
        "contributing_record_ids": list(event.contributing_record_ids),
        "confirmed": event.confirmed,
        "closed": event.closed,
    }


def _active_event_from_json(value: dict[str, Any]) -> ActiveEvent:
    return ActiveEvent(
        event_id=str(value["event_id"]),
        device_id=str(value["device_id"]),
        family=str(value["family"]),
        field=str(value["field"]),
        candidate_flag=str(value["candidate_flag"]),
        start_timestamp=_require_timestamp(value.get("start_time_utc")),
        last_timestamp=_require_timestamp(value.get("end_time_utc")),
        point_count=int(value.get("point_count", 0)),
        contributing_record_ids=[
            str(item) for item in value.get("contributing_record_ids", []) if str(item)
        ],
        confirmed=bool(value.get("confirmed", False)),
        closed=bool(value.get("closed", False)),
    )


def _encode_key(key: EventKey) -> str:
    return encode_tuple_key(key)


def _decode_key(raw_key: str) -> EventKey:
    device_id, family, field, candidate = decode_tuple_key(raw_key, length=4, name="event")
    return device_id, family, field, candidate


def _require_timestamp(value: Any) -> datetime:
    parsed = parse_record_timestamp(value)
    if parsed is None:
        raise ValueError("event timestamp is invalid")
    return parsed
