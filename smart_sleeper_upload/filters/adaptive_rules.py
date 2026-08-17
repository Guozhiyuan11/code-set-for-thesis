"""Historical and adaptive filtering rules for decoded records."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from statistics import median
from typing import Any, Iterable

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


BaselineKey = tuple[str, str, str]


@dataclass
class HistoricalValue:
    """One historical numeric value used by adaptive rules."""

    value: float
    timestamp: datetime | None
    record_id: str


@dataclass
class StuckRun:
    """Consecutive unchanged-value run for one numeric field."""

    value: float
    count: int
    start_timestamp: datetime | None


@dataclass
class QuarantineState:
    """Recent anomalous candidates for possible baseline re-entry."""

    values: list[HistoricalValue]
    start_timestamp: datetime | None
    last_value: float
    consecutive_count: int


@dataclass
class AdaptiveEvaluation:
    """Adaptive evaluation result for one record."""

    findings: list[dict[str, Any]]
    sequence_findings: list[dict[str, Any]]
    cold_start_count: int
    skip_update_keys: set[BaselineKey]


class AdaptiveState:
    """In-memory adaptive state that can later be replaced by persistence."""

    def __init__(self) -> None:
        self.histories: dict[BaselineKey, deque[HistoricalValue]] = defaultdict(deque)
        self.previous_values: dict[BaselineKey, HistoricalValue] = {}
        self.stuck_runs: dict[BaselineKey, StuckRun] = {}
        self.last_seen_by_device_family: dict[tuple[str, str], datetime] = {}
        self.latest_record_by_device_family: dict[tuple[str, str], dict[str, Any]] = {}
        self.reported_silence_events: set[tuple[str, str, str]] = set()
        self.quarantines: dict[BaselineKey, QuarantineState] = {}
        self.baseline_versions: dict[BaselineKey, int] = {}

    def prior_values(self, key: BaselineKey, window_size: int) -> list[HistoricalValue]:
        """Retrieve prior valid values for one baseline key."""

        return list(self.histories.get(key, deque()))[-window_size:]

    def append_valid_value(
        self,
        key: BaselineKey,
        value: float,
        timestamp: datetime | None,
        record_id: str,
        *,
        window_size: int,
    ) -> None:
        """Append a new valid value and keep the rolling window bounded."""

        history = self.histories[key]
        history.append(HistoricalValue(value=value, timestamp=timestamp, record_id=record_id))
        while len(history) > window_size:
            history.popleft()
        self.previous_values[key] = HistoricalValue(value=value, timestamp=timestamp, record_id=record_id)
        self.quarantines.pop(key, None)
        self.baseline_versions.setdefault(key, 1)

    def quarantine_candidate(
        self,
        key: BaselineKey,
        value: float,
        timestamp: datetime,
        record_id: str,
        *,
        rule: dict[str, Any],
    ) -> dict[str, Any] | None:
        """Store an anomalous candidate and migrate baseline if re-entry confirms."""

        reentry = rule.get("baseline_reentry", {})
        if not reentry.get("enabled", False):
            return None

        tolerance = reentry["value_tolerance"]
        previous = self.quarantines.get(key)
        historical = HistoricalValue(value=value, timestamp=timestamp, record_id=record_id)
        if previous is None or abs(value - previous.last_value) > tolerance:
            quarantine = QuarantineState(
                values=[historical],
                start_timestamp=timestamp,
                last_value=value,
                consecutive_count=1,
            )
        else:
            quarantine = QuarantineState(
                values=previous.values + [historical],
                start_timestamp=previous.start_timestamp,
                last_value=value,
                consecutive_count=previous.consecutive_count + 1,
            )
        self.quarantines[key] = quarantine

        elapsed = _elapsed_seconds(quarantine.start_timestamp, timestamp)
        minimum_duration = reentry.get("minimum_duration_seconds")
        if quarantine.consecutive_count < reentry["consecutive_samples"]:
            return None
        if minimum_duration is not None and (elapsed is None or elapsed < minimum_duration):
            return None

        history_values = [item.value for item in self.histories.get(key, deque())]
        previous_median = float(median(history_values)) if history_values else None
        new_values = quarantine.values[-rule["window_size"] :]
        new_median = float(median([item.value for item in new_values]))
        before = self.baseline_versions.get(key, 1)
        after = before + 1
        self.histories[key] = deque(new_values)
        self.previous_values[key] = new_values[-1]
        self.baseline_versions[key] = after
        self.quarantines.pop(key, None)
        return {
            "rule_id": "adaptive.baseline_shift",
            "field": key[2],
            "previous_median": previous_median,
            "new_median": new_median,
            "quarantine_samples": len(new_values),
            "baseline_version_before": before,
            "baseline_version_after": after,
            "candidate_flag": "baseline_shift",
        }

    def retrieve_latest_record(self, device_id: str, family: str) -> dict[str, Any] | None:
        """Retrieve the latest seen record for a device/family."""

        return self.latest_record_by_device_family.get((device_id, family))

    def update_record_seen(self, record: dict[str, Any]) -> None:
        """Update sequence-level last-seen timestamps and latest records."""

        timestamp = record_timestamp(record)
        family = str(record.get("family") or "")
        if not family:
            return
        device_id = device_id_for_record(record)
        self.latest_record_by_device_family[(device_id, family)] = dict(record)
        if timestamp is not None:
            self.last_seen_by_device_family[(device_id, family)] = timestamp

    def reset_device(self, device_id: str) -> None:
        """Reset all adaptive state for one device."""

        for mapping in (
            self.histories,
            self.previous_values,
            self.stuck_runs,
            self.last_seen_by_device_family,
            self.latest_record_by_device_family,
            self.quarantines,
            self.baseline_versions,
        ):
            for key in list(mapping):
                if key[0] == device_id:
                    del mapping[key]
        self.reported_silence_events = {
            key for key in self.reported_silence_events if key[0] != device_id
        }

    def serialize(self) -> dict[str, Any]:
        """Serialize state into deterministic JSON-compatible data."""

        histories: dict[str, list[dict[str, Any]]] = {}
        for key in sorted(self.histories):
            histories[_encode_key(key)] = [
                {
                    "value": item.value,
                    "timestamp": item.timestamp.isoformat() if item.timestamp else None,
                    "record_id": item.record_id,
                }
                for item in self.histories[key]
            ]
        return {
            "histories": histories,
            "previous_values": {
                _encode_key(key): _historical_value_to_json(value)
                for key, value in sorted(self.previous_values.items())
            },
            "stuck_runs": {
                _encode_key(key): {
                    "value": value.value,
                    "count": value.count,
                    "start_timestamp": value.start_timestamp.isoformat()
                    if value.start_timestamp
                    else None,
                }
                for key, value in sorted(self.stuck_runs.items())
            },
            "last_seen_by_device_family": {
                encode_tuple_key(key): value.isoformat()
                for key, value in sorted(self.last_seen_by_device_family.items())
            },
            "latest_record_by_device_family": {
                encode_tuple_key(key): value
                for key, value in sorted(self.latest_record_by_device_family.items())
            },
            "reported_silence_events": [
                list(key) for key in sorted(self.reported_silence_events)
            ],
            "quarantines": {
                _encode_key(key): {
                    "values": [_historical_value_to_json(item) for item in value.values],
                    "start_timestamp": value.start_timestamp.isoformat()
                    if value.start_timestamp
                    else None,
                    "last_value": value.last_value,
                    "consecutive_count": value.consecutive_count,
                }
                for key, value in sorted(self.quarantines.items())
            },
            "baseline_versions": {
                _encode_key(key): int(value)
                for key, value in sorted(self.baseline_versions.items())
            },
        }

    @classmethod
    def restore(cls, payload: dict[str, Any]) -> "AdaptiveState":
        """Restore adaptive state from serialized JSON data."""

        if not isinstance(payload, dict):
            raise ValueError("adaptive state must be an object")
        state = cls()
        for raw_key, values in _object_items(payload.get("histories", {}), "adaptive.histories"):
            key = _decode_key(raw_key)
            if not isinstance(values, list):
                raise ValueError("adaptive history values must be lists")
            state.histories[key] = deque(_historical_value_from_json(item) for item in values)
        for raw_key, value in _object_items(payload.get("previous_values", {}), "adaptive.previous_values"):
            state.previous_values[_decode_key(raw_key)] = _historical_value_from_json(value)
        for raw_key, value in _object_items(payload.get("stuck_runs", {}), "adaptive.stuck_runs"):
            if not isinstance(value, dict):
                raise ValueError("adaptive stuck run values must be objects")
            state.stuck_runs[_decode_key(raw_key)] = StuckRun(
                value=float(value["value"]),
                count=int(value["count"]),
                start_timestamp=parse_record_timestamp(value.get("start_timestamp")),
            )
        for raw_key, value in _object_items(payload.get("last_seen_by_device_family", {}), "adaptive.last_seen"):
            timestamp = parse_record_timestamp(value)
            if timestamp is None:
                raise ValueError("adaptive last-seen timestamp is invalid")
            device, family = decode_tuple_key(
                raw_key, length=2, name="adaptive last-seen"
            )
            state.last_seen_by_device_family[(device, family)] = timestamp
        for raw_key, value in _object_items(
            payload.get("latest_record_by_device_family", {}),
            "adaptive.latest_record_by_device_family",
        ):
            if not isinstance(value, dict):
                raise ValueError("adaptive latest-record entries are invalid")
            device, family = decode_tuple_key(
                raw_key, length=2, name="adaptive latest-record"
            )
            state.latest_record_by_device_family[(device, family)] = dict(value)
        silence_events = payload.get("reported_silence_events", [])
        if not isinstance(silence_events, list):
            raise ValueError("adaptive.reported_silence_events must be a list")
        for item in silence_events:
            if not isinstance(item, list) or len(item) != 3:
                raise ValueError("adaptive silence event entries must have three parts")
            state.reported_silence_events.add((str(item[0]), str(item[1]), str(item[2])))
        for raw_key, value in _object_items(payload.get("quarantines", {}), "adaptive.quarantines"):
            if not isinstance(value, dict) or not isinstance(value.get("values"), list):
                raise ValueError("adaptive quarantine entries are invalid")
            state.quarantines[_decode_key(raw_key)] = QuarantineState(
                values=[_historical_value_from_json(item) for item in value["values"]],
                start_timestamp=parse_record_timestamp(value.get("start_timestamp")),
                last_value=float(value["last_value"]),
                consecutive_count=int(value["consecutive_count"]),
            )
        for raw_key, value in _object_items(payload.get("baseline_versions", {}), "adaptive.baseline_versions"):
            state.baseline_versions[_decode_key(raw_key)] = int(value)
        return state


def evaluate_adaptive_rules(
    record: dict[str, Any],
    config: dict[str, Any],
    state: AdaptiveState,
    *,
    context_block_keys: set[BaselineKey] | None = None,
) -> AdaptiveEvaluation:
    """Evaluate historical/adaptive rules against one hard-filtered record."""

    if not config.get("enabled", True):
        return AdaptiveEvaluation([], [], 0, set())

    if normalize_quality_state(record.get("quality_state")) == "invalid":
        return AdaptiveEvaluation([], [], 0, set())

    family = str(record.get("family") or "")
    if not family_enabled(family, config):
        return AdaptiveEvaluation([], [], 0, set())

    payload = record.get("payload")
    if not isinstance(payload, dict):
        return AdaptiveEvaluation([], [], 0, set())

    timestamp = record_timestamp(record)
    if timestamp is None:
        return AdaptiveEvaluation(
            [{"rule_id": "dynamic.timestamp_unavailable", "state": "not_evaluated"}],
            [],
            0,
            set(),
        )
    sequence_findings = _evaluate_sequence_findings(record, config, state)
    device_id = device_id_for_record(record)
    findings: list[dict[str, Any]] = []
    cold_start_count = 0
    skip_update_keys: set[BaselineKey] = set()
    context_block_keys = context_block_keys or set()

    for field_name, rule in _matching_field_rules(record, config):
        value = as_number(payload.get(field_name))
        if value is None:
            continue
        key = (device_id, family, field_name)
        context_blocked = key in context_block_keys
        if key in context_block_keys:
            skip_update_keys.add(key)

        baseline_finding, cold = _evaluate_rolling_baseline(
            key,
            field_name,
            value,
            rule,
            state,
        )
        if cold is not None:
            findings.append(cold)
            cold_start_count += 1
        if baseline_finding is not None:
            findings.append(baseline_finding)
            skip_update_keys.add(key)

        step_finding = _evaluate_step_change(key, field_name, value, timestamp, rule, state)
        if step_finding is not None:
            findings.append(step_finding)
            skip_update_keys.add(key)

        rate_finding = _evaluate_rate_change(key, field_name, value, timestamp, rule, state)
        if rate_finding is not None:
            findings.append(rate_finding)
            skip_update_keys.add(key)

        if not context_blocked:
            stuck_finding = _evaluate_and_update_stuck_run(key, field_name, value, timestamp, rule, state)
            if stuck_finding is not None:
                findings.append(stuck_finding)
                skip_update_keys.add(key)

        point_anomaly = any(
            finding.get("candidate_flag")
            in {"adaptive_outlier", "step_change", "rate_change", "stuck_sensor"}
            for finding in findings
            if finding.get("field") == field_name
        )
        if point_anomaly and key not in context_block_keys:
            shift = state.quarantine_candidate(
                key,
                value,
                timestamp,
                str(record.get("record_id") or ""),
                rule=rule,
            )
            if shift is not None:
                findings.append(shift)
                skip_update_keys.add(key)

    return AdaptiveEvaluation(findings, sequence_findings, cold_start_count, skip_update_keys)


def update_adaptive_state(
    record: dict[str, Any],
    config: dict[str, Any],
    state: AdaptiveState,
    *,
    skip_update_keys: set[BaselineKey],
    context_block_keys: set[BaselineKey] | None = None,
) -> None:
    """Append valid current values after evaluation has completed."""

    if not config.get("enabled", True):
        return
    if normalize_quality_state(record.get("quality_state")) == "invalid":
        return

    family = str(record.get("family") or "")
    if not family_enabled(family, config):
        return
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return

    timestamp = record_timestamp(record)
    if timestamp is None:
        return
    state.update_record_seen(record)
    device_id = device_id_for_record(record)
    record_id = str(record.get("record_id") or "")
    context_block_keys = context_block_keys or set()
    for field_name, rule in _matching_field_rules(record, config):
        value = as_number(payload.get(field_name))
        if value is None:
            continue
        key = (device_id, family, field_name)
        if key in skip_update_keys or key in context_block_keys:
            continue
        state.append_valid_value(
            key,
            value,
            timestamp,
            record_id,
            window_size=rule["window_size"],
        )


def _evaluate_rolling_baseline(
    key: BaselineKey,
    field_name: str,
    value: float,
    rule: dict[str, Any],
    state: AdaptiveState,
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    threshold = rule.get("mad_score_threshold")
    if threshold is None:
        return None, None

    history = state.prior_values(key, rule["window_size"])
    history_samples = len(history)
    if history_samples < rule["min_samples"]:
        return None, {
            "rule_id": "adaptive.cold_start",
            "field": field_name,
            "history_samples": history_samples,
            "min_samples": rule["min_samples"],
            "state": "cold_start",
        }

    values = [item.value for item in history]
    rolling_median = float(median(values))
    deviations = [abs(item - rolling_median) for item in values]
    mad = float(median(deviations))
    if rule.get("minimum_scale") is not None:
        robust_scale = max(1.4826 * mad, rule["minimum_scale"])
    else:
        robust_scale = max(mad, rule["epsilon"]) / 0.6745
    score = abs(value - rolling_median) / robust_scale
    if score <= threshold:
        return None, None

    return {
        "rule_id": "adaptive.robust_baseline",
        "field": field_name,
        "observed": value,
        "rolling_median": rolling_median,
        "mad": mad,
        "robust_scale": robust_scale,
        "score": score,
        "history_samples": history_samples,
        "baseline_version": state.baseline_versions.get(key, 1),
        "threshold": threshold,
        "candidate_flag": "adaptive_outlier",
    }, None


def _evaluate_step_change(
    key: BaselineKey,
    field_name: str,
    value: float,
    timestamp: datetime | None,
    rule: dict[str, Any],
    state: AdaptiveState,
) -> dict[str, Any] | None:
    threshold = rule.get("step_threshold")
    if threshold is None:
        return None
    previous = state.previous_values.get(key)
    if previous is None:
        return None
    elapsed = _elapsed_seconds(previous.timestamp, timestamp)
    if elapsed is None:
        return None
    if not _gap_allowed(elapsed, rule):
        return None
    step = abs(value - previous.value)
    if step <= threshold:
        return None
    return {
        "rule_id": "adaptive.step_change",
        "field": field_name,
        "observed": value,
        "previous": previous.value,
        "step": step,
        "threshold": threshold,
        "elapsed_seconds": elapsed,
        "candidate_flag": "step_change",
    }


def _evaluate_rate_change(
    key: BaselineKey,
    field_name: str,
    value: float,
    timestamp: datetime | None,
    rule: dict[str, Any],
    state: AdaptiveState,
) -> dict[str, Any] | None:
    threshold = rule.get("rate_threshold_per_second")
    if threshold is None:
        return None
    previous = state.previous_values.get(key)
    if previous is None:
        return None
    elapsed = _elapsed_seconds(previous.timestamp, timestamp)
    if elapsed is None or elapsed <= 0 or not _gap_allowed(elapsed, rule):
        return None
    rate = abs(value - previous.value) / elapsed
    if rate <= threshold:
        return None
    return {
        "rule_id": "adaptive.rate_change",
        "field": field_name,
        "observed": value,
        "previous": previous.value,
        "rate_per_second": rate,
        "threshold": threshold,
        "elapsed_seconds": elapsed,
        "candidate_flag": "rate_change",
    }


def _evaluate_and_update_stuck_run(
    key: BaselineKey,
    field_name: str,
    value: float,
    timestamp: datetime | None,
    rule: dict[str, Any],
    state: AdaptiveState,
) -> dict[str, Any] | None:
    if not rule.get("stuck_enabled", False):
        return None
    tolerance = rule.get("stuck_tolerance")
    sample_count = rule.get("stuck_sample_count")
    if tolerance is None or sample_count is None:
        return None

    previous = state.stuck_runs.get(key)
    if previous is not None and abs(value - previous.value) <= tolerance:
        run = StuckRun(
            value=previous.value,
            count=previous.count + 1,
            start_timestamp=previous.start_timestamp,
        )
    else:
        run = StuckRun(value=value, count=1, start_timestamp=timestamp)
    state.stuck_runs[key] = run

    if run.count < sample_count:
        return None
    elapsed = _elapsed_seconds(run.start_timestamp, timestamp)
    minimum_duration = rule.get("stuck_min_duration_seconds")
    if minimum_duration is not None:
        if elapsed is None or elapsed < minimum_duration:
            return None

    return {
        "rule_id": "adaptive.stuck_sensor",
        "field": field_name,
        "observed": value,
        "stuck_value": run.value,
        "consecutive_samples": run.count,
        "sample_threshold": sample_count,
        "tolerance": tolerance,
        "elapsed_seconds": elapsed,
        "duration_threshold_seconds": minimum_duration,
        "candidate_flag": "stuck_sensor",
    }


def _evaluate_sequence_findings(
    record: dict[str, Any],
    config: dict[str, Any],
    state: AdaptiveState,
) -> list[dict[str, Any]]:
    adaptive = config.get("adaptive", {})
    timestamp = record_timestamp(record)
    if timestamp is None:
        return []
    family = str(record.get("family") or "")
    if not family:
        return []
    if not family_enabled(family, config):
        return []
    device_id = device_id_for_record(record)
    findings: list[dict[str, Any]] = []

    reporting_rule = adaptive.get("expected_reporting", {}).get(family)
    previous_time = state.last_seen_by_device_family.get((device_id, family))
    if reporting_rule is not None and previous_time is not None and family_enabled(family, config):
        elapsed = (timestamp - previous_time).total_seconds()
        threshold = reporting_rule["expected_interval_seconds"] * reporting_rule["gap_multiplier"]
        if elapsed > threshold:
            findings.append(
                {
                    "rule_id": "sequence.reporting_gap",
                    "device_id": device_id,
                    "family": family,
                    "previous_time_utc": previous_time.isoformat(),
                    "current_time_utc": timestamp.isoformat(),
                    "elapsed_seconds": elapsed,
                    "threshold_seconds": threshold,
                    "candidate_flag": "reporting_gap",
                }
            )

    for silent_family, silence_rule in sorted(adaptive.get("family_silence", {}).items()):
        if not family_enabled(silent_family, config):
            continue
        if silent_family == family:
            continue
        last_seen = state.last_seen_by_device_family.get((device_id, silent_family))
        if last_seen is None:
            continue
        elapsed = (timestamp - last_seen).total_seconds()
        threshold = silence_rule["expected_interval_seconds"] * silence_rule["gap_multiplier"]
        event_key = (device_id, silent_family, last_seen.isoformat())
        if elapsed > threshold and event_key not in state.reported_silence_events:
            state.reported_silence_events.add(event_key)
            findings.append(
                {
                    "rule_id": "sequence.family_silence",
                    "device_id": device_id,
                    "family": silent_family,
                    "observed_other_family": family,
                    "last_seen_time_utc": last_seen.isoformat(),
                    "current_time_utc": timestamp.isoformat(),
                    "elapsed_seconds": elapsed,
                    "threshold_seconds": threshold,
                    "candidate_flag": "family_silence",
                }
            )

    return findings


def _matching_field_rules(record: dict[str, Any], config: dict[str, Any]) -> Iterable[tuple[str, dict[str, Any]]]:
    adaptive = config.get("adaptive", {})
    fields = adaptive.get("fields", {})
    family = str(record.get("family") or "")
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return []

    matches: list[tuple[str, dict[str, Any]]] = []
    for key, rule in fields.items():
        rule_family: str | None = None
        field_name = key
        if "." in key:
            rule_family, field_name = key.split(".", 1)
        if rule_family is not None and rule_family != family:
            continue
        if field_name not in payload:
            continue
        if not field_enabled(family, field_name, config):
            continue
        matches.append((field_name, rule))
    matches.sort(key=lambda item: item[0])
    return matches


def _elapsed_seconds(previous: datetime | None, current: datetime | None) -> float | None:
    if previous is None or current is None:
        return None
    return (current - previous).total_seconds()


def _gap_allowed(elapsed: float | None, rule: dict[str, Any]) -> bool:
    minimum = rule.get("min_time_gap_seconds")
    maximum = rule.get("max_time_gap_seconds")
    if elapsed is None:
        return minimum is None and maximum is None
    if elapsed < 0:
        return False
    if minimum is not None and elapsed < minimum:
        return False
    if maximum is not None and elapsed > maximum:
        return False
    return True


def _encode_key(key: BaselineKey) -> str:
    return encode_tuple_key(key)


def _decode_key(raw_key: str) -> BaselineKey:
    device_id, family, field = decode_tuple_key(raw_key, length=3, name="adaptive")
    return device_id, family, field


def _historical_value_to_json(value: HistoricalValue) -> dict[str, Any]:
    return {
        "value": value.value,
        "timestamp": value.timestamp.isoformat() if value.timestamp else None,
        "record_id": value.record_id,
    }


def _historical_value_from_json(value: Any) -> HistoricalValue:
    if not isinstance(value, dict):
        raise ValueError("historical value must be an object")
    timestamp = parse_record_timestamp(value.get("timestamp"))
    return HistoricalValue(
        value=float(value["value"]),
        timestamp=timestamp,
        record_id=str(value.get("record_id") or ""),
    )


def _object_items(value: Any, name: str) -> list[tuple[str, Any]]:
    if not isinstance(value, dict):
        raise ValueError(f"{name} must be an object")
    return list(value.items())
