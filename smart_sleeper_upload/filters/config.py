"""Configuration loading and validation for dynamic filtering."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any


DEFAULT_DYNAMIC_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "dynamic_filtering.json"
VALID_DYNAMIC_MODES = {"off", "shadow", "auto", "enforce"}

DEFAULT_DYNAMIC_FILTER_CONFIG: dict[str, Any] = {
    "enabled": True,
    "enabled_families": [],
    "enabled_fields": [],
    "allow_suspect_context": False,
    "allow_candidate_context": False,
    "deduplication_capacity": 10000,
    "recent_context_tolerance_seconds": 300.0,
    "peer_sensor_groups": [
        {
            "rule_id": "environment.peer_temperature_disagreement",
            "family": "environment",
            "fields": [
                "rtd1_t_x100",
                "rtd2_t_x100",
                "rtd3_t_x100",
                "rtd4_t_x100",
                "tmp102_t_x100",
                "temperature_c",
                "temp",
            ],
            "field_scales": {
                "rtd1_t_x100": 0.01,
                "rtd2_t_x100": 0.01,
                "rtd3_t_x100": 0.01,
                "rtd4_t_x100": 0.01,
                "tmp102_t_x100": 0.01,
                "temperature_c": 1.0,
                "temp": 1.0,
            },
            "aliases": [["temperature_c", "temp"]],
            "min_available": 3,
            "spread_threshold": None,
            "candidate_flag": "peer_sensor_disagreement",
        }
    ],
    "cross_field_rules": [],
    "cross_family_rules": [],
    "adaptive": {
        "default": {
            "window_size": 20,
            "min_samples": 10,
            "mad_score_threshold": None,
            "epsilon": 1e-6,
            "minimum_scale": None,
            "step_threshold": None,
            "rate_threshold_per_second": None,
            "min_time_gap_seconds": None,
            "max_time_gap_seconds": None,
            "stuck_tolerance": None,
            "stuck_sample_count": None,
            "stuck_min_duration_seconds": None,
            "stuck_enabled": False,
            "baseline_reentry": {
                "enabled": False,
                "consecutive_samples": 4,
                "value_tolerance": 0.5,
                "minimum_duration_seconds": 0.0,
                "action": "reset",
            },
        },
        "fields": {},
        "expected_reporting": {},
        "family_silence": {},
    },
    "event_confirmation": {
        "enabled": False,
        "minimum_points": 3,
        "window_points": 5,
        "maximum_gap_seconds": 300.0,
        "minimum_duration_seconds": 0.0,
        "enforcement_policy": "point",
    },
    "model_filter": {
        "enabled": True,
        "model_path": "models/autoencoder-v0",
        "minimum_valid_records": 500,
        "readiness_window": 200,
        "fallback_window": 100,
        "maximum_candidate_rate": 0.1,
        "fallback_candidate_rate": 0.2,
        "minimum_input_completeness": 0.95,
        "fallback_input_completeness": 0.9,
    },
}

TOP_LEVEL_KEYS = {
    "enabled",
    "enabled_families",
    "enabled_fields",
    "allow_suspect_context",
    "allow_candidate_context",
    "deduplication_capacity",
    "recent_context_tolerance_seconds",
    "peer_sensor_groups",
    "cross_field_rules",
    "cross_family_rules",
    "adaptive",
    "event_confirmation",
    "model_filter",
}
PEER_GROUP_KEYS = {
    "rule_id",
    "family",
    "fields",
    "field_scales",
    "aliases",
    "min_available",
    "spread_threshold",
    "candidate_flag",
}
CROSS_FIELD_RULE_KEYS = {
    "rule_id",
    "family",
    "fields",
    "field_scales",
    "max_abs_difference",
    "candidate_flag",
}
CROSS_FAMILY_RULE_KEYS = {
    "rule_id",
    "family",
    "context_family",
    "field",
    "context_field",
    "field_scales",
    "max_abs_difference",
    "tolerance_seconds",
    "candidate_flag",
}
ADAPTIVE_KEYS = {"default", "fields", "expected_reporting", "family_silence"}
FIELD_RULE_KEYS = {
    "window_size",
    "min_samples",
    "mad_score_threshold",
    "epsilon",
    "minimum_scale",
    "step_threshold",
    "rate_threshold_per_second",
    "min_time_gap_seconds",
    "max_time_gap_seconds",
    "stuck_tolerance",
    "stuck_sample_count",
    "stuck_min_duration_seconds",
    "stuck_enabled",
    "baseline_reentry",
}
SEQUENCE_RULE_KEYS = {"expected_interval_seconds", "gap_multiplier"}
BASELINE_REENTRY_KEYS = {
    "enabled",
    "consecutive_samples",
    "value_tolerance",
    "minimum_duration_seconds",
    "action",
}
EVENT_CONFIRMATION_KEYS = {
    "enabled",
    "minimum_points",
    "window_points",
    "maximum_gap_seconds",
    "minimum_duration_seconds",
    "enforcement_policy",
}
EVENT_ENFORCEMENT_POLICIES = {"point", "confirmed_event"}
MODEL_FILTER_KEYS = {"enabled", "model_path", "minimum_valid_records", "readiness_window", "fallback_window", "maximum_candidate_rate", "fallback_candidate_rate", "minimum_input_completeness", "fallback_input_completeness"}
BASELINE_REENTRY_ACTIONS = {"reset"}


def normalize_dynamic_mode(mode: str | None, *, dynamic_config_supplied: bool = False) -> str:
    """Normalize dynamic filtering mode while preserving legacy default behavior."""

    if mode is None:
        return "shadow" if dynamic_config_supplied else "off"
    normalized = str(mode).strip().lower()
    if normalized not in VALID_DYNAMIC_MODES:
        raise ValueError(
            f"Unsupported dynamic filtering mode '{mode}'. "
            f"Expected one of: {', '.join(sorted(VALID_DYNAMIC_MODES))}"
        )
    return normalized


def load_dynamic_filter_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load and validate dynamic filtering configuration."""

    defaults = _load_default_config()
    if path is None:
        return validate_dynamic_filter_config(defaults, source="defaults")

    config_path = Path(path)
    overrides = _load_json_object(config_path)
    merged = _deep_merge(defaults, overrides)
    return validate_dynamic_filter_config(merged, source=str(config_path))


def validate_dynamic_filter_config(config: dict[str, Any], *, source: str = "dynamic config") -> dict[str, Any]:
    """Validate dynamic filtering configuration and return a normalized copy."""

    if not isinstance(config, dict):
        raise ValueError(f"{source} must contain a JSON object")

    unknown = sorted(set(config) - TOP_LEVEL_KEYS)
    if unknown:
        raise ValueError(f"Unsupported dynamic config key(s) in {source}: {', '.join(unknown)}")

    normalized = copy.deepcopy(config)
    normalized["enabled"] = _require_bool(normalized.get("enabled", True), "enabled", source)
    normalized["enabled_families"] = _string_list(
        normalized.get("enabled_families", []),
        "enabled_families",
        source,
    )
    normalized["enabled_fields"] = _string_list(
        normalized.get("enabled_fields", []),
        "enabled_fields",
        source,
    )
    normalized["allow_suspect_context"] = _require_bool(
        normalized.get("allow_suspect_context", False),
        "allow_suspect_context",
        source,
    )
    normalized["allow_candidate_context"] = _require_bool(
        normalized.get("allow_candidate_context", False),
        "allow_candidate_context",
        source,
    )
    normalized["deduplication_capacity"] = _positive_int(
        normalized.get("deduplication_capacity", 10000),
        "deduplication_capacity",
        source,
    )
    normalized["recent_context_tolerance_seconds"] = _optional_non_negative_number(
        normalized.get("recent_context_tolerance_seconds"),
        "recent_context_tolerance_seconds",
        source,
    )

    normalized["peer_sensor_groups"] = _validate_peer_groups(
        normalized.get("peer_sensor_groups", []),
        source,
    )
    normalized["cross_field_rules"] = _validate_cross_field_rules(
        normalized.get("cross_field_rules", []),
        source,
    )
    normalized["cross_family_rules"] = _validate_cross_family_rules(
        normalized.get("cross_family_rules", []),
        source,
    )
    normalized["adaptive"] = _validate_adaptive(normalized.get("adaptive", {}), source)
    normalized["event_confirmation"] = _validate_event_confirmation(
        normalized.get("event_confirmation", {}),
        source,
    )
    normalized["model_filter"] = _validate_model_filter(normalized.get("model_filter", {}), source)
    return normalized


def _load_default_config() -> dict[str, Any]:
    if DEFAULT_DYNAMIC_CONFIG_PATH.is_file():
        loaded = _load_json_object(DEFAULT_DYNAMIC_CONFIG_PATH)
        return _deep_merge(DEFAULT_DYNAMIC_FILTER_CONFIG, loaded)
    return copy.deepcopy(DEFAULT_DYNAMIC_FILTER_CONFIG)


def _load_json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON config in {path}: {exc.msg}") from exc
    if not isinstance(loaded, dict):
        raise ValueError(f"Config file {path} must contain a JSON object at the top level")
    return loaded


def _deep_merge(base: dict[str, Any], overrides: dict[str, Any]) -> dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _validate_peer_groups(value: Any, source: str) -> list[dict[str, Any]]:
    groups = _object_list(value, "peer_sensor_groups", source)
    validated: list[dict[str, Any]] = []
    for index, group in enumerate(groups):
        path = f"peer_sensor_groups[{index}]"
        _reject_unknown(group, PEER_GROUP_KEYS, path, source)
        item = dict(group)
        item["rule_id"] = _optional_string(item.get("rule_id"), f"{path}.rule_id", source) or f"{path}"
        item["family"] = _require_string(item.get("family"), f"{path}.family", source)
        item["fields"] = _non_empty_string_list(item.get("fields"), f"{path}.fields", source)
        item["field_scales"] = _number_map(item.get("field_scales", {}), f"{path}.field_scales", source)
        item["aliases"] = _string_list_list(item.get("aliases", []), f"{path}.aliases", source)
        item["min_available"] = _positive_int(item.get("min_available", 2), f"{path}.min_available", source)
        item["spread_threshold"] = _optional_positive_number(
            item.get("spread_threshold"),
            f"{path}.spread_threshold",
            source,
        )
        item["candidate_flag"] = _optional_string(
            item.get("candidate_flag"),
            f"{path}.candidate_flag",
            source,
        ) or "peer_sensor_disagreement"
        validated.append(item)
    return validated


def _validate_cross_field_rules(value: Any, source: str) -> list[dict[str, Any]]:
    rules = _object_list(value, "cross_field_rules", source)
    validated: list[dict[str, Any]] = []
    for index, rule in enumerate(rules):
        path = f"cross_field_rules[{index}]"
        _reject_unknown(rule, CROSS_FIELD_RULE_KEYS, path, source)
        item = dict(rule)
        item["rule_id"] = _optional_string(item.get("rule_id"), f"{path}.rule_id", source) or f"{path}"
        item["family"] = _require_string(item.get("family"), f"{path}.family", source)
        fields = _non_empty_string_list(item.get("fields"), f"{path}.fields", source)
        if len(fields) != 2:
            raise ValueError(f"{path}.fields in {source} must contain exactly two fields")
        item["fields"] = fields
        item["field_scales"] = _number_map(item.get("field_scales", {}), f"{path}.field_scales", source)
        item["max_abs_difference"] = _optional_positive_number(
            item.get("max_abs_difference"),
            f"{path}.max_abs_difference",
            source,
        )
        item["candidate_flag"] = _optional_string(
            item.get("candidate_flag"),
            f"{path}.candidate_flag",
            source,
        ) or "cross_field_disagreement"
        validated.append(item)
    return validated


def _validate_cross_family_rules(value: Any, source: str) -> list[dict[str, Any]]:
    rules = _object_list(value, "cross_family_rules", source)
    validated: list[dict[str, Any]] = []
    for index, rule in enumerate(rules):
        path = f"cross_family_rules[{index}]"
        _reject_unknown(rule, CROSS_FAMILY_RULE_KEYS, path, source)
        item = dict(rule)
        item["rule_id"] = _optional_string(item.get("rule_id"), f"{path}.rule_id", source) or f"{path}"
        item["family"] = _require_string(item.get("family"), f"{path}.family", source)
        item["context_family"] = _require_string(
            item.get("context_family"),
            f"{path}.context_family",
            source,
        )
        item["field"] = _require_string(item.get("field"), f"{path}.field", source)
        item["context_field"] = _require_string(item.get("context_field"), f"{path}.context_field", source)
        item["field_scales"] = _number_map(item.get("field_scales", {}), f"{path}.field_scales", source)
        item["max_abs_difference"] = _optional_positive_number(
            item.get("max_abs_difference"),
            f"{path}.max_abs_difference",
            source,
        )
        item["tolerance_seconds"] = _optional_non_negative_number(
            item.get("tolerance_seconds"),
            f"{path}.tolerance_seconds",
            source,
        )
        item["candidate_flag"] = _optional_string(
            item.get("candidate_flag"),
            f"{path}.candidate_flag",
            source,
        ) or "cross_family_disagreement"
        validated.append(item)
    return validated


def _validate_adaptive(value: Any, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"adaptive in {source} must be an object")
    _reject_unknown(value, ADAPTIVE_KEYS, "adaptive", source)

    default = _validate_field_rule(value.get("default", {}), "adaptive.default", source)
    fields_raw = value.get("fields", {})
    if not isinstance(fields_raw, dict):
        raise ValueError(f"adaptive.fields in {source} must be an object")

    fields: dict[str, dict[str, Any]] = {}
    for key, rule in fields_raw.items():
        field_key = _require_string(key, "adaptive.fields key", source)
        if not isinstance(rule, dict):
            raise ValueError(f"adaptive.fields.{field_key} in {source} must be an object")
        fields[field_key] = _validate_field_rule(
            _deep_merge(default, rule),
            f"adaptive.fields.{field_key}",
            source,
        )

    return {
        "default": default,
        "fields": fields,
        "expected_reporting": _validate_sequence_rules(
            value.get("expected_reporting", {}),
            "adaptive.expected_reporting",
            source,
        ),
        "family_silence": _validate_sequence_rules(
            value.get("family_silence", {}),
            "adaptive.family_silence",
            source,
        ),
    }


def _validate_field_rule(value: Any, path: str, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} in {source} must be an object")
    _reject_unknown(value, FIELD_RULE_KEYS, path, source)
    rule = dict(value)
    rule["window_size"] = _positive_int(rule.get("window_size", 20), f"{path}.window_size", source)
    rule["min_samples"] = _non_negative_int(rule.get("min_samples", 10), f"{path}.min_samples", source)
    if rule["min_samples"] < 1:
        raise ValueError(f"{path}.min_samples in {source} must be at least 1")
    if rule["min_samples"] > rule["window_size"]:
        raise ValueError(f"{path}.min_samples in {source} must be <= {path}.window_size")
    rule["mad_score_threshold"] = _optional_positive_number(
        rule.get("mad_score_threshold"),
        f"{path}.mad_score_threshold",
        source,
    )
    rule["epsilon"] = _positive_number(rule.get("epsilon", 1e-6), f"{path}.epsilon", source)
    rule["minimum_scale"] = _optional_positive_number(
        rule.get("minimum_scale"),
        f"{path}.minimum_scale",
        source,
    )
    rule["step_threshold"] = _optional_positive_number(
        rule.get("step_threshold"),
        f"{path}.step_threshold",
        source,
    )
    rule["rate_threshold_per_second"] = _optional_positive_number(
        rule.get("rate_threshold_per_second"),
        f"{path}.rate_threshold_per_second",
        source,
    )
    rule["min_time_gap_seconds"] = _optional_non_negative_number(
        rule.get("min_time_gap_seconds"),
        f"{path}.min_time_gap_seconds",
        source,
    )
    rule["max_time_gap_seconds"] = _optional_positive_number(
        rule.get("max_time_gap_seconds"),
        f"{path}.max_time_gap_seconds",
        source,
    )
    if (
        rule["min_time_gap_seconds"] is not None
        and rule["max_time_gap_seconds"] is not None
        and rule["min_time_gap_seconds"] > rule["max_time_gap_seconds"]
    ):
        raise ValueError(
            f"{path}.min_time_gap_seconds in {source} must be <= {path}.max_time_gap_seconds"
        )
    rule["stuck_tolerance"] = _optional_non_negative_number(
        rule.get("stuck_tolerance"),
        f"{path}.stuck_tolerance",
        source,
    )
    rule["stuck_sample_count"] = _optional_positive_int(
        rule.get("stuck_sample_count"),
        f"{path}.stuck_sample_count",
        source,
    )
    rule["stuck_min_duration_seconds"] = _optional_non_negative_number(
        rule.get("stuck_min_duration_seconds"),
        f"{path}.stuck_min_duration_seconds",
        source,
    )
    rule["stuck_enabled"] = _require_bool(rule.get("stuck_enabled", False), f"{path}.stuck_enabled", source)
    rule["baseline_reentry"] = _validate_baseline_reentry(
        rule.get("baseline_reentry", {}),
        f"{path}.baseline_reentry",
        source,
    )
    return rule


def _validate_baseline_reentry(value: Any, path: str, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} in {source} must be an object")
    _reject_unknown(value, BASELINE_REENTRY_KEYS, path, source)
    action = _optional_string(value.get("action"), f"{path}.action", source) or "reset"
    if action not in BASELINE_REENTRY_ACTIONS:
        raise ValueError(f"{path}.action in {source} must be one of: reset")
    return {
        "enabled": _require_bool(value.get("enabled", False), f"{path}.enabled", source),
        "consecutive_samples": _positive_int(
            value.get("consecutive_samples", 4),
            f"{path}.consecutive_samples",
            source,
        ),
        "value_tolerance": _optional_non_negative_number(
            value.get("value_tolerance", 0.5),
            f"{path}.value_tolerance",
            source,
        ),
        "minimum_duration_seconds": _optional_non_negative_number(
            value.get("minimum_duration_seconds", 0.0),
            f"{path}.minimum_duration_seconds",
            source,
        ),
        "action": action,
    }


def _validate_event_confirmation(value: Any, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"event_confirmation in {source} must be an object")
    _reject_unknown(value, EVENT_CONFIRMATION_KEYS, "event_confirmation", source)
    policy = _optional_string(
        value.get("enforcement_policy"),
        "event_confirmation.enforcement_policy",
        source,
    ) or "point"
    if policy not in EVENT_ENFORCEMENT_POLICIES:
        raise ValueError(
            "event_confirmation.enforcement_policy in "
            f"{source} must be one of: {', '.join(sorted(EVENT_ENFORCEMENT_POLICIES))}"
        )
    minimum_points = _positive_int(
        value.get("minimum_points", 3),
        "event_confirmation.minimum_points",
        source,
    )
    window_points = _positive_int(
        value.get("window_points", 5),
        "event_confirmation.window_points",
        source,
    )
    if minimum_points > window_points:
        raise ValueError(
            "event_confirmation.minimum_points in "
            f"{source} must be <= event_confirmation.window_points"
        )
    return {
        "enabled": _require_bool(value.get("enabled", False), "event_confirmation.enabled", source),
        "minimum_points": minimum_points,
        "window_points": window_points,
        "maximum_gap_seconds": _optional_positive_number(
            value.get("maximum_gap_seconds", 300.0),
            "event_confirmation.maximum_gap_seconds",
            source,
        ),
        "minimum_duration_seconds": _optional_non_negative_number(
            value.get("minimum_duration_seconds", 0.0),
            "event_confirmation.minimum_duration_seconds",
            source,
        ),
        "enforcement_policy": policy,
    }


def _validate_model_filter(value: Any, source: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"model_filter in {source} must be an object")
    _reject_unknown(value, MODEL_FILTER_KEYS, "model_filter", source)
    result = {
        "enabled": _require_bool(value.get("enabled", True), "model_filter.enabled", source),
        "model_path": _require_string(value.get("model_path", "models/autoencoder-v0"), "model_filter.model_path", source),
        "minimum_valid_records": _positive_int(value.get("minimum_valid_records", 500), "model_filter.minimum_valid_records", source),
        "readiness_window": _positive_int(value.get("readiness_window", 200), "model_filter.readiness_window", source),
        "fallback_window": _positive_int(value.get("fallback_window", 100), "model_filter.fallback_window", source),
    }
    for key, default in (("maximum_candidate_rate", 0.1), ("fallback_candidate_rate", 0.2), ("minimum_input_completeness", 0.95), ("fallback_input_completeness", 0.9)):
        number = _positive_number(value.get(key, default), f"model_filter.{key}", source)
        if number > 1:
            raise ValueError(f"model_filter.{key} in {source} must be <= 1")
        result[key] = number
    return result


def _validate_sequence_rules(value: Any, path: str, source: str) -> dict[str, dict[str, float]]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} in {source} must be an object")

    rules: dict[str, dict[str, float]] = {}
    for family, rule in value.items():
        family_key = _require_string(family, f"{path} key", source)
        if not isinstance(rule, dict):
            raise ValueError(f"{path}.{family_key} in {source} must be an object")
        _reject_unknown(rule, SEQUENCE_RULE_KEYS, f"{path}.{family_key}", source)
        expected = _optional_positive_number(
            rule.get("expected_interval_seconds"),
            f"{path}.{family_key}.expected_interval_seconds",
            source,
        )
        multiplier = _optional_positive_number(
            rule.get("gap_multiplier"),
            f"{path}.{family_key}.gap_multiplier",
            source,
        )
        if expected is None or multiplier is None:
            continue
        rules[family_key] = {
            "expected_interval_seconds": expected,
            "gap_multiplier": multiplier,
        }
    return rules


def _reject_unknown(value: dict[str, Any], allowed: set[str], path: str, source: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"Unsupported dynamic config key(s) in {source} for {path}: {', '.join(unknown)}")


def _require_bool(value: Any, path: str, source: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} in {source} must be a boolean")
    return value


def _require_string(value: Any, path: str, source: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} in {source} must be a non-empty string")
    return value.strip()


def _optional_string(value: Any, path: str, source: str) -> str | None:
    if value is None:
        return None
    return _require_string(value, path, source)


def _string_list(value: Any, path: str, source: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{path} in {source} must be a list")
    return [_require_string(item, f"{path}[]", source) for item in value]


def _non_empty_string_list(value: Any, path: str, source: str) -> list[str]:
    items = _string_list(value, path, source)
    if not items:
        raise ValueError(f"{path} in {source} must contain at least one item")
    return items


def _string_list_list(value: Any, path: str, source: str) -> list[list[str]]:
    if not isinstance(value, list):
        raise ValueError(f"{path} in {source} must be a list")
    return [_non_empty_string_list(item, f"{path}[]", source) for item in value]


def _object_list(value: Any, path: str, source: str) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        raise ValueError(f"{path} in {source} must be a list")
    objects: list[dict[str, Any]] = []
    for index, item in enumerate(value):
        if not isinstance(item, dict):
            raise ValueError(f"{path}[{index}] in {source} must be an object")
        objects.append(item)
    return objects


def _number_map(value: Any, path: str, source: str) -> dict[str, float]:
    if not isinstance(value, dict):
        raise ValueError(f"{path} in {source} must be an object")
    return {
        _require_string(key, f"{path} key", source): _number(number, f"{path}.{key}", source)
        for key, number in value.items()
    }


def _number(value: Any, path: str, source: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{path} in {source} must be numeric")
    return float(value)


def _positive_number(value: Any, path: str, source: str) -> float:
    number = _number(value, path, source)
    if number <= 0:
        raise ValueError(f"{path} in {source} must be greater than zero")
    return number


def _optional_positive_number(value: Any, path: str, source: str) -> float | None:
    if value is None:
        return None
    return _positive_number(value, path, source)


def _optional_non_negative_number(value: Any, path: str, source: str) -> float | None:
    if value is None:
        return None
    number = _number(value, path, source)
    if number < 0:
        raise ValueError(f"{path} in {source} must be non-negative")
    return number


def _positive_int(value: Any, path: str, source: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} in {source} must be an integer")
    if value <= 0:
        raise ValueError(f"{path} in {source} must be greater than zero")
    return int(value)


def _optional_positive_int(value: Any, path: str, source: str) -> int | None:
    if value is None:
        return None
    return _positive_int(value, path, source)


def _non_negative_int(value: Any, path: str, source: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} in {source} must be an integer")
    if value < 0:
        raise ValueError(f"{path} in {source} must be non-negative")
    return int(value)
