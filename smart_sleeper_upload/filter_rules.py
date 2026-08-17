"""SMART Sleeper record filtering for normalized JSON records."""

from __future__ import annotations

import argparse
import json
import logging
import math
from pathlib import Path
from typing import Any, TypedDict

from filters.config import load_dynamic_filter_config, validate_dynamic_filter_config
from filters.engine import run_filter_engine, run_filter_engine_staged
from filters.models import (
    dedupe_sorted_labels as _dedupe_sorted_labels,
    merge_quality_state as _merge_states,
    normalize_quality_state as _normalize_quality_state,
)
from filters.state import DynamicState


LOGGER = logging.getLogger(__name__)

DECODED_BASE_FIELDS = [
    "record_id",
    "schema_version",
    "sleeper_id",
    "batch_id",
    "function_id",
    "family",
    "source_time_utc",
    "ingest_time_utc",
    "quality_state",
    "quality_flags",
    "analysis_tags",
    "payload",
]
DECODED_LIST_FIELDS = {"quality_flags", "analysis_tags"}
PRESERVED_RECORD_DICT_FIELDS = ("platform_meta", "raw_unmapped")

INVALID_VALUE_FLAG = "invalid_value"
RECORD_ANALYSIS_TAGS = {
    "noise_like",
    "flatwheel_candidate",
    "squeal_candidate",
    "event_candidate",
}
FAMILY_FUNCTION_IDS = {
    "environment": {"0x41"},
    "microphone_fft": {"0x47", "0x48", "0x49", "0x4A", "0x4B"},
    "ae": {"0x4C", "0x4D", "0x4E", "0x4F"},
}
DEFAULT_FILTER_CONFIG_PATH = Path(__file__).resolve().parent / "config" / "filter_thresholds.json"
FILTER_THRESHOLD_SCHEMA: dict[str, tuple[str, ...]] = {
    "environment": ("temp_abs_limit_x100",),
    "microphone_fft": (
        "low_power_threshold",
        "low_band_power_threshold",
        "flatwheel_band_threshold",
        "squeal_peak_freq_threshold",
        "squeal_centroid_threshold",
        "high_band_threshold",
    ),
    "ae": (
        "low_energy_threshold",
        "event_energy_threshold",
        "event_amplitude_margin",
    ),
}


class EnvironmentThresholdConfig(TypedDict):
    """Thresholds for environment record filtering."""

    temp_abs_limit_x100: float


class MicrophoneThresholdConfig(TypedDict):
    """Thresholds for microphone FFT record filtering."""

    low_power_threshold: float
    low_band_power_threshold: float
    flatwheel_band_threshold: float
    squeal_peak_freq_threshold: float
    squeal_centroid_threshold: float
    high_band_threshold: float


class AeThresholdConfig(TypedDict):
    """Thresholds for AE record filtering."""

    low_energy_threshold: float
    event_energy_threshold: float
    event_amplitude_margin: float


class FilterThresholdConfig(TypedDict):
    """Top-level filter threshold configuration."""

    environment: EnvironmentThresholdConfig
    microphone_fft: MicrophoneThresholdConfig
    ae: AeThresholdConfig


def _normalize_label_list(value: Any, *, label_type: str) -> list[str]:
    """Normalize list-like quality or analysis labels into sorted unique strings."""

    if value is None:
        return []
    if not isinstance(value, list):
        LOGGER.warning("Replacing non-list %s with an empty list", label_type)
        return []
    return _dedupe_sorted_labels(value)


def _normalize_object_field(value: Any, *, field_name: str) -> dict[str, Any]:
    """Normalize preserved object fields into mutable dicts."""

    if value is None:
        return {}
    if not isinstance(value, dict):
        LOGGER.warning("Replacing non-object %s with an empty object", field_name)
        return {}
    return dict(value)


def _normalize_record_semantics(record: dict[str, Any]) -> dict[str, Any]:
    """Ensure record-layer quality fields and analysis tags follow the current schema."""

    quality_flags = _normalize_label_list(record.get("quality_flags"), label_type="quality_flags")
    analysis_tags = _normalize_label_list(record.get("analysis_tags"), label_type="analysis_tags")

    migrated_tags = [flag for flag in quality_flags if flag in RECORD_ANALYSIS_TAGS]
    if migrated_tags:
        quality_flags = [flag for flag in quality_flags if flag not in RECORD_ANALYSIS_TAGS]
        analysis_tags = _dedupe_sorted_labels(analysis_tags + migrated_tags)
        if not quality_flags and _normalize_quality_state(record.get("quality_state")) == "suspect":
            record["quality_state"] = "decoded"

    record["quality_flags"] = quality_flags
    record["analysis_tags"] = analysis_tags
    for field_name in PRESERVED_RECORD_DICT_FIELDS:
        record[field_name] = _normalize_object_field(
            record.get(field_name),
            field_name=field_name,
        )
    return record


def _load_json_object(path: Path) -> dict[str, Any]:
    """Load a JSON file and ensure its top level is an object."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            loaded = json.load(handle)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON config in {path}: {exc.msg}") from exc

    if not isinstance(loaded, dict):
        raise ValueError(f"Config file {path} must contain a JSON object at the top level")
    return loaded


def _validate_config_number(section: str, key: str, value: Any, source: str) -> float:
    """Validate one numeric config value."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(
            f"Config field '{section}.{key}' in {source} must be numeric, "
            f"got {type(value).__name__}"
        )
    return float(value)


def _validate_filter_threshold_config(config: dict[str, Any], *, source: str) -> FilterThresholdConfig:
    """Validate and normalize the full threshold config."""

    unknown_sections = sorted(set(config) - set(FILTER_THRESHOLD_SCHEMA))
    if unknown_sections:
        raise ValueError(
            f"Unsupported config section(s) in {source}: {', '.join(unknown_sections)}"
        )

    validated: dict[str, dict[str, float]] = {}
    for section_name, keys in FILTER_THRESHOLD_SCHEMA.items():
        section_value = config.get(section_name)
        if not isinstance(section_value, dict):
            raise ValueError(f"Config section '{section_name}' in {source} must be an object")

        unknown_keys = sorted(set(section_value) - set(keys))
        if unknown_keys:
            raise ValueError(
                f"Unsupported config key(s) in {source} for section '{section_name}': "
                f"{', '.join(unknown_keys)}"
            )

        validated_section: dict[str, float] = {}
        for key in keys:
            if key not in section_value:
                raise ValueError(f"Missing config field '{section_name}.{key}' in {source}")
            validated_section[key] = _validate_config_number(
                section_name,
                key,
                section_value[key],
                source,
            )
        validated[section_name] = validated_section

    return {
        "environment": EnvironmentThresholdConfig(**validated["environment"]),
        "microphone_fft": MicrophoneThresholdConfig(**validated["microphone_fft"]),
        "ae": AeThresholdConfig(**validated["ae"]),
    }


def _merge_filter_threshold_overrides(
    defaults: FilterThresholdConfig,
    overrides: dict[str, Any],
    *,
    source: str,
) -> FilterThresholdConfig:
    """Merge a partial override config onto validated defaults."""

    unknown_sections = sorted(set(overrides) - set(FILTER_THRESHOLD_SCHEMA))
    if unknown_sections:
        raise ValueError(
            f"Unsupported config section(s) in {source}: {', '.join(unknown_sections)}"
        )

    merged: dict[str, dict[str, Any]] = {
        section_name: dict(defaults[section_name])
        for section_name in FILTER_THRESHOLD_SCHEMA
    }
    for section_name, section_override in overrides.items():
        if not isinstance(section_override, dict):
            raise ValueError(f"Config section '{section_name}' in {source} must be an object")

        allowed_keys = FILTER_THRESHOLD_SCHEMA[section_name]
        unknown_keys = sorted(set(section_override) - set(allowed_keys))
        if unknown_keys:
            raise ValueError(
                f"Unsupported config key(s) in {source} for section '{section_name}': "
                f"{', '.join(unknown_keys)}"
            )

        merged[section_name].update(section_override)

    return _validate_filter_threshold_config(merged, source=source)


def load_filter_threshold_config(path: str | Path | None = None) -> FilterThresholdConfig:
    """Load filter thresholds from JSON, filling omitted overrides from defaults."""

    default_source = str(DEFAULT_FILTER_CONFIG_PATH)
    default_config = _validate_filter_threshold_config(
        _load_json_object(DEFAULT_FILTER_CONFIG_PATH),
        source=default_source,
    )
    if path is None:
        return default_config

    config_path = Path(path)
    overrides = _load_json_object(config_path)
    return _merge_filter_threshold_overrides(default_config, overrides, source=str(config_path))


def _normalize_function_id(value: Any) -> str | None:
    """Normalize function IDs like 0x41 or 41 into uppercase hex form."""

    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.lower().startswith("0x"):
            parsed = int(text, 16)
        else:
            parsed = int(float(text))
    except (TypeError, ValueError):
        return text.upper()
    return f"0x{parsed:02X}"


def _coerce_payload(value: Any) -> dict[str, Any] | None:
    """Return payload as a mutable dict if possible."""

    if isinstance(value, dict):
        return dict(value)
    return None


def _get_number(payload: dict[str, Any], key: str) -> float | None:
    """Read a numeric field from payload."""

    value = payload.get(key)
    if value is None or isinstance(value, bool):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _get_bool(payload: dict[str, Any], key: str) -> bool | None:
    """Read a boolean field from payload."""

    value = payload.get(key)
    if isinstance(value, bool):
        return value
    return None


def _has_present_value(value: Any) -> bool:
    """Return True when a payload value is meaningfully present."""

    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _has_payload_field(payload: dict[str, Any], key: str) -> bool:
    """Return True when a payload field is present and non-empty."""

    return _has_present_value(payload.get(key))


def _has_non_empty_string(value: Any) -> bool:
    """Return True when a value is a non-empty string."""

    return isinstance(value, str) and bool(value.strip())


def _gps_fix_is_unreliable(value: Any) -> bool:
    """Return True when a GPS fix field indicates no fix or an unreliable fix."""

    if value is None:
        return False
    if isinstance(value, bool):
        return not value
    if isinstance(value, (int, float)):
        return float(value) <= 0

    normalized = str(value).strip().lower().replace("-", " ").replace("_", " ")
    if not normalized:
        return False

    if normalized in {"0", "false", "invalid", "no fix", "none", "not fixed", "unfixed"}:
        return True
    return "no fix" in normalized or "invalid" in normalized


def load_decoded_records(path: str | Path) -> list[dict[str, Any]]:
    """Load JSONL records defensively, preserving one dict per line."""

    records: list[dict[str, Any]] = []
    jsonl_path = Path(path)

    with jsonl_path.open("r", encoding="utf-8-sig") as handle:
        for line_number, raw_line in enumerate(handle, start=1):
            line = raw_line.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                LOGGER.warning("Skipping invalid JSONL line %s in %s", line_number, jsonl_path)
                continue

            if not isinstance(parsed, dict):
                LOGGER.warning("Skipping non-object JSONL line %s in %s", line_number, jsonl_path)
                continue

            for field_name in DECODED_BASE_FIELDS:
                if field_name not in parsed:
                    parsed[field_name] = [] if field_name in DECODED_LIST_FIELDS else None
            records.append(_normalize_record_semantics(parsed))

    return records


def _merge_quality(record: dict[str, Any], flags: list[str], state: str) -> dict[str, Any]:
    """Apply merged quality state and flags to a record."""

    existing_flags = _normalize_label_list(record.get("quality_flags"), label_type="quality_flags")
    record["quality_flags"] = _dedupe_sorted_labels(existing_flags + flags)
    record["quality_state"] = _merge_states(record.get("quality_state"), state)
    return record


def _merge_analysis_tags(record: dict[str, Any], tags: list[str]) -> dict[str, Any]:
    """Apply merged analysis tags to a record without changing quality state."""

    existing_tags = _normalize_label_list(record.get("analysis_tags"), label_type="analysis_tags")
    record["analysis_tags"] = _dedupe_sorted_labels(existing_tags + tags)
    return record


def _apply_function_family_mismatch(record: dict[str, Any]) -> dict[str, Any]:
    """Flag records whose function ID does not match the expected family range."""

    family = str(record.get("family") or "")
    function_id = _normalize_function_id(record.get("function_id"))
    if function_id is not None:
        record["function_id"] = function_id
    allowed = FAMILY_FUNCTION_IDS.get(family)
    if allowed and function_id is not None and function_id not in allowed:
        return _merge_quality(record, ["function_family_mismatch"], "suspect")
    return record


def _filter_environment_record(
    record: dict[str, Any],
    config: EnvironmentThresholdConfig,
) -> dict[str, Any]:
    """Apply value cleanup rules to environment payloads."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    flags: list[str] = []
    invalid_field_count = 0

    for field_name in ("moist_pc", "sleeper_rh", "humidity_pct"):
        value = _get_number(payload, field_name)
        if value is not None and not (0 <= value <= 100):
            flags.append(INVALID_VALUE_FLAG)
            invalid_field_count += 1

    flood_flag = _get_number(payload, "flood_flag")
    if flood_flag is not None and flood_flag not in {0, 1}:
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    rain_mm = _get_number(payload, "rain_mm")
    if rain_mm is None:
        rain_mm = _get_number(payload, "rainfall_mm")
    if rain_mm is not None and rain_mm < 0:
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    if payload.get("year") == 0xFFFF:
        payload["year"] = None
        flags.append("invalid_gps_time")

    for part_name in ("month", "day", "hour", "minute", "second"):
        if payload.get(part_name) == 0xFF:
            payload[part_name] = None
            flags.append("invalid_gps_time")

    for field_name in ("rtd1_t_x100", "rtd2_t_x100", "rtd3_t_x100", "rtd4_t_x100", "tmp102_t_x100"):
        value = _get_number(payload, field_name)
        if value is None:
            continue
        if abs(value) > config["temp_abs_limit_x100"]:
            payload[field_name] = None
            flags.append(INVALID_VALUE_FLAG)
            invalid_field_count += 1

    temp_abs_limit_c = config["temp_abs_limit_x100"] / 100
    temperature_c = _get_number(payload, "temperature_c")
    if temperature_c is not None and abs(temperature_c) > temp_abs_limit_c:
        payload["temperature_c"] = None
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    record["payload"] = payload
    record["_quality_meta"] = {"environment_invalid_field_count": invalid_field_count}
    state = "suspect" if flags else "decoded"
    return _merge_quality(record, flags, state)


def _filter_gas_record(record: dict[str, Any]) -> dict[str, Any]:
    """Apply conservative structural checks to gas payloads."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    flags: list[str] = []
    invalid_field_count = 0
    gas_conc = _get_number(payload, "gasConc")

    if _has_payload_field(payload, "gasConc") and gas_conc is None:
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    gas_type = payload.get("gasType")
    if _has_payload_field(payload, "gasType") and not _has_non_empty_string(gas_type):
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    if invalid_field_count == 0 and _has_non_empty_string(gas_type) and not _has_payload_field(payload, "gasConc"):
        flags.append("incomplete_gas_measurement")

    record["payload"] = payload
    record["_quality_meta"] = {"gas_invalid_field_count": invalid_field_count}
    if invalid_field_count:
        return _merge_quality(record, flags, "invalid")
    if "incomplete_gas_measurement" in flags:
        return _merge_quality(record, flags, "suspect")
    return _merge_quality(record, flags, "decoded")


def _filter_gps_status_record(record: dict[str, Any]) -> dict[str, Any]:
    """Apply conservative structural checks to GPS status payloads."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    flags: list[str] = []
    invalid_field_count = 0
    horizontal_dilution = _get_number(payload, "horizontalDilution")
    num_satellites = _get_number(payload, "numSatellites")

    if _has_payload_field(payload, "horizontalDilution") and (
        horizontal_dilution is None or horizontal_dilution < 0
    ):
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    if _has_payload_field(payload, "numSatellites") and (
        num_satellites is None or num_satellites < 0
    ):
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    if invalid_field_count == 0 and _gps_fix_is_unreliable(payload.get("gpsFixStatus")):
        flags.append("gps_fix_unreliable")

    record["payload"] = payload
    record["_quality_meta"] = {"gps_status_invalid_field_count": invalid_field_count}
    if invalid_field_count:
        return _merge_quality(record, flags, "invalid")
    if "gps_fix_unreliable" in flags:
        return _merge_quality(record, flags, "suspect")
    return _merge_quality(record, flags, "decoded")


def _filter_network_context_record(record: dict[str, Any]) -> dict[str, Any]:
    """Apply conservative structural checks to network context payloads."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    flags: list[str] = []
    invalid_field_count = 0
    bandwidth = _get_number(payload, "Bandwidth")
    spread_factor = _get_number(payload, "SpreadFactor")

    if _has_payload_field(payload, "Bandwidth") and (bandwidth is None or bandwidth <= 0):
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    if _has_payload_field(payload, "SpreadFactor") and (
        spread_factor is None or not (6 <= spread_factor <= 12)
    ):
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    record["payload"] = payload
    record["_quality_meta"] = {"network_context_invalid_field_count": invalid_field_count}
    state = "invalid" if invalid_field_count else "decoded"
    return _merge_quality(record, flags, state)


def _filter_device_telemetry_record(record: dict[str, Any]) -> dict[str, Any]:
    """Apply conservative structural checks to device telemetry payloads."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    flags: list[str] = []
    invalid_field_count = 0

    for field_name in ("batteryVoltage", "inputVoltage"):
        value = _get_number(payload, field_name)
        if _has_payload_field(payload, field_name) and (value is None or value <= 0):
            flags.append(INVALID_VALUE_FLAG)
            invalid_field_count += 1

    battery_charge = _get_number(payload, "batteryCharge")
    if _has_payload_field(payload, "batteryCharge") and battery_charge is None:
        flags.append(INVALID_VALUE_FLAG)
        invalid_field_count += 1

    has_battery_voltage = _has_payload_field(payload, "batteryVoltage")
    has_input_voltage = _has_payload_field(payload, "inputVoltage")
    has_battery_charge = _has_payload_field(payload, "batteryCharge")
    has_map_status = _has_payload_field(payload, "MapStatus")
    if invalid_field_count == 0 and (has_battery_charge or has_map_status) and not (
        has_battery_voltage or has_input_voltage
    ):
        flags.append("incomplete_device_telemetry")

    record["payload"] = payload
    record["_quality_meta"] = {"device_telemetry_invalid_field_count": invalid_field_count}
    if invalid_field_count:
        return _merge_quality(record, flags, "invalid")
    if "incomplete_device_telemetry" in flags:
        return _merge_quality(record, flags, "suspect")
    return _merge_quality(record, flags, "decoded")


def _microphone_band_values(payload: dict[str, Any]) -> tuple[dict[str, float], str]:
    """Return available microphone band powers under canonical keys plus scale."""

    expected_bands = [
        "band_0_100",
        "band_100_1000",
        "band_1000_5000",
        "band_5000_10000",
        "band_10000_15000",
        "band_15000_20000",
    ]
    values = {name: _get_number(payload, name) for name in expected_bands}
    if any(value is not None for value in values.values()):
        return {name: value for name, value in values.items() if value is not None}, "linear"

    nested = payload.get("band_powers_db")
    if not isinstance(nested, dict):
        return {}, "unknown"

    normalized: dict[str, float] = {}
    for alias, value in nested.items():
        try:
            if value is not None:
                normalized[str(alias)] = float(value)
        except (TypeError, ValueError):
            continue
    return normalized, "db"


def _filter_microphone_record(
    record: dict[str, Any],
    config: MicrophoneThresholdConfig,
) -> dict[str, Any]:
    """Apply numeric validity checks and analysis tagging for microphone FFT records."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    analysis_tags: list[str] = []
    total_power = _get_number(payload, "total_power")
    total_power_db = _get_number(payload, "total_power_db")
    spectral_flatness = _get_number(payload, "spectral_flatness")
    peak_freq_hz = _get_number(payload, "peak_freq_hz")
    spectral_centroid_hz = _get_number(payload, "spectral_centroid_hz")
    band_values, band_scale = _microphone_band_values(payload)

    invalid = False
    if total_power is not None and total_power <= 0:
        invalid = True
    if spectral_flatness is not None and not (0 <= spectral_flatness <= 1):
        invalid = True
    if band_scale == "linear" and any(value < 0 for value in band_values.values()):
        invalid = True
    if invalid:
        return _merge_quality(record, [INVALID_VALUE_FLAG], "invalid")

    if total_power is not None and band_values and band_scale == "linear":
        if total_power <= config["low_power_threshold"] and all(
            value <= config["low_band_power_threshold"] for value in band_values.values()
        ):
            analysis_tags.append("noise_like")

        band_100_1000 = band_values.get("band_100_1000")
        max_band_value = max(band_values.values()) if band_values else None
        if (
            band_100_1000 is not None
            and max_band_value is not None
            and band_100_1000 >= config["flatwheel_band_threshold"]
            and band_100_1000 == max_band_value
        ):
            analysis_tags.append("flatwheel_candidate")
    elif total_power_db is not None and band_values and band_scale == "db":
        # Minimal-version limitation:
        # for dB-only band payloads we keep compatibility for basic validity/noise-like
        # checks, but we do not infer flatwheel_candidate or squeal_candidate.
        if total_power_db <= -100 and all(value <= -90 for value in band_values.values()):
            analysis_tags.append("noise_like")

    high_band_1 = band_values.get("band_10000_15000")
    high_band_2 = band_values.get("band_15000_20000")
    if (
        band_scale == "linear"
        and
        peak_freq_hz is not None
        and spectral_centroid_hz is not None
        and high_band_1 is not None
        and high_band_2 is not None
        and peak_freq_hz >= config["squeal_peak_freq_threshold"]
        and spectral_centroid_hz >= config["squeal_centroid_threshold"]
        and high_band_1 >= config["high_band_threshold"]
        and high_band_2 >= config["high_band_threshold"]
    ):
        analysis_tags.append("squeal_candidate")

    record = _merge_analysis_tags(record, analysis_tags)
    return _merge_quality(record, [], "decoded")


def _filter_ae_record(
    record: dict[str, Any],
    config: AeThresholdConfig,
) -> dict[str, Any]:
    """Apply baseline validity rules and analysis tagging to AE records."""

    payload = _coerce_payload(record.get("payload"))
    if payload is None:
        return _merge_quality(record, ["invalid_payload"], "invalid")

    quality_flags: list[str] = []
    analysis_tags: list[str] = []
    amplitude_v = _get_number(payload, "amplitude_v")
    duration_s = _get_number(payload, "duration_s")
    if duration_s is None:
        duration_us = _get_number(payload, "duration_us")
        if duration_us is not None:
            duration_s = duration_us / 1_000_000.0
    counts = _get_number(payload, "counts")
    energy_marse = _get_number(payload, "energy_marse")
    if energy_marse is None:
        energy_marse = _get_number(payload, "energy")
    threshold_v = _get_number(payload, "threshold_v")
    if _get_bool(payload, "source_node_clock_ok") is False:
        quality_flags.append("time_unreliable")

    invalid = False
    if duration_s is not None and duration_s <= 0:
        invalid = True
    if counts is not None and counts < 0:
        invalid = True
    if energy_marse is not None and energy_marse < 0:
        invalid = True
    if threshold_v is not None and threshold_v <= 0:
        invalid = True
    if invalid:
        return _merge_quality(record, quality_flags + [INVALID_VALUE_FLAG], "invalid")

    if (
        amplitude_v is not None
        and threshold_v is not None
        and energy_marse is not None
        and amplitude_v <= threshold_v
        and energy_marse <= config["low_energy_threshold"]
    ):
        analysis_tags.append("noise_like")

    if (
        amplitude_v is not None
        and threshold_v is not None
        and energy_marse is not None
        and amplitude_v >= threshold_v + config["event_amplitude_margin"]
        and energy_marse >= config["event_energy_threshold"]
    ):
        analysis_tags.append("event_candidate")

    record = _merge_analysis_tags(record, analysis_tags)
    state = "suspect" if "time_unreliable" in quality_flags else "decoded"
    return _merge_quality(record, quality_flags, state)


def _filter_decoded_records_hard(
    records: list[dict[str, Any]],
    config: FilterThresholdConfig | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run the deterministic hard/static record filtering layer."""

    active_config = load_filter_threshold_config() if config is None else config
    cleaned_records: list[dict[str, Any]] = []
    quality_meta_counts = {
        "environment_invalid_field_count": 0,
        "gas_invalid_field_count": 0,
        "gps_status_invalid_field_count": 0,
        "network_context_invalid_field_count": 0,
        "device_telemetry_invalid_field_count": 0,
    }

    for raw_record in records:
        record = _normalize_record_semantics(dict(raw_record))
        family = record.get("family")
        if family == "environment":
            filtered_record = _filter_environment_record(record, active_config["environment"])
        elif family == "gas":
            filtered_record = _filter_gas_record(record)
        elif family == "gps_status":
            filtered_record = _filter_gps_status_record(record)
        elif family == "network_context":
            filtered_record = _filter_network_context_record(record)
        elif family == "device_telemetry":
            filtered_record = _filter_device_telemetry_record(record)
        elif family == "microphone_fft":
            filtered_record = _filter_microphone_record(record, active_config["microphone_fft"])
        elif family == "ae":
            filtered_record = _filter_ae_record(record, active_config["ae"])
        else:
            filtered_record = _merge_quality(record, [], record.get("quality_state") or "decoded")

        filtered_record = _apply_function_family_mismatch(filtered_record)
        quality_meta = filtered_record.pop("_quality_meta", {})
        if isinstance(quality_meta, dict):
            for key in quality_meta_counts:
                quality_meta_counts[key] += int(quality_meta.get(key, 0))
        cleaned_records.append(filtered_record)

    report = {
        "total_decoded_records": len(cleaned_records),
        "environment_records": sum(record.get("family") == "environment" for record in cleaned_records),
        "microphone_fft_records": sum(record.get("family") == "microphone_fft" for record in cleaned_records),
        "ae_records": sum(record.get("family") == "ae" for record in cleaned_records),
        "gas_records": sum(record.get("family") == "gas" for record in cleaned_records),
        "gps_status_records": sum(record.get("family") == "gps_status" for record in cleaned_records),
        "network_context_records": sum(
            record.get("family") == "network_context" for record in cleaned_records
        ),
        "device_telemetry_records": sum(
            record.get("family") == "device_telemetry" for record in cleaned_records
        ),
        "invalid_records": sum(record.get("quality_state") == "invalid" for record in cleaned_records),
        "suspect_records": sum(record.get("quality_state") == "suspect" for record in cleaned_records),
        "environment_invalid_field_count": quality_meta_counts["environment_invalid_field_count"],
        "gas_invalid_field_count": quality_meta_counts["gas_invalid_field_count"],
        "gps_status_invalid_field_count": quality_meta_counts["gps_status_invalid_field_count"],
        "network_context_invalid_field_count": quality_meta_counts["network_context_invalid_field_count"],
        "device_telemetry_invalid_field_count": quality_meta_counts[
            "device_telemetry_invalid_field_count"
        ],
        "microphone_noise_like_count": sum(
            "noise_like" in record.get("analysis_tags", [])
            for record in cleaned_records
            if record.get("family") == "microphone_fft"
        ),
        "microphone_flatwheel_candidate_count": sum(
            "flatwheel_candidate" in record.get("analysis_tags", [])
            for record in cleaned_records
            if record.get("family") == "microphone_fft"
        ),
        "microphone_squeal_candidate_count": sum(
            "squeal_candidate" in record.get("analysis_tags", [])
            for record in cleaned_records
            if record.get("family") == "microphone_fft"
        ),
        "ae_noise_like_count": sum(
            "noise_like" in record.get("analysis_tags", [])
            for record in cleaned_records
            if record.get("family") == "ae"
        ),
        "ae_event_candidate_count": sum(
            "event_candidate" in record.get("analysis_tags", [])
            for record in cleaned_records
            if record.get("family") == "ae"
        ),
        "ae_time_unreliable_count": sum(
            "time_unreliable" in record.get("quality_flags", [])
            for record in cleaned_records
            if record.get("family") == "ae"
        ),
    }
    return cleaned_records, report


def filter_decoded_records(
    records: list[dict[str, Any]],
    config: FilterThresholdConfig | None = None,
    dynamic_config: dict[str, Any] | str | Path | None = None,
    dynamic_mode: str | None = None,
    dynamic_state_path: str | Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Filter decoded records and optionally run dynamic filtering layers."""

    active_config = load_filter_threshold_config() if config is None else config

    def hard_filter(hard_records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return _filter_decoded_records_hard(hard_records, config=active_config)

    return run_filter_engine(
        records,
        hard_filter=hard_filter,
        dynamic_mode=dynamic_mode,
        dynamic_config=dynamic_config,
        dynamic_state_path=dynamic_state_path,
    )


def filter_decoded_records_with_state(
    records: list[dict[str, Any]],
    config: FilterThresholdConfig | None = None,
    dynamic_config: dict[str, Any] | str | Path | None = None,
    dynamic_mode: str | None = None,
    dynamic_state_path: str | Path | None = None,
    dynamic_state: DynamicState | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], DynamicState | None]:
    """Filter decoded records and return an unsaved candidate dynamic state."""

    active_config = load_filter_threshold_config() if config is None else config

    def hard_filter(hard_records: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        return _filter_decoded_records_hard(hard_records, config=active_config)

    return run_filter_engine_staged(
        records,
        hard_filter=hard_filter,
        dynamic_mode=dynamic_mode,
        dynamic_config=dynamic_config,
        dynamic_state_path=dynamic_state_path,
        dynamic_state=dynamic_state,
    )


def save_cleaned_records(records: list[dict[str, Any]], path: str | Path) -> None:
    """Write cleaned records to JSONL."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def save_quality_report(report: dict[str, Any], path: str | Path) -> None:
    """Write quality report JSON."""

    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, ensure_ascii=False)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""

    parser = argparse.ArgumentParser(description="SMART Sleeper filtering baseline")
    parser.add_argument("--records", required=True, help="Path to normalized JSONL records")
    parser.add_argument("--outdir", required=True, help="Output directory")
    parser.add_argument("--config", help="Optional path to filter threshold JSON config")
    parser.add_argument("--dynamic-config", help="Optional path to dynamic filtering JSON config")
    parser.add_argument("--dynamic-state", help="Optional path to persistent dynamic filtering state JSON")
    parser.add_argument(
        "--dynamic-mode",
        choices=("off", "shadow", "auto", "enforce"),
        default=None,
        help="Dynamic filtering mode. Defaults to off unless --dynamic-config is supplied.",
    )
    return parser.parse_args()


def main() -> None:
    """Filter normalized JSONL records."""

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    threshold_config = load_filter_threshold_config(args.config)
    decoded_records = load_decoded_records(args.records)
    cleaned_records, record_report = filter_decoded_records(
        decoded_records,
        config=threshold_config,
        dynamic_config=args.dynamic_config,
        dynamic_mode=args.dynamic_mode,
        dynamic_state_path=args.dynamic_state,
    )
    cleaned_records_path = outdir / "cleaned_records.jsonl"
    save_cleaned_records(cleaned_records, cleaned_records_path)

    quality_report_path = outdir / "quality_report.json"
    save_quality_report(record_report, quality_report_path)

    print(
        "Records: "
        f"total={record_report['total_decoded_records']} "
        f"suspect={record_report['suspect_records']} "
        f"invalid={record_report['invalid_records']}"
    )
    print(f"Wrote {cleaned_records_path}")
    print(f"Wrote {quality_report_path}")


if __name__ == "__main__":
    main()
