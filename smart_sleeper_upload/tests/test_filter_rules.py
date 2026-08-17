import json
import subprocess
import sys
from pathlib import Path

import pytest

from filters.config import validate_dynamic_filter_config
from filters.state import load_dynamic_state
from filter_rules import (
    filter_decoded_records,
    load_filter_threshold_config,
    load_decoded_records,
)


def _write_threshold_config(tmp_path, payload):
    config_path = tmp_path / "filter_thresholds.json"
    config_path.write_text(json.dumps(payload), encoding="utf-8")
    return config_path


def test_filter_environment_record_repairs_gps_time_and_temperature():
    records = [
        {
            "record_id": "r1",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 1,
            "function_id": "0x41",
            "family": "environment",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "year": 0xFFFF,
                "month": 0xFF,
                "rtd1_t_x100": 25001,
                "moist_pc": 30,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "suspect"
    assert cleaned[0]["payload"]["year"] is None
    assert cleaned[0]["payload"]["month"] is None
    assert cleaned[0]["payload"]["rtd1_t_x100"] is None
    assert "invalid_gps_time" in cleaned[0]["quality_flags"]
    assert "invalid_value" in cleaned[0]["quality_flags"]
    assert cleaned[0]["analysis_tags"] == []
    assert report["environment_invalid_field_count"] == 1


def test_filter_microphone_record_candidate_tags_without_invalidating():
    records = [
        {
            "record_id": "r2",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 2,
            "function_id": "0x47",
            "family": "microphone_fft",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "total_power": 30,
                "peak_freq_hz": 9000,
                "spectral_centroid_hz": 8200,
                "spectral_flatness": 0.2,
                "band_0_100": 3,
                "band_100_1000": 25,
                "band_1000_5000": 4,
                "band_5000_10000": 5,
                "band_10000_15000": 18,
                "band_15000_20000": 17,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "decoded"
    assert cleaned[0]["quality_flags"] == []
    assert "flatwheel_candidate" in cleaned[0]["analysis_tags"]
    assert "squeal_candidate" in cleaned[0]["analysis_tags"]
    assert report["microphone_flatwheel_candidate_count"] == 1
    assert report["microphone_squeal_candidate_count"] == 1
    assert report["suspect_records"] == 0


def test_filter_microphone_record_migrates_legacy_analysis_flags():
    records = [
        {
            "record_id": "r2-legacy",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 21,
            "function_id": "0x47",
            "family": "microphone_fft",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "suspect",
            "quality_flags": ["flatwheel_candidate"],
            "payload": {
                "total_power": 30,
                "peak_freq_hz": 9000,
                "spectral_centroid_hz": 8200,
                "spectral_flatness": 0.2,
                "band_0_100": 3,
                "band_100_1000": 25,
                "band_1000_5000": 4,
                "band_5000_10000": 5,
                "band_10000_15000": 18,
                "band_15000_20000": 17,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "decoded"
    assert cleaned[0]["quality_flags"] == []
    assert "flatwheel_candidate" in cleaned[0]["analysis_tags"]
    assert "squeal_candidate" in cleaned[0]["analysis_tags"]
    assert report["suspect_records"] == 0


def test_filter_microphone_record_respects_flatwheel_threshold_override(tmp_path):
    config = load_filter_threshold_config(
        _write_threshold_config(
            tmp_path,
            {
                "microphone_fft": {
                    "flatwheel_band_threshold": 30.0,
                }
            },
        )
    )
    records = [
        {
            "record_id": "r2-config",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 22,
            "function_id": "0x47",
            "family": "microphone_fft",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "total_power": 30,
                "peak_freq_hz": 9000,
                "spectral_centroid_hz": 8200,
                "spectral_flatness": 0.2,
                "band_0_100": 3,
                "band_100_1000": 25,
                "band_1000_5000": 4,
                "band_5000_10000": 5,
                "band_10000_15000": 18,
                "band_15000_20000": 17,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records, config=config)

    assert cleaned[0]["quality_state"] == "decoded"
    assert "flatwheel_candidate" not in cleaned[0]["analysis_tags"]
    assert "squeal_candidate" in cleaned[0]["analysis_tags"]
    assert report["microphone_flatwheel_candidate_count"] == 0
    assert report["microphone_squeal_candidate_count"] == 1


def test_filter_ae_record_marks_invalid_and_time_unreliable():
    records = [
        {
            "record_id": "r3",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 3,
            "function_id": "0x4C",
            "family": "ae",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "amplitude_v": 0.2,
                "duration_s": 0.0,
                "counts": 10,
                "energy_marse": 2.0,
                "threshold_v": 0.1,
                "source_node_clock_ok": False,
            },
        },
        {
            "record_id": "r4",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 4,
            "function_id": "0x4C",
            "family": "ae",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "amplitude_v": 0.15,
                "duration_s": 0.01,
                "counts": 5,
                "energy_marse": 3.0,
                "threshold_v": 0.15,
                "source_node_clock_ok": False,
            },
        },
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "invalid"
    assert "invalid_value" in cleaned[0]["quality_flags"]
    assert cleaned[0]["analysis_tags"] == []
    assert cleaned[1]["quality_state"] == "suspect"
    assert "noise_like" in cleaned[1]["analysis_tags"]
    assert "time_unreliable" in cleaned[1]["quality_flags"]
    assert "noise_like" not in cleaned[1]["quality_flags"]
    assert report["ae_noise_like_count"] == 1
    assert report["ae_time_unreliable_count"] == 2


def test_filter_ae_record_respects_event_threshold_override(tmp_path):
    config = load_filter_threshold_config(
        _write_threshold_config(
            tmp_path,
            {
                "ae": {
                    "event_energy_threshold": 40.0,
                }
            },
        )
    )
    records = [
        {
            "record_id": "r4-config",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 23,
            "function_id": "0x4C",
            "family": "ae",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "amplitude_v": 0.25,
                "duration_s": 0.01,
                "counts": 5,
                "energy_marse": 50.0,
                "threshold_v": 0.15,
                "source_node_clock_ok": True,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records, config=config)

    assert cleaned[0]["quality_state"] == "decoded"
    assert cleaned[0]["quality_flags"] == []
    assert "event_candidate" in cleaned[0]["analysis_tags"]
    assert report["ae_event_candidate_count"] == 1


def test_load_decoded_records_skips_bad_lines(tmp_path):
    jsonl_path = tmp_path / "records.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "record_id": "r1",
                        "schema_version": "decoded_record/v1",
                        "sleeper_id": "SS-1",
                        "batch_id": 1,
                        "function_id": "0x41",
                        "family": "environment",
                        "source_time_utc": "2026-03-01T00:00:00Z",
                        "ingest_time_utc": "2026-03-01T00:00:01Z",
                        "quality_state": "decoded",
                        "quality_flags": [],
                        "payload": {},
                    }
                ),
                "not-json",
            ]
        ),
        encoding="utf-8",
    )

    records = load_decoded_records(jsonl_path)

    assert len(records) == 1
    assert records[0]["record_id"] == "r1"
    assert records[0]["analysis_tags"] == []


def test_load_decoded_records_handles_utf8_bom_on_first_line(tmp_path):
    jsonl_path = tmp_path / "records_bom.jsonl"
    jsonl_path.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "record_id": "bom-1",
                        "schema_version": "decoded_record/v1",
                        "sleeper_id": "SS-1",
                        "batch_id": 1,
                        "function_id": "0x41",
                        "family": "environment",
                        "source_time_utc": "2026-03-01T00:00:00Z",
                        "ingest_time_utc": "2026-03-01T00:00:01Z",
                        "quality_state": "decoded",
                        "quality_flags": [],
                        "payload": {},
                    }
                ),
                json.dumps(
                    {
                        "record_id": "bom-2",
                        "schema_version": "decoded_record/v1",
                        "sleeper_id": "SS-1",
                        "batch_id": 2,
                        "function_id": "0x47",
                        "family": "microphone_fft",
                        "source_time_utc": "2026-03-01T00:00:00Z",
                        "ingest_time_utc": "2026-03-01T00:00:01Z",
                        "quality_state": "decoded",
                        "quality_flags": [],
                        "payload": {},
                    }
                ),
            ]
        ),
        encoding="utf-8-sig",
    )

    records = load_decoded_records(jsonl_path)

    assert [record["record_id"] for record in records] == ["bom-1", "bom-2"]
    assert all(record["analysis_tags"] == [] for record in records)


def test_clean_record_quality_state_uses_decoded_not_accepted():
    records = [
        {
            "record_id": "r-clean",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 5,
            "function_id": "0x41",
            "family": "environment",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "moist_pc": 40,
                "sleeper_rh": 55,
                "rain_mm": 1,
                "flood_flag": 0,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "decoded"
    assert report["suspect_records"] == 0
    assert report["invalid_records"] == 0


def test_function_family_mismatch_marks_record_suspect():
    records = [
        {
            "record_id": "r-mismatch",
            "schema_version": "decoded_record/v1",
            "sleeper_id": "SS-1",
            "batch_id": 6,
            "function_id": "0x41",
            "family": "microphone_fft",
            "source_time_utc": "2026-03-01T00:00:00Z",
            "ingest_time_utc": "2026-03-01T00:00:01Z",
            "quality_state": "decoded",
            "quality_flags": [],
            "payload": {
                "total_power": 10,
                "spectral_flatness": 0.3,
                "band_0_100": 1,
                "band_100_1000": 2,
                "band_1000_5000": 1,
                "band_5000_10000": 1,
                "band_10000_15000": 1,
                "band_15000_20000": 1,
            },
        }
    ]

    cleaned, report = filter_decoded_records(records)

    assert cleaned[0]["quality_state"] == "suspect"
    assert "function_family_mismatch" in cleaned[0]["quality_flags"]
    assert cleaned[0]["analysis_tags"] == []
    assert report["suspect_records"] == 1


def _dynamic_timestamp(offset_seconds):
    hours = offset_seconds // 3600
    minutes = (offset_seconds % 3600) // 60
    seconds = offset_seconds % 60
    return f"2026-03-01T{hours:02d}:{minutes:02d}:{seconds:02d}Z"


def _dynamic_record(
    record_id,
    offset_seconds,
    payload,
    *,
    device="SS-1",
    family="environment",
    quality_state="decoded",
    quality_flags=None,
    platform_meta=None,
    raw_unmapped=None,
):
    return {
        "record_id": record_id,
        "schema_version": "decoded_record/v1",
        "sleeper_id": device,
        "function_id": "0x41" if family == "environment" else None,
        "family": family,
        "source_time_utc": _dynamic_timestamp(offset_seconds),
        "ingest_time_utc": _dynamic_timestamp(offset_seconds + 1),
        "quality_state": quality_state,
        "quality_flags": list(quality_flags or []),
        "analysis_tags": [],
        "payload": dict(payload),
        "platform_meta": dict(platform_meta or {}),
        "raw_unmapped": dict(raw_unmapped or {}),
    }


def _peer_config(*, threshold=5.0, min_available=3):
    return {
        "peer_sensor_groups": [
            {
                "rule_id": "test.peer_temperature_disagreement",
                "family": "environment",
                "fields": ["rtd1_t_x100", "rtd2_t_x100", "temperature_c", "temp"],
                "field_scales": {
                    "rtd1_t_x100": 0.01,
                    "rtd2_t_x100": 0.01,
                    "temperature_c": 1.0,
                    "temp": 1.0,
                },
                "aliases": [["temperature_c", "temp"]],
                "min_available": min_available,
                "spread_threshold": threshold,
                "candidate_flag": "peer_sensor_disagreement",
            }
        ],
        "adaptive": {"fields": {}},
    }


def _adaptive_config(fields, *, expected_reporting=None, family_silence=None):
    return {
        "peer_sensor_groups": [],
        "adaptive": {
            "fields": fields,
            "expected_reporting": expected_reporting or {},
            "family_silence": family_silence or {},
        },
    }


def _context_rule_ids(record):
    return [
        item["rule_id"]
        for item in record.get("filter_evidence", {}).get("context", [])
    ]


def _adaptive_rule_ids(record):
    return [
        item["rule_id"]
        for item in record.get("filter_evidence", {}).get("adaptive", [])
    ]


def test_dynamic_off_preserves_existing_hard_static_behavior():
    records = [
        _dynamic_record(
            "hard-off",
            0,
            {"year": 0xFFFF, "month": 0xFF, "rtd1_t_x100": 25001},
        )
    ]

    cleaned, report = filter_decoded_records(records, dynamic_mode="off")

    assert cleaned[0]["quality_state"] == "suspect"
    assert "invalid_value" in cleaned[0]["quality_flags"]
    assert "filter_evidence" not in cleaned[0]
    assert report["filtering_mode"] == "off"


def test_context_rules_do_nothing_when_required_fields_are_absent():
    records = [_dynamic_record("ctx-absent", 0, {"temperature_c": 20.0})]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=1.0, min_available=3),
        dynamic_mode="shadow",
    )

    assert _context_rule_ids(cleaned[0]) == []
    assert report["context_candidate_count"] == 0


def test_peer_temperature_disagreement_is_detected_with_explicit_config():
    records = [
        _dynamic_record(
            "ctx-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0, min_available=3),
        dynamic_mode="shadow",
    )

    assert "test.peer_temperature_disagreement" in _context_rule_ids(cleaned[0])
    assert report["context_candidate_count"] == 1


def test_temperature_aliases_are_not_counted_twice():
    records = [
        _dynamic_record(
            "ctx-alias",
            0,
            {"rtd1_t_x100": 5000, "temperature_c": 20.0, "temp": 20.0},
        )
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=10.0, min_available=3),
        dynamic_mode="shadow",
    )

    assert _context_rule_ids(cleaned[0]) == []
    assert report["context_candidate_count"] == 0


def test_shadow_mode_does_not_change_hard_layer_state_or_flags():
    records = [
        _dynamic_record(
            "shadow-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="shadow",
    )

    assert cleaned[0]["quality_state"] == "decoded"
    assert cleaned[0]["quality_flags"] == []
    assert report["context_candidate_count"] == 1


def test_enforce_mode_escalates_decoded_records_to_suspect():
    records = [
        _dynamic_record(
            "enforce-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="enforce",
    )

    assert cleaned[0]["quality_state"] == "suspect"
    assert "peer_sensor_disagreement" in cleaned[0]["quality_flags"]
    assert report["enforced_suspect_count"] == 1


def test_dynamic_rules_never_downgrade_invalid_records():
    records = [
        _dynamic_record(
            "invalid-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
            quality_state="invalid",
            quality_flags=["invalid_value"],
        )
    ]

    cleaned, _ = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="enforce",
    )

    assert cleaned[0]["quality_state"] == "invalid"
    assert "invalid_value" in cleaned[0]["quality_flags"]


def test_adaptive_rules_do_nothing_during_cold_start():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
            }
        }
    )
    records = [
        _dynamic_record("cold-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("cold-2", 60, {"temperature_c": 40.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert all("adaptive.robust_baseline" not in _adaptive_rule_ids(record) for record in cleaned)
    assert report["adaptive_candidate_count"] == 0
    assert report["cold_start_count"] == 2


def test_clear_outlier_after_sufficient_history_is_detected():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            }
        }
    )
    records = [
        _dynamic_record("hist-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("hist-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("hist-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("hist-4", 180, {"temperature_c": 40.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.robust_baseline" in _adaptive_rule_ids(cleaned[-1])
    assert report["adaptive_candidate_count"] == 1


def test_current_sample_is_not_included_in_its_own_baseline():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 1,
                "mad_score_threshold": 1.0,
                "epsilon": 0.1,
            }
        }
    )
    records = [
        _dynamic_record("no-lookahead-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("no-lookahead-2", 60, {"temperature_c": 100.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    outlier_evidence = [
        item
        for item in cleaned[1]["filter_evidence"]["adaptive"]
        if item["rule_id"] == "adaptive.robust_baseline"
    ][0]

    assert outlier_evidence["history_samples"] == 1


def test_invalid_historical_values_are_excluded_from_baseline():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 2,
                "mad_score_threshold": 1.0,
                "epsilon": 0.1,
            }
        }
    )
    records = [
        _dynamic_record(
            "invalid-history",
            0,
            {"temperature_c": 100.0},
            quality_state="invalid",
            quality_flags=["invalid_value"],
        ),
        _dynamic_record("valid-history", 60, {"temperature_c": 20.0}),
        _dynamic_record("after-invalid", 120, {"temperature_c": 40.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.robust_baseline" not in _adaptive_rule_ids(cleaned[-1])
    assert report["adaptive_candidate_count"] == 0
    assert report["cold_start_count"] == 2


def test_adaptive_baselines_are_separate_by_device():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            }
        }
    )
    records = [
        _dynamic_record("a1", 0, {"temperature_c": 20.0}, device="A"),
        _dynamic_record("a2", 60, {"temperature_c": 20.0}, device="A"),
        _dynamic_record("a3", 120, {"temperature_c": 20.0}, device="A"),
        _dynamic_record("b1", 180, {"temperature_c": 80.0}, device="B"),
        _dynamic_record("a4", 240, {"temperature_c": 80.0}, device="A"),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    by_id = {record["record_id"]: record for record in cleaned}

    assert "adaptive.robust_baseline" in _adaptive_rule_ids(by_id["a4"])
    assert "adaptive.robust_baseline" not in _adaptive_rule_ids(by_id["b1"])


def test_adaptive_baselines_are_separate_by_field_and_family():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            },
            "environment.humidity_pct": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            },
            "gas.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            },
        }
    )
    records = [
        _dynamic_record("temp-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("temp-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("temp-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("humid-outlier", 180, {"humidity_pct": 90.0}),
        _dynamic_record("gas-outlier", 240, {"temperature_c": 80.0}, family="gas"),
        _dynamic_record("temp-outlier", 300, {"temperature_c": 80.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    by_id = {record["record_id"]: record for record in cleaned}

    assert "adaptive.robust_baseline" in _adaptive_rule_ids(by_id["temp-outlier"])
    assert "adaptive.robust_baseline" not in _adaptive_rule_ids(by_id["humid-outlier"])
    assert "adaptive.robust_baseline" not in _adaptive_rule_ids(by_id["gas-outlier"])


def test_input_order_does_not_change_dynamic_results():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            }
        }
    )
    ordered = [
        _dynamic_record("order-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("order-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("order-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("order-4", 180, {"temperature_c": 80.0}),
    ]
    shuffled = [ordered[3], ordered[1], ordered[0], ordered[2]]

    cleaned_ordered, _ = filter_decoded_records(ordered, dynamic_config=config, dynamic_mode="shadow")
    cleaned_shuffled, _ = filter_decoded_records(shuffled, dynamic_config=config, dynamic_mode="shadow")
    ordered_rules = {record["record_id"]: _adaptive_rule_ids(record) for record in cleaned_ordered}
    shuffled_rules = {record["record_id"]: _adaptive_rule_ids(record) for record in cleaned_shuffled}

    assert ordered_rules == shuffled_rules


def test_irregular_timestamps_are_handled_for_step_rules():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "step_threshold": 5.0,
                "min_time_gap_seconds": 5.0,
                "max_time_gap_seconds": 20.0,
            }
        }
    )
    records = [
        _dynamic_record("irregular-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("irregular-2", 30, {"temperature_c": 30.0}),
        _dynamic_record("irregular-3", 40, {"temperature_c": 50.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.step_change" not in _adaptive_rule_ids(cleaned[1])
    assert "adaptive.step_change" in _adaptive_rule_ids(cleaned[2])
    assert report["adaptive_candidate_count"] == 1


def test_step_change_detection_works():
    config = _adaptive_config({"environment.temperature_c": {"step_threshold": 5.0}})
    records = [
        _dynamic_record("step-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("step-2", 60, {"temperature_c": 20.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.step_change" in _adaptive_rule_ids(cleaned[1])


def test_rate_of_change_detection_uses_elapsed_time():
    config = _adaptive_config(
        {"environment.temperature_c": {"rate_threshold_per_second": 4.0}}
    )
    records = [
        _dynamic_record("rate-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("rate-2", 2, {"temperature_c": 20.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    rate_evidence = [
        item
        for item in cleaned[1]["filter_evidence"]["adaptive"]
        if item["rule_id"] == "adaptive.rate_change"
    ][0]

    assert rate_evidence["elapsed_seconds"] == 2.0
    assert rate_evidence["rate_per_second"] == 5.0


def test_stuck_sensor_detection_works_after_configured_count():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "stuck_enabled": True,
                "stuck_tolerance": 0.0,
                "stuck_sample_count": 3,
            }
        }
    )
    records = [
        _dynamic_record("stuck-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("stuck-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("stuck-3", 120, {"temperature_c": 20.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.stuck_sensor" in _adaptive_rule_ids(cleaned[2])
    assert report["adaptive_candidate_count"] == 1


def test_reporting_gaps_are_added_to_sequence_findings():
    config = _adaptive_config(
        {},
        expected_reporting={
            "environment": {
                "expected_interval_seconds": 60.0,
                "gap_multiplier": 2.0,
            }
        },
    )
    records = [
        _dynamic_record("gap-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("gap-2", 130, {"temperature_c": 21.0}),
    ]

    _, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert report["reporting_gap_count"] == 1
    assert report["sequence_findings"][0]["rule_id"] == "sequence.reporting_gap"


def test_family_silence_events_are_added_to_sequence_findings():
    config = _adaptive_config(
        {},
        family_silence={
            "gas": {
                "expected_interval_seconds": 60.0,
                "gap_multiplier": 2.0,
            }
        },
    )
    records = [
        _dynamic_record("gas-seen", 0, {"gasConc": 1.0}, family="gas"),
        _dynamic_record("env-after-silence", 130, {"temperature_c": 20.0}),
    ]

    _, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert report["family_silence_count"] == 1
    assert report["sequence_findings"][0]["rule_id"] == "sequence.family_silence"


def test_raw_metadata_and_unmapped_data_are_preserved_with_dynamic_filtering():
    records = [
        _dynamic_record(
            "metadata-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
            platform_meta={"ControllerName": "smart-sleeper-1"},
            raw_unmapped={"extra": 42},
        )
    ]

    cleaned, _ = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="enforce",
    )

    assert cleaned[0]["platform_meta"] == {"ControllerName": "smart-sleeper-1"}
    assert cleaned[0]["raw_unmapped"] == {"extra": 42}


def test_dynamic_configuration_validation_rejects_unknown_or_invalid_values():
    with pytest.raises(ValueError):
        validate_dynamic_filter_config({"unknown": True}, source="test")

    with pytest.raises(ValueError):
        validate_dynamic_filter_config(
            {
                "adaptive": {
                    "fields": {
                        "environment.temperature_c": {
                            "mad_score_threshold": -1.0,
                        }
                    }
                }
            },
            source="test",
        )


def test_rules_without_explicit_thresholds_remain_disabled():
    records = [
        _dynamic_record(
            "disabled-rules",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]
    config = {
        "peer_sensor_groups": [
            {
                "family": "environment",
                "fields": ["rtd1_t_x100", "rtd2_t_x100", "temperature_c"],
                "min_available": 3,
                "spread_threshold": None,
            }
        ],
        "adaptive": {
            "fields": {
                "environment.temperature_c": {
                    "window_size": 5,
                    "min_samples": 1,
                }
            }
        },
    }

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert _context_rule_ids(cleaned[0]) == []
    assert _adaptive_rule_ids(cleaned[0]) == []
    assert report["context_candidate_count"] == 0
    assert report["adaptive_candidate_count"] == 0


def test_dynamic_output_is_deterministic_across_repeated_runs():
    records = [
        _dynamic_record("det-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("det-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("det-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("det-4", 180, {"temperature_c": 80.0}),
    ]
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            }
        }
    )

    first_records, first_report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")
    second_records, second_report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")

    assert json.dumps(first_records, sort_keys=True) == json.dumps(second_records, sort_keys=True)
    assert json.dumps(first_report, sort_keys=True) == json.dumps(second_report, sort_keys=True)


def test_cli_dynamic_filtering_with_decoded_jsonl_records(tmp_path):
    records_path = tmp_path / "records.jsonl"
    config_path = tmp_path / "dynamic.json"
    outdir = tmp_path / "out"
    records = [
        _dynamic_record(
            "cli-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]
    records_path.write_text(
        "\n".join(json.dumps(record) for record in records),
        encoding="utf-8",
    )
    config_path.write_text(json.dumps(_peer_config(threshold=5.0)), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "filter_rules.py",
            "--records",
            str(records_path),
            "--outdir",
            str(outdir),
            "--dynamic-config",
            str(config_path),
            "--dynamic-mode",
            "enforce",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    cleaned = [
        json.loads(line)
        for line in (outdir / "cleaned_records.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    report = json.loads((outdir / "quality_report.json").read_text(encoding="utf-8"))
    assert cleaned[0]["quality_state"] == "decoded"
    assert "peer_sensor_disagreement" not in cleaned[0]["quality_flags"]
    assert report["filtering_mode"] == "enforce"


def test_enforce_report_recalculates_final_quality_counts():
    records = [
        _dynamic_record(
            "final-count-peer",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        ),
        _dynamic_record("final-count-clean", 60, {"temperature_c": 20.0}),
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="enforce",
    )

    assert [record["quality_state"] for record in cleaned].count("suspect") == 1
    assert report["decoded_records"] == 1
    assert report["suspect_records"] == 1
    assert report["invalid_records"] == 0


def test_dynamic_config_without_explicit_mode_defaults_to_shadow():
    records = [
        _dynamic_record(
            "api-shadow-default",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        )
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=_peer_config(threshold=5.0))

    assert report["filtering_mode"] == "shadow"
    assert cleaned[0]["quality_state"] == "decoded"
    assert report["context_candidate_count"] == 1


def test_dynamic_configuration_validation_rejects_edge_cases():
    bad_configs = [
        {"adaptive": {"fields": {"environment.temperature_c": {"min_samples": 0}}}},
        {"adaptive": {"fields": {"environment.temperature_c": {"window_size": 2, "min_samples": 3}}}},
        {
            "adaptive": {
                "fields": {
                    "environment.temperature_c": {
                        "min_time_gap_seconds": 10.0,
                        "max_time_gap_seconds": 5.0,
                    }
                }
            }
        },
        {"event_confirmation": {"minimum_points": 3, "window_points": 2}},
        {
            "adaptive": {
                "fields": {
                    "environment.temperature_c": {
                        "baseline_reentry": {"consecutive_samples": 0}
                    }
                }
            }
        },
        {"event_confirmation": {"unknown": True}},
    ]

    for config in bad_configs:
        with pytest.raises(ValueError):
            validate_dynamic_filter_config(config, source="bad-test")


def test_enabled_false_disables_all_dynamic_behaviour():
    config = _adaptive_config(
        {"environment.temperature_c": {"step_threshold": 1.0}},
        expected_reporting={"environment": {"expected_interval_seconds": 1.0, "gap_multiplier": 1.0}},
    )
    config.update(_peer_config(threshold=1.0))
    config["enabled"] = False
    records = [
        _dynamic_record(
            "disabled-all-1",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
        ),
        _dynamic_record("disabled-all-2", 10, {"temperature_c": 100.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")

    assert all("filter_evidence" not in record for record in cleaned)
    assert report["context_candidate_count"] == 0
    assert report["adaptive_candidate_count"] == 0
    assert report["sequence_findings"] == []
    assert report["suspect_records"] == 0


def test_family_qualified_enabled_fields_work_in_context_and_adaptive():
    config = _peer_config(threshold=5.0)
    config["enabled_fields"] = ["environment.temperature_c", "environment.rtd1_t_x100", "environment.rtd2_t_x100"]
    config["adaptive"] = {
        "fields": {
            "environment.temperature_c": {"step_threshold": 1.0},
            "environment.humidity_pct": {"step_threshold": 1.0},
        }
    }
    records = [
        _dynamic_record("qualified-1", 0, {"temperature_c": 20.0, "humidity_pct": 50.0}),
        _dynamic_record(
            "qualified-2",
            60,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0, "humidity_pct": 90.0},
        ),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "test.peer_temperature_disagreement" in _context_rule_ids(cleaned[1])
    assert "adaptive.step_change" in _adaptive_rule_ids(cleaned[1])
    assert all(
        not (item.get("field") == "humidity_pct" and item.get("rule_id") == "adaptive.step_change")
        for item in cleaned[1]["filter_evidence"]["adaptive"]
    )


def test_missing_timestamp_produces_non_candidate_evidence_and_no_state_update(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {"environment.temperature_c": {"window_size": 3, "min_samples": 1, "mad_score_threshold": 1.0}}
    )
    record = _dynamic_record("missing-time", 0, {"temperature_c": 20.0})
    record.pop("source_time_utc")
    record.pop("ingest_time_utc")

    cleaned, report = filter_decoded_records(
        [record],
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    evidence = cleaned[0]["filter_evidence"]["adaptive"]
    assert evidence == [{"rule_id": "dynamic.timestamp_unavailable", "state": "not_evaluated"}]
    assert "candidate_flag" not in evidence[0]
    assert report["timestamp_not_evaluated_count"] == 1
    assert state.adaptive.histories == {}


def test_missing_timestamp_does_not_trigger_step_or_rate():
    config = _adaptive_config(
        {"environment.temperature_c": {"step_threshold": 1.0, "rate_threshold_per_second": 1.0}}
    )
    records = [
        _dynamic_record("time-ok", 0, {"temperature_c": 20.0}),
        _dynamic_record("time-missing", 60, {"temperature_c": 30.0}),
    ]
    records[1].pop("source_time_utc")
    records[1].pop("ingest_time_utc")

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.step_change" not in _adaptive_rule_ids(cleaned[1])
    assert "adaptive.rate_change" not in _adaptive_rule_ids(cleaned[1])


def test_hard_invalid_records_skip_dynamic_processing_and_state(tmp_path):
    state_path = tmp_path / "state.json"
    records = [
        _dynamic_record(
            "hard-invalid-dynamic",
            0,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 45.0},
            quality_state="invalid",
            quality_flags=["invalid_value"],
        )
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=_peer_config(threshold=5.0),
        dynamic_mode="enforce",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert cleaned[0]["quality_state"] == "invalid"
    assert cleaned[0]["filter_evidence"]["context"] == []
    assert cleaned[0]["filter_evidence"]["adaptive"] == []
    assert state.context.serialize()["latest_by_device_family"] == {}
    assert state.adaptive.histories == {}
    assert report["context_candidate_count"] == 0


def test_suspect_context_requires_explicit_configuration():
    config = {
        "peer_sensor_groups": [],
        "cross_family_rules": [
            {
                "rule_id": "test.gas_context",
                "family": "environment",
                "context_family": "gas",
                "field": "temperature_c",
                "context_field": "gasConc",
                "max_abs_difference": 1.0,
                "candidate_flag": "cross_family_disagreement",
            }
        ],
        "adaptive": {"fields": {}},
    }
    records = [
        _dynamic_record("suspect-gas", 0, {"gasConc": 10.0}, family="gas", quality_state="suspect"),
        _dynamic_record("env-uses-gas", 60, {"temperature_c": 30.0}),
    ]

    default_cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    config["allow_suspect_context"] = True
    allowed_cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert _context_rule_ids(default_cleaned[1]) == []
    assert "test.gas_context" in _context_rule_ids(allowed_cleaned[1])


def test_context_findings_identify_affected_fields_and_block_only_those_baselines(tmp_path):
    state_path = tmp_path / "state.json"
    config = _peer_config(threshold=5.0)
    config["adaptive"] = {
        "fields": {
            "environment.temperature_c": {"window_size": 5, "min_samples": 3, "mad_score_threshold": 5.0},
            "environment.humidity_pct": {"window_size": 5, "min_samples": 3, "mad_score_threshold": 5.0},
        }
    }
    records = [
        _dynamic_record("ctx-protect-1", 0, {"temperature_c": 20.0, "humidity_pct": 50.0}),
        _dynamic_record(
            "ctx-protect-2",
            60,
            {
                "rtd1_t_x100": 2000,
                "rtd2_t_x100": 2100,
                "temperature_c": 45.0,
                "humidity_pct": 51.0,
            },
        ),
    ]

    cleaned, _ = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    context_finding = cleaned[1]["filter_evidence"]["context"][0]
    assert sorted(context_finding["affected_fields"]) == [
        "rtd1_t_x100",
        "rtd2_t_x100",
        "temperature_c",
    ]
    assert len(state.adaptive.histories[("SS-1", "environment", "temperature_c")]) == 1
    assert len(state.adaptive.histories[("SS-1", "environment", "humidity_pct")]) == 2


def test_cross_field_and_cross_family_findings_identify_affected_fields():
    config = {
        "peer_sensor_groups": [],
        "cross_field_rules": [
            {
                "rule_id": "test.cross_field",
                "family": "environment",
                "fields": ["temperature_c", "humidity_pct"],
                "max_abs_difference": 1.0,
                "candidate_flag": "cross_field_disagreement",
            }
        ],
        "cross_family_rules": [
            {
                "rule_id": "test.cross_family",
                "family": "environment",
                "context_family": "gas",
                "field": "temperature_c",
                "context_field": "gasConc",
                "max_abs_difference": 1.0,
                "candidate_flag": "cross_family_disagreement",
            }
        ],
        "adaptive": {"fields": {}},
    }
    records = [
        _dynamic_record("cf-gas", 0, {"gasConc": 10.0}, family="gas"),
        _dynamic_record("cf-env", 60, {"temperature_c": 30.0, "humidity_pct": 40.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    findings = cleaned[1]["filter_evidence"]["context"]
    by_rule = {finding["rule_id"]: finding for finding in findings}

    assert by_rule["test.cross_field"]["affected_fields"] == ["temperature_c", "humidity_pct"]
    assert by_rule["test.cross_family"]["affected_fields"] == ["temperature_c"]


def test_same_timestamp_cross_family_context_is_order_independent():
    config = {
        "peer_sensor_groups": [],
        "cross_family_rules": [
            {
                "rule_id": "test.same_time_context",
                "family": "environment",
                "context_family": "gas",
                "field": "temperature_c",
                "context_field": "gasConc",
                "max_abs_difference": 1.0,
                "tolerance_seconds": 0.0,
                "candidate_flag": "cross_family_disagreement",
            }
        ],
        "adaptive": {"fields": {}},
    }
    gas = _dynamic_record("same-time-gas", 0, {"gasConc": 10.0}, family="gas")
    env = _dynamic_record("same-time-env", 0, {"temperature_c": 30.0})

    first, _ = filter_decoded_records([gas, env], dynamic_config=config, dynamic_mode="shadow")
    second, _ = filter_decoded_records([env, gas], dynamic_config=config, dynamic_mode="shadow")
    first_env = next(record for record in first if record["record_id"] == "same-time-env")
    second_env = next(record for record in second if record["record_id"] == "same-time-env")

    assert _context_rule_ids(first_env) == ["test.same_time_context"]
    assert _context_rule_ids(second_env) == ["test.same_time_context"]


def test_future_timestamp_context_is_not_used():
    config = {
        "peer_sensor_groups": [],
        "cross_family_rules": [
            {
                "rule_id": "test.no_future_context",
                "family": "environment",
                "context_family": "gas",
                "field": "temperature_c",
                "context_field": "gasConc",
                "max_abs_difference": 1.0,
                "tolerance_seconds": 999.0,
                "candidate_flag": "cross_family_disagreement",
            }
        ],
        "adaptive": {"fields": {}},
    }
    records = [
        _dynamic_record("future-env", 0, {"temperature_c": 30.0}),
        _dynamic_record("future-gas", 60, {"gasConc": 10.0}, family="gas"),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert _context_rule_ids(cleaned[0]) == []


def test_minimum_scale_prevents_zero_mad_overreaction():
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 4.0,
                "minimum_scale": 1.0,
            }
        }
    )
    records = [
        _dynamic_record("scale-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("scale-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("scale-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("scale-4", 180, {"temperature_c": 20.1}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")

    assert "adaptive.robust_baseline" not in _adaptive_rule_ids(cleaned[-1])


def test_quarantine_isolated_outlier_does_not_migrate_and_return_clears(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
                "baseline_reentry": {
                    "enabled": True,
                    "consecutive_samples": 3,
                    "value_tolerance": 0.5,
                    "minimum_duration_seconds": 0,
                    "action": "reset",
                },
            }
        }
    )
    records = [
        _dynamic_record("quarantine-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("quarantine-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("quarantine-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("quarantine-outlier", 180, {"temperature_c": 30.0}),
        _dynamic_record("quarantine-return", 240, {"temperature_c": 20.0}),
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert "adaptive.robust_baseline" in _adaptive_rule_ids(cleaned[3])
    assert report["baseline_shift_count"] == 0
    assert state.adaptive.quarantines == {}


def test_sustained_new_level_confirms_baseline_shift_and_increments_version(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
                "baseline_reentry": {
                    "enabled": True,
                    "consecutive_samples": 3,
                    "value_tolerance": 0.5,
                    "minimum_duration_seconds": 0,
                    "action": "reset",
                },
            }
        }
    )
    records = [
        _dynamic_record("shift-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("shift-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("shift-3", 120, {"temperature_c": 20.0}),
        _dynamic_record("shift-4", 180, {"temperature_c": 30.0}),
        _dynamic_record("shift-5", 240, {"temperature_c": 30.1}),
        _dynamic_record("shift-6", 300, {"temperature_c": 29.9}),
    ]

    cleaned, report = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert "adaptive.baseline_shift" in _adaptive_rule_ids(cleaned[-1])
    assert report["baseline_shift_count"] == 1
    assert state.adaptive.baseline_versions[("SS-1", "environment", "temperature_c")] == 2


def test_context_affected_candidates_cannot_migrate_baseline(tmp_path):
    state_path = tmp_path / "state.json"
    config = _peer_config(threshold=5.0)
    config["adaptive"] = {
        "fields": {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
                "baseline_reentry": {
                    "enabled": True,
                    "consecutive_samples": 2,
                    "value_tolerance": 0.5,
                    "minimum_duration_seconds": 0,
                    "action": "reset",
                },
            }
        }
    }
    records = [
        _dynamic_record("ctx-shift-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("ctx-shift-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("ctx-shift-3", 120, {"temperature_c": 20.0}),
        _dynamic_record(
            "ctx-shift-4",
            180,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 30.0},
        ),
        _dynamic_record(
            "ctx-shift-5",
            240,
            {"rtd1_t_x100": 2000, "rtd2_t_x100": 2100, "temperature_c": 30.0},
        ),
    ]

    _, report = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert report["baseline_shift_count"] == 0
    assert state.adaptive.baseline_versions[("SS-1", "environment", "temperature_c")] == 1


def test_persistent_state_reload_continues_without_cold_start(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
                "epsilon": 0.1,
            }
        }
    )
    first_records = [
        _dynamic_record("persist-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("persist-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("persist-3", 120, {"temperature_c": 20.0}),
    ]
    second_records = [_dynamic_record("persist-4", 180, {"temperature_c": 40.0})]

    _, first_report = filter_decoded_records(
        first_records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    second_cleaned, second_report = filter_decoded_records(
        second_records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )

    assert first_report["state_saved"] is True
    assert second_report["state_loaded"] is True
    assert "adaptive.robust_baseline" in _adaptive_rule_ids(second_cleaned[0])
    assert second_report["cold_start_count"] == 0


def test_state_reload_preserves_context_silence_quarantine_and_stuck_runs(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "step_threshold": 5.0,
                "stuck_enabled": True,
                "stuck_tolerance": 0.0,
                "stuck_sample_count": 3,
                "baseline_reentry": {
                    "enabled": True,
                    "consecutive_samples": 3,
                    "value_tolerance": 0.5,
                    "minimum_duration_seconds": 0,
                    "action": "reset",
                },
            }
        },
        family_silence={"gas": {"expected_interval_seconds": 60.0, "gap_multiplier": 2.0}},
    )
    records = [
        _dynamic_record("persist-gas", 0, {"gasConc": 1.0}, family="gas"),
        _dynamic_record("persist-env-1", 60, {"temperature_c": 20.0}),
        _dynamic_record("persist-env-2", 120, {"temperature_c": 20.0}),
        _dynamic_record("persist-env-3", 180, {"temperature_c": 30.0}),
    ]

    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    state, loaded = load_dynamic_state(state_path)

    assert loaded is True
    assert state.context.serialize()["latest_by_device_family"]
    assert state.adaptive.reported_silence_events
    assert state.adaptive.quarantines
    assert state.adaptive.stuck_runs


def test_unsupported_state_version_is_rejected(tmp_path):
    state_path = tmp_path / "bad_state.json"
    state_path.write_text(json.dumps({"state_schema_version": 999}), encoding="utf-8")

    with pytest.raises(ValueError):
        load_dynamic_state(state_path)


def test_duplicate_processing_does_not_update_state_twice(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config(
        {
            "environment.temperature_c": {
                "window_size": 5,
                "min_samples": 3,
                "mad_score_threshold": 5.0,
            }
        }
    )
    records = [
        _dynamic_record("dedup-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("dedup-2", 60, {"temperature_c": 21.0}),
    ]

    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    second_cleaned, second_report = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert second_report["duplicate_state_update_count"] == 2
    assert all(
        item["rule_id"] == "dynamic.duplicate_state_update_skipped"
        for record in second_cleaned
        for item in record["filter_evidence"]["adaptive"]
    )
    assert len(state.adaptive.histories[("SS-1", "environment", "temperature_c")]) == 2


def test_device_reset_removes_only_that_device_state(tmp_path):
    state_path = tmp_path / "state.json"
    config = _adaptive_config({"environment.temperature_c": {"step_threshold": 5.0}})
    records = [
        _dynamic_record("reset-a", 0, {"temperature_c": 20.0}, device="A"),
        _dynamic_record("reset-b", 0, {"temperature_c": 30.0}, device="B"),
    ]

    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    state, _ = load_dynamic_state(state_path)
    state.reset_device("A")

    assert ("A", "environment", "temperature_c") not in state.adaptive.histories
    assert ("B", "environment", "temperature_c") in state.adaptive.histories


def test_event_confirmation_confirmed_event_policy_waits_for_repeated_candidates():
    config = _adaptive_config({"environment.temperature_c": {"step_threshold": 5.0}})
    config["event_confirmation"] = {
        "enabled": True,
        "minimum_points": 2,
        "window_points": 3,
        "maximum_gap_seconds": 300,
        "minimum_duration_seconds": 0,
        "enforcement_policy": "confirmed_event",
    }
    records = [
        _dynamic_record("event-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("event-2", 60, {"temperature_c": 20.0}),
        _dynamic_record("event-3", 120, {"temperature_c": 30.0}),
    ]

    cleaned, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")

    assert cleaned[1]["quality_state"] == "decoded"
    assert cleaned[2]["quality_state"] == "suspect"
    assert report["confirmed_anomaly_event_count"] == 1
    assert report["anomaly_events"][0]["candidate_flag"] == "step_change"
    assert report["anomaly_events"][0]["field"] == "temperature_c"


def test_event_confirmation_separates_fields_and_candidate_flags():
    config = _adaptive_config(
        {
            "environment.temperature_c": {"step_threshold": 5.0},
            "environment.humidity_pct": {"step_threshold": 5.0},
        }
    )
    config["event_confirmation"] = {
        "enabled": True,
        "minimum_points": 2,
        "window_points": 4,
        "maximum_gap_seconds": 300,
        "minimum_duration_seconds": 0,
        "enforcement_policy": "point",
    }
    records = [
        _dynamic_record("event-sep-1", 0, {"temperature_c": 10.0, "humidity_pct": 50.0}),
        _dynamic_record("event-sep-2", 60, {"temperature_c": 20.0, "humidity_pct": 50.0}),
        _dynamic_record("event-sep-3", 120, {"temperature_c": 30.0, "humidity_pct": 70.0}),
        _dynamic_record("event-sep-4", 180, {"temperature_c": 30.0, "humidity_pct": 90.0}),
    ]

    _, report = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")
    fields = sorted(event["field"] for event in report["anomaly_events"])

    assert fields == ["humidity_pct", "temperature_c"]
    assert len({event["event_id"] for event in report["anomaly_events"]}) == 2


def test_point_event_policy_preserves_existing_enforce_behaviour():
    config = _adaptive_config({"environment.temperature_c": {"step_threshold": 5.0}})
    config["event_confirmation"] = {
        "enabled": True,
        "minimum_points": 3,
        "window_points": 3,
        "maximum_gap_seconds": 300,
        "minimum_duration_seconds": 0,
        "enforcement_policy": "point",
    }
    records = [
        _dynamic_record("point-event-1", 0, {"temperature_c": 10.0}),
        _dynamic_record("point-event-2", 60, {"temperature_c": 20.0}),
    ]

    cleaned, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")

    assert cleaned[1]["quality_state"] == "suspect"


def test_cli_dynamic_state_end_to_end_and_duplicate_protection(tmp_path):
    records_path = tmp_path / "records.jsonl"
    config_path = tmp_path / "dynamic.json"
    state_path = tmp_path / "state.json"
    outdir = tmp_path / "out"
    records = [
        _dynamic_record("cli-state-1", 0, {"temperature_c": 20.0}),
        _dynamic_record("cli-state-2", 60, {"temperature_c": 21.0}),
    ]
    records_path.write_text("\n".join(json.dumps(record) for record in records), encoding="utf-8")
    config_path.write_text(
        json.dumps(_adaptive_config({"environment.temperature_c": {"step_threshold": 5.0}})),
        encoding="utf-8",
    )

    first = subprocess.run(
        [
            sys.executable,
            "filter_rules.py",
            "--records",
            str(records_path),
            "--outdir",
            str(outdir),
            "--dynamic-config",
            str(config_path),
            "--dynamic-state",
            str(state_path),
            "--dynamic-mode",
            "shadow",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        text=True,
        capture_output=True,
        check=False,
    )
    second = subprocess.run(
        [
            sys.executable,
            "filter_rules.py",
            "--records",
            str(records_path),
            "--outdir",
            str(outdir),
            "--dynamic-config",
            str(config_path),
            "--dynamic-state",
            str(state_path),
            "--dynamic-mode",
            "shadow",
        ],
        cwd=Path(__file__).resolve().parent.parent,
        text=True,
        capture_output=True,
        check=False,
    )

    assert first.returncode == 0, first.stderr
    assert second.returncode == 0, second.stderr
    assert state_path.exists()
    assert (outdir / "cleaned_records.jsonl").exists()
    report = json.loads((outdir / "quality_report.json").read_text(encoding="utf-8"))
    assert report["state_loaded"] is True
    assert report["state_saved"] is True
    assert report["duplicate_state_update_count"] == 2
