import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

import run_pipeline
from filter_rules import filter_decoded_records
from filters.state import DynamicState, load_dynamic_state


def _ts(offset_seconds: int) -> str:
    minute, second = divmod(offset_seconds, 60)
    return f"2026-03-01T00:{minute:02d}:{second:02d}Z"


def _record(record_id, offset, payload, *, family="environment", device="SS-1", state="decoded"):
    return {
        "record_id": record_id,
        "schema_version": "test-v1",
        "sleeper_id": device,
        "device_id": device,
        "family": family,
        "function_id": "0x41",
        "source_time_utc": _ts(offset),
        "ingest_time_utc": _ts(offset + 1),
        "quality_state": state,
        "quality_flags": [],
        "analysis_tags": [],
        "payload": dict(payload),
        "platform_meta": {},
        "raw_unmapped": {},
    }


def _platform_record(record_id, offset, payload, *, family="environment"):
    return {
        "record_id": record_id,
        "schema_version": "test-v1",
        "family": family,
        "function_id": "0x41",
        "source_time_utc": _ts(offset),
        "ingest_time_utc": _ts(offset + 1),
        "quality_state": "decoded",
        "quality_flags": [],
        "analysis_tags": [],
        "payload": dict(payload),
        "platform_meta": {
            "ControllerName": "smart-sleeper-protos",
            "Site": "ARCS",
            "Location": "Perth",
            "Area": "Perth",
        },
        "raw_unmapped": {},
    }


def _config():
    return {
        "peer_sensor_groups": [],
        "cross_field_rules": [],
        "cross_family_rules": [],
        "adaptive": {"fields": {}, "expected_reporting": {}, "family_silence": {}},
    }


def test_disabled_families_do_not_enter_context_or_state(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["enabled_families"] = ["environment"]
    config["cross_family_rules"] = [
        {
            "rule_id": "test.gas_context",
            "family": "environment",
            "context_family": "gas",
            "field": "temperature_c",
            "context_field": "gasConc",
            "max_abs_difference": 1.0,
            "candidate_flag": "cross_family_disagreement",
        }
    ]
    config["adaptive"]["fields"] = {
        "gas.gasConc": {"step_threshold": 1.0},
        "environment.temperature_c": {"step_threshold": 1.0},
    }
    records = [
        _record("gas-disabled", 0, {"gasConc": 10.0}, family="gas"),
        _record("env-enabled", 60, {"temperature_c": 30.0}),
    ]

    cleaned, _ = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    assert cleaned[1]["filter_evidence"]["context"] == []
    assert all(key[1] != "gas" for key in state.adaptive.histories)
    context_keys = [
        json.loads(key)
        for key in state.context.serialize()["latest_by_device_family"]
    ]
    assert context_keys == [["SS-1", "environment"]]


def test_shadow_candidate_does_not_update_trusted_context(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["cross_family_rules"] = [
        {
            "rule_id": "test.gas_context",
            "family": "environment",
            "context_family": "gas",
            "field": "temperature_c",
            "context_field": "gasConc",
            "max_abs_difference": 1.0,
            "candidate_flag": "cross_family_disagreement",
        }
    ]
    config["adaptive"]["fields"] = {"gas.gasConc": {"step_threshold": 5.0}}
    records = [
        _record("gas-clean", 0, {"gasConc": 10.0}, family="gas"),
        _record("gas-shadow-candidate", 60, {"gasConc": 30.0}, family="gas"),
        _record("env-should-use-clean-context", 120, {"temperature_c": 10.0}),
    ]

    cleaned, _ = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )

    assert "adaptive.step_change" in {
        item["rule_id"] for item in cleaned[1]["filter_evidence"]["adaptive"]
    }
    assert cleaned[1]["quality_state"] == "decoded"
    assert cleaned[2]["filter_evidence"]["context"] == []


def test_context_blocked_field_does_not_mutate_stuck_state_but_unrelated_field_updates(tmp_path):
    state_path = tmp_path / "state.json"
    config = {
        "peer_sensor_groups": [
            {
                "rule_id": "test.peer",
                "family": "environment",
                "fields": ["rtd1_t_x100", "rtd2_t_x100", "temperature_c"],
                "field_scales": {"rtd1_t_x100": 0.01, "rtd2_t_x100": 0.01},
                "min_available": 3,
                "spread_threshold": 0.5,
                "candidate_flag": "peer_sensor_disagreement",
            }
        ],
        "adaptive": {
            "fields": {
                "environment.temperature_c": {
                    "stuck_enabled": True,
                    "stuck_tolerance": 0.0,
                    "stuck_sample_count": 2,
                },
                "environment.humidity_pct": {"step_threshold": 100.0},
            }
        },
    }
    records = [
        _record("ctx-stuck-1", 0, {"temperature_c": 20.0, "humidity_pct": 50.0}),
        _record(
            "ctx-stuck-2",
            60,
            {
                "rtd1_t_x100": 2000,
                "rtd2_t_x100": 2100,
                "temperature_c": 20.0,
                "humidity_pct": 51.0,
            },
        ),
    ]

    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    state, _ = load_dynamic_state(state_path)

    stuck = state.adaptive.stuck_runs[("SS-1", "environment", "temperature_c")]
    assert stuck.count == 1
    assert len(state.adaptive.histories[("SS-1", "environment", "humidity_pct")]) == 2


def test_sequence_findings_are_record_evidence_and_enforce_suspect():
    config = _config()
    config["adaptive"]["expected_reporting"] = {
        "environment": {"expected_interval_seconds": 60.0, "gap_multiplier": 2.0}
    }
    records = [
        _record("gap-1", 0, {"temperature_c": 20.0}),
        _record("gap-2", 130, {"temperature_c": 21.0}),
    ]

    shadow, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow")
    enforced, _ = filter_decoded_records(records, dynamic_config=config, dynamic_mode="enforce")

    assert shadow[1]["filter_evidence"]["sequence"][0]["rule_id"] == "sequence.reporting_gap"
    assert shadow[1]["quality_state"] == "decoded"
    assert enforced[1]["quality_state"] == "suspect"
    assert "reporting_gap" in enforced[1]["quality_flags"]


def test_dedup_reset_is_device_scoped_and_v1_migration_preserves_baselines(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["adaptive"]["fields"] = {"environment.temperature_c": {"step_threshold": 100.0}}
    records = [
        _record("dedup-a", 0, {"temperature_c": 20.0}, device="A"),
        _record("dedup-b", 0, {"temperature_c": 30.0}, device="B"),
    ]
    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    state, _ = load_dynamic_state(state_path)

    assert {entry.device_id for entry in state.deduplication} == {"A", "B"}
    state.reset_device("A")
    assert {entry.device_id for entry in state.deduplication} == {"B"}

    payload = json.loads(state_path.read_text(encoding="utf-8"))
    payload["state_schema_version"] = 1
    payload["deduplication"] = {"fingerprints": ["legacy-unmapped"]}
    state_path.write_text(json.dumps(payload), encoding="utf-8")
    migrated, _ = load_dynamic_state(state_path)

    assert migrated.deduplication == []
    assert ("A", "environment", "temperature_c") in migrated.adaptive.histories
    assert migrated.serialize()["state_schema_version"] == 3


def test_state_serializes_and_restores_device_family_keys_with_and_without_pipes(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["adaptive"]["fields"] = {"environment.temperature_c": {"step_threshold": 100.0}}
    records = [
        _record("normal-device", 0, {"temperature_c": 20.0}, device="SS-1"),
        _platform_record("platform-device", 0, {"temperature_c": 21.0}),
    ]

    filter_decoded_records(records, dynamic_config=config, dynamic_mode="shadow", dynamic_state_path=state_path)
    raw_state = json.loads(state_path.read_text(encoding="utf-8"))
    last_seen_keys = sorted(raw_state["adaptive"]["last_seen_by_device_family"])
    latest_record_keys = sorted(raw_state["adaptive"]["latest_record_by_device_family"])

    assert raw_state["state_schema_version"] == 3
    assert json.loads(last_seen_keys[0]) == ["SS-1", "environment"]
    assert json.loads(last_seen_keys[1]) == [
        "platform:smart-sleeper-protos|ARCS|Perth|Perth",
        "environment",
    ]
    assert latest_record_keys == last_seen_keys

    restored, _ = load_dynamic_state(state_path)

    assert ("SS-1", "environment") in restored.adaptive.last_seen_by_device_family
    assert (
        "platform:smart-sleeper-protos|ARCS|Perth|Perth",
        "environment",
    ) in restored.adaptive.last_seen_by_device_family
    assert (
        "platform:smart-sleeper-protos|ARCS|Perth|Perth",
        "environment",
    ) in restored.adaptive.latest_record_by_device_family


def test_legacy_pipe_delimited_state_keys_with_device_pipes_migrate_without_data_loss():
    device_id = "platform:smart-sleeper-protos|ARCS|Perth|Perth"
    device_family_key = f"{device_id}|environment"
    baseline_key = f"{device_id}|environment|temperature_c"
    event_key = f"{device_id}|environment|temperature_c|step_change"
    timestamp = "2026-03-01T00:00:00+00:00"
    historical = {"value": 20.0, "timestamp": timestamp, "record_id": "legacy"}
    legacy_payload = {
        "state_schema_version": 2,
        "adaptive": {
            "histories": {baseline_key: [historical]},
            "previous_values": {baseline_key: historical},
            "stuck_runs": {
                baseline_key: {
                    "value": 20.0,
                    "count": 1,
                    "start_timestamp": timestamp,
                }
            },
            "last_seen_by_device_family": {device_family_key: timestamp},
            "latest_record_by_device_family": {
                device_family_key: _platform_record("legacy-latest", 0, {"temperature_c": 20.0})
            },
            "reported_silence_events": [[device_id, "environment", timestamp]],
            "quarantines": {
                baseline_key: {
                    "values": [historical],
                    "start_timestamp": timestamp,
                    "last_value": 20.0,
                    "consecutive_count": 1,
                }
            },
            "baseline_versions": {baseline_key: 2},
        },
        "context": {
            "latest_by_device_family": {
                device_family_key: {
                    "timestamp": timestamp,
                    "record": _platform_record("legacy-context", 0, {"temperature_c": 20.0}),
                }
            }
        },
        "events": {
            "active_events": {
                event_key: {
                    "event_id": "event:legacy",
                    "device_id": device_id,
                    "family": "environment",
                    "field": "temperature_c",
                    "candidate_flag": "step_change",
                    "start_time_utc": timestamp,
                    "end_time_utc": timestamp,
                    "point_count": 1,
                    "contributing_record_ids": ["legacy"],
                    "confirmed": False,
                    "closed": False,
                }
            },
            "closed_events": {},
        },
        "deduplication": {
            "entries": [{"fingerprint": "fp-legacy", "device_id": device_id}]
        },
    }

    migrated = DynamicState.restore(legacy_payload)

    assert (device_id, "environment") in migrated.adaptive.last_seen_by_device_family
    assert (device_id, "environment") in migrated.adaptive.latest_record_by_device_family
    assert (device_id, "environment", "temperature_c") in migrated.adaptive.histories
    assert (device_id, "environment", "temperature_c") in migrated.adaptive.quarantines
    assert migrated.adaptive.baseline_versions[(device_id, "environment", "temperature_c")] == 2
    assert (device_id, "environment") in migrated.context._latest_by_device_family
    assert (device_id, "environment", "temperature_c", "step_change") in migrated.events.active_events
    assert migrated.deduplication[0].device_id == device_id

    serialized = migrated.serialize()

    assert serialized["state_schema_version"] == 3
    assert json.loads(next(iter(serialized["adaptive"]["last_seen_by_device_family"]))) == [
        device_id,
        "environment",
    ]
    assert json.loads(next(iter(serialized["context"]["latest_by_device_family"]))) == [
        device_id,
        "environment",
    ]


def test_platform_device_id_state_loads_through_second_independent_filtering_run(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["adaptive"]["fields"] = {"environment.temperature_c": {"step_threshold": 100.0}}

    filter_decoded_records(
        [_platform_record("platform-run-1", 0, {"temperature_c": 20.0})],
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    filter_decoded_records(
        [_platform_record("platform-run-2", 60, {"temperature_c": 21.0})],
        dynamic_config=config,
        dynamic_mode="shadow",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)

    key = ("platform:smart-sleeper-protos|ARCS|Perth|Perth", "environment", "temperature_c")
    assert len(state.adaptive.histories[key]) == 2


def test_long_continuous_event_keeps_stable_event_id(tmp_path):
    state_path = tmp_path / "state.json"
    config = _config()
    config["adaptive"]["fields"] = {"environment.temperature_c": {"step_threshold": 5.0}}
    config["event_confirmation"] = {
        "enabled": True,
        "minimum_points": 2,
        "window_points": 2,
        "maximum_gap_seconds": 300,
        "minimum_duration_seconds": 0,
        "enforcement_policy": "point",
    }
    records = [
        _record("event-long-1", 0, {"temperature_c": 10.0}),
        _record("event-long-2", 60, {"temperature_c": 20.0}),
        _record("event-long-3", 120, {"temperature_c": 30.0}),
        _record("event-long-4", 180, {"temperature_c": 40.0}),
        _record("event-long-5", 240, {"temperature_c": 50.0}),
    ]

    _, report = filter_decoded_records(
        records,
        dynamic_config=config,
        dynamic_mode="enforce",
        dynamic_state_path=state_path,
    )
    state, _ = load_dynamic_state(state_path)
    active_events = list(state.events.active_events.values())

    assert len({event["event_id"] for event in report["anomaly_events"]}) == 1
    assert len(active_events) == 1
    assert active_events[0].event_id == report["anomaly_events"][0]["event_id"]
    assert active_events[0].point_count == 4
    assert len(active_events[0].contributing_record_ids) == 2


def test_mocked_pipeline_uploads_fixed_snapshot_on_every_execution(tmp_path, monkeypatch):
    raw_path = Path(__file__).parents[1] / "data" / "smart_sleeper_data.json"
    workdir = tmp_path / "work"
    config_path = tmp_path / "dynamic.json"
    config_path.write_text(json.dumps({"adaptive": {"fields": {}}}), encoding="utf-8")
    remote_state = {}

    def fake_fetch_state(**_):
        if "value" not in remote_state:
            return {"found": False}
        return {"found": True, "state_json": remote_state["value"]}

    monkeypatch.setattr(run_pipeline, "fetch_dynamic_state", fake_fetch_state)
    upload_calls = []

    def fake_upload(**kwargs):
        upload_calls.append(kwargs)
        assert kwargs["candidate_state"] is not None
        assert kwargs["source_type"] == "smart_sleeper"
        assert kwargs["execution_slot"] == "2026-03-01T00:00:00+00:00"
        remote_state["value"] = kwargs["candidate_state"].serialize()
        return {
            "success": True,
            "ingest_run_id": "00000000-0000-0000-0000-000000000001",
            "run_key": "dynamic-v2:test",
            "inserted_records": len(kwargs["records"]),
            "updated_records": 0,
            "skipped_records": 0,
            "anomaly_events": 0,
            "state_saved": True,
        }

    monkeypatch.setattr(run_pipeline, "upload_dynamic", fake_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline.py",
            "--input-json",
            str(raw_path),
            "--workdir",
            str(workdir),
            "--execution-slot",
            "2026-03-01T00:00:00Z",
            "--dynamic-config",
            str(config_path),
            "--dynamic-mode",
            "shadow",
        ],
    )

    assert run_pipeline.main() == 0
    assert run_pipeline.main() == 0
    assert len(upload_calls) == 2
    assert [len(call["records"]) for call in upload_calls] == [32, 0]
    assert all(len(call["run_record_fingerprints"]) == 32 for call in upload_calls)
    assert not workdir.exists()
    assert not (workdir / "filtered_output").exists()


def test_execution_slot_is_stable_within_each_fifteen_minute_interval():
    first = run_pipeline.resolve_execution_slot(
        None,
        now=datetime(2026, 3, 1, 0, 14, 59, tzinfo=timezone.utc),
    )
    second = run_pipeline.resolve_execution_slot(
        None,
        now=datetime(2026, 3, 1, 0, 15, 0, tzinfo=timezone.utc),
    )

    assert first == "2026-03-01T00:00:00+00:00"
    assert second == "2026-03-01T00:15:00+00:00"


def test_records_not_seen_omits_complete_duplicates():
    existing = _record("existing", 0, {"temperature_c": 20.0})
    new = _record("new", 60, {"temperature_c": 21.0})
    known = {run_pipeline.record_fingerprint(existing)}

    selected = run_pipeline.records_not_seen([existing, new], known)

    assert [record["record_id"] for record in selected] == ["new"]
