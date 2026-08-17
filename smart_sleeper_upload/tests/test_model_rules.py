from filters.model_rules import AutoencoderPackage, ModelRuntimeState, update_auto_mode


def _record():
    return {
        "family": "environment",
        "payload": {
            "rtd1_t_x100": 2200, "rtd2_t_x100": 2210, "rtd3_t_x100": 2190,
            "rtd4_t_x100": 2180, "tmp102_t_x100": 2170, "moist_pc": 30, "sleeper_rh": 45,
        },
    }


def test_autoencoder_package_is_deterministic_and_reports_all_errors():
    package = AutoencoderPackage("models/autoencoder-v0")
    first, first_skip = package.evaluate(_record())
    second, second_skip = package.evaluate(_record())
    assert first_skip is second_skip is None
    assert first == second
    assert set(first["error_by_field"]) == set(package.metadata["feature_order"])
    assert "overall_reconstruction_error" in first


def test_autoencoder_skips_incomplete_environment_records_without_candidate():
    record = _record()
    del record["payload"]["sleeper_rh"]
    finding, reason = AutoencoderPackage("models/autoencoder-v0").evaluate(record)
    assert finding is None
    assert reason == "missing_or_non_numeric:sleeper_rh"


def test_auto_mode_requires_a_validated_package_before_enforcement():
    state = ModelRuntimeState(package_id="package", valid_records=500)
    state.recent_complete.extend([True] * 200)
    state.recent_candidates.extend([False] * 200)
    config = {"model_filter": {"minimum_valid_records": 500, "readiness_window": 200, "fallback_window": 100, "minimum_input_completeness": .95, "fallback_input_completeness": .9, "maximum_candidate_rate": .1, "fallback_candidate_rate": .2}}
    assert update_auto_mode(state, config, requested_mode="auto", package_id="package", model_validated=False, event_enabled=True) == "shadow"
    assert state.mode_transition_reason == "model_validation_not_passed"


def test_auto_mode_counts_each_valid_record_once_after_duplicate_processing(tmp_path):
    from filter_rules import filter_decoded_records

    record = _record() | {"record_id": "model-dedup", "device_id": "SS-1", "timestamp": "2026-01-01T00:00:00Z", "quality_state": "decoded", "quality_flags": []}
    config = {"enabled": True, "model_filter": {"enabled": True, "model_path": "models/autoencoder-v0", "minimum_valid_records": 500, "readiness_window": 200, "fallback_window": 100, "maximum_candidate_rate": .1, "fallback_candidate_rate": .2, "minimum_input_completeness": .95, "fallback_input_completeness": .9}, "event_confirmation": {"enabled": True, "minimum_points": 3, "window_points": 5, "maximum_gap_seconds": 300, "minimum_duration_seconds": 0, "enforcement_policy": "confirmed_event"}}
    state_path = tmp_path / "state.json"
    filter_decoded_records([record], dynamic_config=config, dynamic_mode="auto", dynamic_state_path=state_path)
    _, report = filter_decoded_records([record], dynamic_config=config, dynamic_mode="auto", dynamic_state_path=state_path)
    from filters.state import load_dynamic_state
    state, _ = load_dynamic_state(state_path)
    assert state.model.valid_records == 1
    assert report["duplicate_state_update_count"] == 1
