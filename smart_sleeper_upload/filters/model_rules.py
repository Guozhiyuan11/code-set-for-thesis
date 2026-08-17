"""Immutable autoencoder inference and Auto-mode state helpers."""

from __future__ import annotations

import json
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np


FEATURES = ("rtd1_t_x100", "rtd2_t_x100", "rtd3_t_x100", "rtd4_t_x100", "tmp102_t_x100", "moist_pc", "sleeper_rh")
TEMPERATURE_FEATURES = FEATURES[:5]
WEIGHT_NAMES = ("w1", "b1", "w2", "b2", "w3", "b3", "w4", "b4")
WEIGHT_SHAPES = ((7, 4), (4,), (4, 2), (2,), (2, 4), (4,), (4, 7), (7,))
REQUIRED_METADATA = {"model_version", "feature_order", "median", "iqr", "overall_threshold", "field_thresholds", "random_seed", "trained_at_utc", "training_feature_mapping", "validation_status", "metrics"}


@dataclass
class ModelRuntimeState:
    package_id: str | None = None
    effective_mode: str = "shadow"
    mode_transition_reason: str = "initial_shadow"
    valid_records: int = 0
    recent_complete: deque[bool] = field(default_factory=deque)
    recent_candidates: deque[bool] = field(default_factory=deque)
    consecutive_failures: int = 0

    def serialize(self) -> dict[str, Any]:
        return {"package_id": self.package_id, "effective_mode": self.effective_mode, "mode_transition_reason": self.mode_transition_reason, "valid_records": self.valid_records, "recent_complete": list(self.recent_complete), "recent_candidates": list(self.recent_candidates), "consecutive_failures": self.consecutive_failures}

    @classmethod
    def restore(cls, value: Any) -> "ModelRuntimeState":
        if not isinstance(value, dict): return cls()
        state = cls(package_id=value.get("package_id") if isinstance(value.get("package_id"), str) else None, effective_mode=str(value.get("effective_mode", value.get("mode", "shadow")) or "shadow"), mode_transition_reason=str(value.get("mode_transition_reason") or "restored"), valid_records=max(0, int(value.get("valid_records", 0))), consecutive_failures=max(0, int(value.get("consecutive_failures", 0))))
        state.recent_complete.extend(bool(item) for item in value.get("recent_complete", []))
        state.recent_candidates.extend(bool(item) for item in value.get("recent_candidates", []))
        return state

    def reset_for_package(self, package_id: str | None) -> None:
        self.package_id, self.effective_mode, self.valid_records, self.consecutive_failures = package_id, "shadow", 0, 0
        self.mode_transition_reason = "model_package_changed"
        self.recent_complete.clear(); self.recent_candidates.clear()

    def trim_windows(self, capacity: int) -> None:
        for values in (self.recent_complete, self.recent_candidates):
            while len(values) > capacity: values.popleft()


class AutoencoderPackage:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.metadata = self._load_metadata()
        self.median, self.iqr = np.asarray(self.metadata["median"], dtype=float), np.asarray(self.metadata["iqr"], dtype=float)
        self.field_thresholds = np.asarray([self.metadata["field_thresholds"][feature] for feature in FEATURES], dtype=float)
        self.overall_threshold = float(self.metadata["overall_threshold"])
        if self.median.shape != (7,) or self.iqr.shape != (7,) or np.any(self.iqr <= 0): raise ValueError("model package scaler is invalid")
        self.weights = self._load_weights()
        self.package_id = f"{self.metadata['model_version']}:{self.metadata['trained_at_utc']}"
        self.validation_passed = self.metadata["validation_status"] == "passed"

    def _load_metadata(self) -> dict[str, Any]:
        metadata_path, weights_path = self.path / "metadata.json", self.path / "weights.npz"
        if not metadata_path.is_file() or not weights_path.is_file(): raise ValueError("model package must contain metadata.json and weights.npz")
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        if REQUIRED_METADATA - set(metadata) or tuple(metadata.get("feature_order", [])) != FEATURES: raise ValueError("model package metadata is incomplete or has an unsupported feature order")
        return metadata

    def _load_weights(self) -> tuple[np.ndarray, ...]:
        with np.load(self.path / "weights.npz") as raw: weights = tuple(raw[name] for name in WEIGHT_NAMES)
        if tuple(weight.shape for weight in weights) != WEIGHT_SHAPES: raise ValueError("model package weights do not match 7→4→2→4→7")
        return weights

    def evaluate(self, record: dict[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
        values, reason = _model_input(record)
        if values is None: return None, reason
        scaled = (values - self.median) / self.iqr
        errors = np.square(scaled - self._reconstruct(scaled))
        return self._finding(errors), None

    def _reconstruct(self, scaled: np.ndarray) -> np.ndarray:
        w1, b1, w2, b2, w3, b3, w4, b4 = self.weights
        hidden_1 = np.tanh(scaled @ w1 + b1); hidden_2 = np.tanh(hidden_1 @ w2 + b2); hidden_3 = np.tanh(hidden_2 @ w3 + b3)
        return hidden_3 @ w4 + b4

    def _finding(self, errors: np.ndarray) -> dict[str, Any]:
        largest = int(errors.argmax())
        candidate_fields = [feature for feature, error, threshold in zip(FEATURES, errors, self.field_thresholds) if error > threshold]
        finding = {"rule_id": "model.autoencoder_reconstruction", "field": FEATURES[largest], "overall_reconstruction_error": float(errors.mean()), "error_by_field": {feature: float(error) for feature, error in zip(FEATURES, errors)}, "max_feature_error": float(errors[largest]), "temperature_group_error": float(errors[:5].mean()), "environment_group_error": float(errors[5:].mean()), "overall_threshold": self.overall_threshold, "field_threshold": float(self.field_thresholds[largest]), "field_thresholds": {feature: float(threshold) for feature, threshold in zip(FEATURES, self.field_thresholds)}, "model_version": self.metadata["model_version"]}
        if finding["overall_reconstruction_error"] > self.overall_threshold or candidate_fields:
            finding["candidate_flag"] = "reconstruction_outlier"; finding["affected_fields"] = candidate_fields or [FEATURES[largest]]
        return finding


def update_auto_mode(state: ModelRuntimeState, config: dict[str, Any], *, requested_mode: str, package_id: str | None, model_validated: bool, event_enabled: bool) -> str:
    settings = config["model_filter"]
    state.trim_windows(max(settings["readiness_window"], settings["fallback_window"]))
    if state.package_id != package_id: state.reset_for_package(package_id)
    state.effective_mode, state.mode_transition_reason = _mode_decision(state, settings, requested_mode, model_validated, event_enabled)
    return state.effective_mode


def _model_input(record: dict[str, Any]) -> tuple[np.ndarray | None, str | None]:
    if record.get("family") != "environment": return None, "family_not_applicable"
    payload = record.get("payload")
    if not isinstance(payload, dict): return None, "missing_payload"
    values: list[float] = []
    for feature in FEATURES:
        try: value = float(payload.get(feature))
        except (TypeError, ValueError): return None, f"missing_or_non_numeric:{feature}"
        if not np.isfinite(value): return None, f"missing_or_non_numeric:{feature}"
        if feature in TEMPERATURE_FEATURES: values.append(value / 100.0)
        elif 0 <= value <= 100: values.append(value)
        else: return None, f"out_of_range:{feature}"
    return np.asarray(values, dtype=float), None


def _mode_decision(state: ModelRuntimeState, settings: dict[str, Any], requested: str, validated: bool, event_enabled: bool) -> tuple[str, str]:
    if requested == "shadow": return "shadow", "requested_shadow"
    if requested == "enforce": return "enforce", "requested_enforce"
    if not validated: return "shadow", "model_validation_not_passed"
    if state.consecutive_failures: return "shadow", "model_failure"
    fallback_window = settings["fallback_window"]
    if state.effective_mode == "enforce" and _rate_tail(state.recent_candidates, fallback_window) > settings["fallback_candidate_rate"]: return "shadow", "candidate_rate_above_20_percent"
    if state.effective_mode == "enforce" and _rate_tail(state.recent_complete, fallback_window) < settings["fallback_input_completeness"]: return "shadow", "input_completeness_below_90_percent"
    if state.valid_records < settings["minimum_valid_records"]: return "shadow", f"waiting_for_{settings['minimum_valid_records']}_valid_records"
    if not event_enabled: return "shadow", "event_confirmation_disabled"
    readiness_window = settings["readiness_window"]
    if len(state.recent_complete) < readiness_window or _rate_tail(state.recent_complete, readiness_window) < settings["minimum_input_completeness"]: return "shadow", "input_completeness_below_95_percent"
    if _rate_tail(state.recent_candidates, readiness_window) > settings["maximum_candidate_rate"]: return "shadow", "candidate_rate_above_10_percent"
    return "enforce", "readiness_requirements_met"


def _rate_tail(values: deque[bool], count: int) -> float:
    tail = list(values)[-count:]
    return sum(tail) / len(tail) if tail else 0.0
