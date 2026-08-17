"""Filtering engine that composes hard, context, adaptive, state, and event layers."""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from .adaptive_rules import BaselineKey, evaluate_adaptive_rules, update_adaptive_state
from .config import load_dynamic_filter_config, normalize_dynamic_mode, validate_dynamic_filter_config
from .context_rules import ContextState, evaluate_context_rules
from .event_rules import event_enforcement_policy
from .model_rules import AutoencoderPackage, update_auto_mode
from .models import (
    dedupe_sorted_labels,
    device_id_for_record,
    ensure_filter_evidence,
    family_enabled,
    final_quality_counts,
    finding_is_candidate,
    merge_quality_state,
    normalize_quality_state,
    record_fingerprint,
    record_timestamp,
    sorted_count_dict,
    timestamp_sort_value,
)
from .state import STATE_SCHEMA_VERSION, DynamicState, load_dynamic_state, save_dynamic_state

HardLayer = Callable[
    [list[dict[str, Any]]],
    tuple[list[dict[str, Any]], dict[str, Any]],
]


def run_filter_engine(
    records: list[dict[str, Any]],
    *,
    hard_filter: HardLayer,
    dynamic_mode: str | None = None,
    dynamic_config: dict[str, Any] | str | Path | None = None,
    dynamic_state_path: str | Path | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run hard filtering plus optional dynamic filtering layers."""

    cleaned_records, report, next_state = run_filter_engine_staged(
        records,
        hard_filter=hard_filter,
        dynamic_mode=dynamic_mode,
        dynamic_config=dynamic_config,
        dynamic_state_path=dynamic_state_path,
    )
    if next_state is not None and dynamic_state_path is not None:
        save_dynamic_state(next_state, dynamic_state_path)
        report["state_saved"] = True
    return cleaned_records, report


def run_filter_engine_staged(
    records: list[dict[str, Any]],
    *,
    hard_filter: HardLayer,
    dynamic_mode: str | None = None,
    dynamic_config: dict[str, Any] | str | Path | None = None,
    dynamic_state_path: str | Path | None = None,
    dynamic_state: DynamicState | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any], DynamicState | None]:
    """Run filtering and return the candidate next dynamic state without saving it."""

    mode = normalize_dynamic_mode(
        dynamic_mode,
        dynamic_config_supplied=dynamic_config is not None,
    )
    hard_records, hard_report = hard_filter(records)
    report = dict(hard_report)
    report["filtering_mode"] = mode
    report["hard_layer_records_evaluated"] = len(hard_records)
    report["hard_invalid_count"] = sum(
        normalize_quality_state(record.get("quality_state")) == "invalid"
        for record in hard_records
    )

    if mode == "off":
        report.update(final_quality_counts(hard_records))
        return hard_records, report, None

    config = _resolve_dynamic_config(dynamic_config)
    if not config.get("enabled", True):
        dynamic_report = _new_dynamic_report(mode, len(hard_records), report["hard_invalid_count"])
        report.update(_finalize_dynamic_report(dynamic_report, hard_records))
        return hard_records, report, None

    if dynamic_state is None:
        state, state_loaded = load_dynamic_state(dynamic_state_path)
    else:
        state = dynamic_state
        state_loaded = True
    dynamic_report = _new_dynamic_report(mode, len(hard_records), report["hard_invalid_count"])
    dynamic_report["state_loaded"] = state_loaded
    run_seen_fingerprints: set[str] = set()
    model = None
    model_config = config["model_filter"]
    if model_config["enabled"]:
        try:
            model = AutoencoderPackage(model_config["model_path"])
            update_auto_mode(state.model, config, requested_mode=mode, package_id=model.package_id, model_validated=model.validation_passed, event_enabled=config["event_confirmation"]["enabled"])
            dynamic_report["model_version"] = model.metadata["model_version"]
        except (OSError, ValueError, KeyError) as exc:
            dynamic_report["model_load_failure"] = str(exc)
            state.model.consecutive_failures += 1
            update_auto_mode(state.model, config, requested_mode=mode, package_id=None, model_validated=False, event_enabled=config["event_confirmation"]["enabled"])

    for group in _record_groups(hard_records):
        snapshot = _same_time_context_snapshot(group, hard_records, state.context, config)
        pending_updates: list[tuple[dict[str, Any], set[BaselineKey], set[BaselineKey], bool]] = []
        pending_context_updates: list[tuple[dict[str, Any], bool]] = []

        for index in group:
            record = hard_records[index]
            evidence = ensure_filter_evidence(record, mode)
            evidence["hard"] = _hard_evidence(record)

            if normalize_quality_state(record.get("quality_state")) == "invalid":
                continue

            timestamp = record_timestamp(record)
            fingerprint = record_fingerprint(record)
            duplicate = state.fingerprint_seen(fingerprint) or fingerprint in run_seen_fingerprints
            state_update_eligible = _record_can_update_state(record, config)
            if duplicate:
                evidence["adaptive"].append(
                    {
                        "rule_id": "dynamic.duplicate_state_update_skipped",
                        "state": "not_updated",
                    }
                )
                dynamic_report["duplicate_state_update_count"] += 1

            context_findings = evaluate_context_rules(record, config, snapshot)
            context_block_keys = _context_block_keys(record, context_findings)

            if duplicate:
                adaptive_findings: list[dict[str, Any]] = []
                sequence_findings: list[dict[str, Any]] = []
                cold_start_count = 0
                skip_update_keys: set[BaselineKey] = set(context_block_keys)
            else:
                adaptive_eval = evaluate_adaptive_rules(
                    record,
                    config,
                    state.adaptive,
                    context_block_keys=context_block_keys,
                )
                adaptive_findings = adaptive_eval.findings
                sequence_findings = adaptive_eval.sequence_findings
                cold_start_count = adaptive_eval.cold_start_count
                skip_update_keys = adaptive_eval.skip_update_keys

            evidence["context"].extend(context_findings)
            evidence["adaptive"].extend(adaptive_findings)
            evidence["sequence"].extend(sequence_findings)

            model_findings: list[dict[str, Any]] = []
            if model is not None:
                try:
                    finding, skipped_reason = model.evaluate(record)
                except Exception:
                    finding, skipped_reason = None, "inference_failed"
                    if not duplicate:
                        state.model.consecutive_failures += 1
                        dynamic_report["model_inference_failure_count"] += 1
                if finding is not None:
                    model_findings.append(finding)
                    if not duplicate:
                        state.model.valid_records += 1
                        state.model.recent_complete.append(True)
                        state.model.recent_candidates.append(bool(finding.get("candidate_flag")))
                        state.model.consecutive_failures = 0
                elif skipped_reason != "family_not_applicable":
                    model_findings.append({"rule_id": "model.autoencoder_not_evaluated", "state": "not_evaluated", "reason": skipped_reason, "model_version": model.metadata["model_version"]})
                    if not duplicate:
                        state.model.recent_complete.append(False)
            elif model_config["enabled"]:
                evidence["model"].append({"rule_id": "model.autoencoder_unavailable", "state": "not_evaluated", "reason": dynamic_report.get("model_load_failure", "load_failed")})
            evidence["model"].extend(model_findings)

            all_findings = context_findings + adaptive_findings + model_findings + sequence_findings
            event_summaries: list[dict[str, Any]] = []
            enforce_flags: set[str] = set()
            if not duplicate and state_update_eligible:
                event_summaries, enforce_flags = state.events.add_candidates(
                    record,
                    all_findings,
                    config,
                )
            _update_dynamic_report(
                dynamic_report,
                record,
                context_findings,
                adaptive_findings,
                sequence_findings,
                model_findings,
                cold_start_count,
                event_summaries,
            )

            effective_mode = mode
            if mode == "auto":
                effective_mode = update_auto_mode(state.model, config, requested_mode=mode, package_id=model.package_id if model is not None else None, model_validated=bool(model and model.validation_passed), event_enabled=config["event_confirmation"]["enabled"])
            if effective_mode == "enforce":
                if event_enforcement_policy(config) == "point":
                    enforce_flags = {
                        str(finding["candidate_flag"])
                        for finding in all_findings
                        if finding.get("candidate_flag")
                    }
                _enforce_dynamic_findings(record, all_findings, dynamic_report, enforce_flags)

            pending_updates.append((record, skip_update_keys, context_block_keys, duplicate))
            pending_context_updates.append((record, duplicate, _has_candidate_findings(all_findings)))
            if not duplicate and state_update_eligible:
                run_seen_fingerprints.add(fingerprint)

        for record, duplicate, dynamic_candidate in pending_context_updates:
            if duplicate:
                continue
            if _record_can_update_context(record, config, dynamic_candidate=dynamic_candidate):
                state.context.update_latest_valid(
                    record,
                    allow_suspect_context=config.get("allow_suspect_context", False),
                )
        for record, skip_update_keys, context_block_keys, duplicate in pending_updates:
            if duplicate:
                continue
            if not _record_can_update_state(record, config):
                continue
            update_adaptive_state(
                record,
                config,
                state.adaptive,
                skip_update_keys=skip_update_keys,
                context_block_keys=context_block_keys,
            )
            state.mark_fingerprint(
                record_fingerprint(record),
                device_id=device_id_for_record(record),
                capacity=config["deduplication_capacity"],
            )

    dynamic_report["quarantine_active_count"] = len(state.adaptive.quarantines)
    dynamic_report["effective_mode"] = state.model.effective_mode if mode == "auto" else mode
    dynamic_report["mode_transition_reason"] = state.model.mode_transition_reason
    dynamic_report["model_effective_mode"] = dynamic_report["effective_mode"]
    report.update(_finalize_dynamic_report(dynamic_report, hard_records))
    return hard_records, report, state


def _resolve_dynamic_config(dynamic_config: dict[str, Any] | str | Path | None) -> dict[str, Any]:
    if dynamic_config is None:
        return load_dynamic_filter_config()
    if isinstance(dynamic_config, (str, Path)):
        return load_dynamic_filter_config(dynamic_config)
    return validate_dynamic_filter_config(dynamic_config, source="dynamic_config")


def _record_groups(records: list[dict[str, Any]]) -> list[list[int]]:
    grouped: dict[tuple[str, tuple[int, str]], list[int]] = defaultdict(list)
    for index, record in enumerate(records):
        grouped[(device_id_for_record(record), timestamp_sort_value(record))].append(index)
    return [
        sorted(indices, key=lambda index: _stable_record_key(records[index], index))
        for _, indices in sorted(grouped.items(), key=lambda item: item[0])
    ]


def _stable_record_key(record: dict[str, Any], index: int) -> tuple[str, str, str, int]:
    return (
        str(record.get("family") or ""),
        str(record.get("record_id") or ""),
        json.dumps(record.get("payload") or {}, sort_keys=True, default=str),
        index,
    )


def _same_time_context_snapshot(
    group: list[int],
    records: list[dict[str, Any]],
    context_state: ContextState,
    config: dict[str, Any],
) -> ContextState:
    snapshot = context_state.copy()
    for index in group:
        record = records[index]
        if not _record_can_update_context(
            record,
            config,
            dynamic_candidate=_record_has_dynamic_candidate(record),
        ):
            continue
        snapshot.update_latest_valid(
            record,
            allow_suspect_context=config.get("allow_suspect_context", False),
        )
    return snapshot


def _record_can_update_state(record: dict[str, Any], config: dict[str, Any]) -> bool:
    if normalize_quality_state(record.get("quality_state")) == "invalid":
        return False
    family = str(record.get("family") or "")
    if not family_enabled(family, config):
        return False
    return record_timestamp(record) is not None


def _record_can_update_context(
    record: dict[str, Any],
    config: dict[str, Any],
    *,
    dynamic_candidate: bool,
) -> bool:
    if not _record_can_update_state(record, config):
        return False
    if dynamic_candidate and not config.get("allow_candidate_context", False):
        return False
    state = normalize_quality_state(record.get("quality_state"))
    if state == "suspect" and not config.get("allow_suspect_context", False):
        return False
    return True


def _has_candidate_findings(findings: list[dict[str, Any]]) -> bool:
    return any(finding_is_candidate(finding) for finding in findings)


def _record_has_dynamic_candidate(record: dict[str, Any]) -> bool:
    evidence = record.get("filter_evidence")
    if not isinstance(evidence, dict):
        return False
    for layer in ("context", "adaptive", "sequence"):
        values = evidence.get(layer)
        if isinstance(values, list) and _has_candidate_findings(
            [item for item in values if isinstance(item, dict)]
        ):
            return True
    return False


def _context_block_keys(record: dict[str, Any], findings: list[dict[str, Any]]) -> set[BaselineKey]:
    device_id = device_id_for_record(record)
    family = str(record.get("family") or "")
    keys: set[BaselineKey] = set()
    for finding in findings:
        affected = finding.get("affected_fields")
        if isinstance(affected, list):
            for field in affected:
                if isinstance(field, str) and field:
                    keys.add((device_id, family, field))
    return keys


def _hard_evidence(record: dict[str, Any]) -> list[dict[str, Any]]:
    evidence: list[dict[str, Any]] = []
    for flag in record.get("quality_flags", []):
        evidence.append({"rule_id": "hard.quality_flag", "flag": flag})
    state = normalize_quality_state(record.get("quality_state"))
    if state != "decoded":
        evidence.append({"rule_id": "hard.quality_state", "state": state})
    return evidence


def _new_dynamic_report(mode: str, record_count: int, hard_invalid_count: int) -> dict[str, Any]:
    return {
        "requested_mode": mode,
        "filtering_mode": mode,
        "context_layer_records_evaluated": record_count,
        "adaptive_layer_records_evaluated": record_count,
        "hard_invalid_count": hard_invalid_count,
        "context_candidate_count": 0,
        "adaptive_candidate_count": 0,
        "enforced_suspect_count": 0,
        "cold_start_count": 0,
        "timestamp_not_evaluated_count": 0,
        "duplicate_state_update_count": 0,
        "baseline_shift_count": 0,
        "quarantine_active_count": 0,
        "event_candidate_count": 0,
        "confirmed_anomaly_event_count": 0,
        "dynamic_rule_trigger_counts": {},
        "dynamic_family_trigger_counts": {},
        "dynamic_device_trigger_counts": {},
        "sequence_findings": [],
        "anomaly_events": [],
        "reporting_gap_count": 0,
        "family_silence_count": 0,
        "state_schema_version": STATE_SCHEMA_VERSION,
        "state_loaded": False,
        "state_saved": False,
        "model_records_evaluated": 0,
        "model_records_skipped": 0,
        "model_candidate_count": 0,
        "model_inference_failure_count": 0,
        "model_field_candidate_counts": {},
        "model_effective_mode": "shadow",
        "effective_mode": "shadow",
        "mode_transition_reason": "initial_shadow",
    }


def _update_dynamic_report(
    report: dict[str, Any],
    record: dict[str, Any],
    context_findings: list[dict[str, Any]],
    adaptive_findings: list[dict[str, Any]],
    sequence_findings: list[dict[str, Any]],
    model_findings: list[dict[str, Any]],
    cold_start_count: int,
    event_summaries: list[dict[str, Any]],
) -> None:
    context_candidates = [finding for finding in context_findings if finding.get("candidate_flag")]
    adaptive_candidates = [finding for finding in adaptive_findings if finding.get("candidate_flag")]
    report["context_candidate_count"] += len(context_candidates)
    report["adaptive_candidate_count"] += len(adaptive_candidates)
    report["cold_start_count"] += cold_start_count
    report["sequence_findings"].extend(sequence_findings)
    report["anomaly_events"].extend(event_summaries)
    report["event_candidate_count"] += sum(1 for finding in context_candidates + adaptive_candidates)
    report["confirmed_anomaly_event_count"] += sum(
        1 for event in event_summaries if event.get("status") == "confirmed"
    )

    for finding in context_findings + adaptive_findings + sequence_findings:
        if finding.get("rule_id") == "dynamic.timestamp_unavailable":
            report["timestamp_not_evaluated_count"] += 1
        if finding.get("rule_id") == "adaptive.baseline_shift":
            report["baseline_shift_count"] += 1

    model_candidates = [finding for finding in model_findings if finding.get("candidate_flag")]
    report["model_records_evaluated"] += sum(1 for finding in model_findings if finding.get("rule_id") == "model.autoencoder_reconstruction")
    report["model_records_skipped"] += sum(1 for finding in model_findings if finding.get("rule_id") == "model.autoencoder_not_evaluated")
    report["model_candidate_count"] += len(model_candidates)
    for finding in model_candidates:
        for field in finding.get("affected_fields", []): _increment(report["model_field_candidate_counts"], str(field))
    for finding in context_candidates + adaptive_candidates + model_candidates + sequence_findings:
        _increment(report["dynamic_rule_trigger_counts"], str(finding.get("rule_id") or "unknown"))
        family = str(finding.get("family") or record.get("family") or "unknown")
        _increment(report["dynamic_family_trigger_counts"], family)
        device_id = str(finding.get("device_id") or device_id_for_record(record))
        _increment(report["dynamic_device_trigger_counts"], device_id)
        if finding.get("candidate_flag") == "reporting_gap":
            report["reporting_gap_count"] += 1
        if finding.get("candidate_flag") == "family_silence":
            report["family_silence_count"] += 1


def _enforce_dynamic_findings(
    record: dict[str, Any],
    findings: list[dict[str, Any]],
    report: dict[str, Any],
    enforce_flags: set[str],
) -> None:
    if not enforce_flags:
        return

    old_state = normalize_quality_state(record.get("quality_state"))
    if old_state == "invalid":
        return

    allowed_flags = {
        str(finding["candidate_flag"])
        for finding in findings
        if finding.get("candidate_flag") in enforce_flags
    }
    if not allowed_flags:
        return

    existing_flags = record.get("quality_flags")
    if not isinstance(existing_flags, list):
        existing_flags = []
    record["quality_flags"] = dedupe_sorted_labels(list(existing_flags) + sorted(allowed_flags))
    record["quality_state"] = merge_quality_state(record.get("quality_state"), "suspect")
    if old_state == "decoded" and normalize_quality_state(record.get("quality_state")) == "suspect":
        report["enforced_suspect_count"] += 1


def _finalize_dynamic_report(report: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, Any]:
    finalized = dict(report)
    finalized.update(final_quality_counts(records))
    finalized["dynamic_rule_trigger_counts"] = sorted_count_dict(
        finalized["dynamic_rule_trigger_counts"]
    )
    finalized["dynamic_family_trigger_counts"] = sorted_count_dict(
        finalized["dynamic_family_trigger_counts"]
    )
    finalized["dynamic_device_trigger_counts"] = sorted_count_dict(
        finalized["dynamic_device_trigger_counts"]
    )
    finalized["model_field_candidate_counts"] = sorted_count_dict(finalized["model_field_candidate_counts"])
    finalized["sequence_findings"] = sorted(
        finalized["sequence_findings"],
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )
    finalized["anomaly_events"] = sorted(
        finalized["anomaly_events"],
        key=lambda item: json.dumps(item, sort_keys=True, default=str),
    )
    return finalized


def _increment(counts: dict[str, int], key: str) -> None:
    counts[key] = int(counts.get(key, 0)) + 1
