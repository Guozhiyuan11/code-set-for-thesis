import io
import json
import urllib.error

import pytest

from filters.state import DynamicState
from supabase_uploader import (
    SupabaseIngestError,
    build_ingest_payload,
    canonical_config_hash,
    deterministic_run_key,
    prepare_records_for_upload,
    upload_dynamic,
)

EXECUTION_SLOT = "2026-03-01T00:00:00+00:00"


def _record(record_id="r1", timestamp="2026-03-01T00:00:00Z"):
    return {
        "record_id": record_id,
        "family": "environment",
        "timestamp": timestamp,
        "source_type": "smart_sleeper",
        "quality_state": "decoded",
        "quality_flags": [],
        "analysis_tags": [],
        "payload": {"temperature_c": 20.0},
        "platform_meta": {},
        "raw_unmapped": {},
        "filter_evidence": {"hard": [], "context": [], "adaptive": [], "sequence": []},
    }


def test_config_hash_and_run_key_are_stable_and_sensitive_to_config():
    left = {"b": [2, 1], "a": {"x": True}}
    right = {"a": {"x": True}, "b": [2, 1]}
    changed = {"a": {"x": False}, "b": [2, 1]}

    assert canonical_config_hash(left) == canonical_config_hash(right)
    assert canonical_config_hash(left) != canonical_config_hash(changed)

    kwargs = {
        "source_type": "smart_sleeper",
        "source_file_name": "source.json",
        "pipeline_version": "dynamic-v2",
        "filtering_mode": "shadow",
        "dynamic_config_hash": canonical_config_hash(left),
        "record_fingerprints": ["b", "a"],
        "state_key": "state",
        "execution_slot": EXECUTION_SLOT,
    }
    first = deterministic_run_key(**kwargs)
    second = deterministic_run_key(**{**kwargs, "record_fingerprints": ["a", "b"]})
    third = deterministic_run_key(**{**kwargs, "dynamic_config_hash": canonical_config_hash(changed)})
    next_slot = deterministic_run_key(
        **{**kwargs, "execution_slot": "2026-03-01T00:15:00+00:00"}
    )

    assert first == second
    assert first != third
    assert first != next_slot


def test_prepare_records_rejects_missing_timestamp():
    with pytest.raises(SupabaseIngestError, match="canonical timestamp"):
        prepare_records_for_upload([_record(timestamp="not-a-time")], source_type="smart_sleeper")


def test_build_ingest_payload_contains_rpc_contract_fields():
    payload = build_ingest_payload(
        records=[_record()],
        quality_report={"anomaly_events": []},
        candidate_state=DynamicState.empty(),
        source_file_name="source.json",
        filtering_mode="shadow",
        pipeline_version="dynamic-v2",
        state_key="state",
        execution_slot=EXECUTION_SLOT,
        dynamic_config={"enabled": True},
    )

    assert payload["run"]["run_key"].startswith("dynamic-v2:")
    assert payload["run"]["state_schema_version"] == 3
    assert payload["run"]["execution_slot"] == EXECUTION_SLOT
    assert payload["records"][0]["record_fingerprint"]
    assert payload["state"]["state_key"] == "state"


def test_run_key_stays_stable_when_known_records_are_omitted_from_upload():
    logical_fingerprints = ["logical-r1"]
    common = {
        "quality_report": {"anomaly_events": []},
        "candidate_state": DynamicState.empty(),
        "source_file_name": "source.json",
        "filtering_mode": "shadow",
        "pipeline_version": "dynamic-v2",
        "state_key": "state",
        "execution_slot": EXECUTION_SLOT,
        "run_record_fingerprints": logical_fingerprints,
    }

    first = build_ingest_payload(records=[_record()], **common)
    retry = build_ingest_payload(records=[], **common)

    assert first["run"]["run_key"] == retry["run"]["run_key"]
    assert retry["records"] == []


def test_upload_uses_rpc_returned_counts_and_apikey_header(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SECRET_KEY", "sb_secret_test")
    seen = {}

    class FakeResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(
                {
                    "success": True,
                    "ingest_run_id": "00000000-0000-0000-0000-000000000001",
                    "run_key": seen["body"]["run"]["run_key"],
                    "inserted_records": 0,
                    "updated_records": 1,
                    "skipped_records": 0,
                    "anomaly_events": 0,
                    "state_saved": True,
                }
            ).encode("utf-8")

    def fake_urlopen(request, timeout):
        seen["headers"] = dict(request.header_items())
        seen["body"] = json.loads(request.data.decode("utf-8"))
        seen["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    result = upload_dynamic(
        records=[_record()],
        quality_report={"anomaly_events": []},
        candidate_state=DynamicState.empty(),
        source_file_name="source.json",
        filtering_mode="shadow",
        pipeline_version="dynamic-v2",
        state_key="state",
        execution_slot=EXECUTION_SLOT,
        dynamic_config={"enabled": True},
        timeout_seconds=3.0,
    )

    assert seen["headers"]["Apikey"] == "sb_secret_test"
    assert seen["body"]["records"][0]["record_id"] == "r1"
    assert seen["timeout"] == 3.0
    assert result["inserted_records"] == 0
    assert result["updated_records"] == 1


def test_upload_rejects_missing_env_without_leaking_secret(monkeypatch):
    monkeypatch.delenv("SUPABASE_URL", raising=False)
    monkeypatch.delenv("SUPABASE_SECRET_KEY", raising=False)

    with pytest.raises(SupabaseIngestError, match="SUPABASE_SECRET_KEY"):
        upload_dynamic(
            records=[_record()],
            quality_report={"anomaly_events": []},
            candidate_state=DynamicState.empty(),
            source_file_name="source.json",
            filtering_mode="shadow",
            pipeline_version="dynamic-v2",
            state_key="state",
            execution_slot=EXECUTION_SLOT,
        )


def test_upload_rejects_non_json_and_http_errors(monkeypatch):
    monkeypatch.setenv("SUPABASE_URL", "https://example.supabase.co")
    monkeypatch.setenv("SUPABASE_SECRET_KEY", "sb_secret_should_not_print")

    class NonJsonResponse:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return b"not json"

    monkeypatch.setattr("urllib.request.urlopen", lambda request, timeout: NonJsonResponse())
    with pytest.raises(SupabaseIngestError, match="non-JSON"):
        upload_dynamic(
            records=[_record()],
            quality_report={"anomaly_events": []},
            candidate_state=DynamicState.empty(),
            source_file_name="source.json",
            filtering_mode="shadow",
            pipeline_version="dynamic-v2",
            state_key="state",
            execution_slot=EXECUTION_SLOT,
        )

    def raise_http_error(request, timeout):
        raise urllib.error.HTTPError(
            request.full_url,
            500,
            "server error",
            hdrs={},
            fp=io.BytesIO(b'{"error":"failed","secret":"hidden"}'),
        )

    monkeypatch.setattr("urllib.request.urlopen", raise_http_error)
    with pytest.raises(SupabaseIngestError) as excinfo:
        upload_dynamic(
            records=[_record()],
            quality_report={"anomaly_events": []},
            candidate_state=DynamicState.empty(),
            source_file_name="source.json",
            filtering_mode="shadow",
            pipeline_version="dynamic-v2",
            state_key="state",
            execution_slot=EXECUTION_SLOT,
        )

    assert "sb_secret" not in str(excinfo.value)
    assert "hidden" not in str(excinfo.value)
