import json
from pathlib import Path

from filter_rules import filter_decoded_records
from smart_sleeper_source import (
    decode_smart_sleeper_rows,
    fetch_smart_sleeper_json,
    load_smart_sleeper_json,
    rows_from_smart_sleeper_export,
)


DATA_PATH = Path(__file__).parents[1] / "data" / "smart_sleeper_data.json"


def test_real_smart_sleeper_export_runs_through_filter():
    rows = rows_from_smart_sleeper_export(load_smart_sleeper_json(DATA_PATH))
    records = decode_smart_sleeper_rows(rows)
    cleaned, report = filter_decoded_records(records)

    assert len(rows) == 155
    assert len(records) == len(cleaned) == 32
    assert all(record["family"] == "environment" for record in cleaned)
    assert all(record["source_type"] == "smart_sleeper" for record in cleaned)
    assert all(isinstance(record["payload"]["rtd1_t_x100"], int) for record in cleaned)
    assert all(record["payload"]["year"] is None for record in cleaned)
    assert report["environment_records"] == 32


def test_record_id_is_stable_across_fetch_batches():
    rows = rows_from_smart_sleeper_export(load_smart_sleeper_json(DATA_PATH))
    record = decode_smart_sleeper_rows(rows)[0]
    repeated = decode_smart_sleeper_rows(list(reversed(rows)))[-1]

    assert record["record_id"] == repeated["record_id"]
    assert record["record_id"].startswith("smart_sleeper:")


def test_fetch_uses_basic_auth_without_putting_credentials_in_url(monkeypatch):
    payload = {"results": []}
    seen = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self):
            return json.dumps(payload).encode("utf-8")

    def fake_urlopen(request, timeout):
        seen["url"] = request.full_url
        seen["headers"] = dict(request.header_items())
        seen["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setenv("SMART_SLEEPER_USERNAME", "test-user")
    monkeypatch.setenv("SMART_SLEEPER_PASSWORD", "test-password")
    monkeypatch.setattr("urllib.request.urlopen", fake_urlopen)

    assert fetch_smart_sleeper_json(source_url="https://example.test/influx/json") == payload
    assert seen["url"] == "https://example.test/influx/json"
    assert seen["headers"]["Authorization"].startswith("Basic ")
    assert "test-password" not in seen["url"]
