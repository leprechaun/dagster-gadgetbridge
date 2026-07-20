from gadgetbridge_pipeline.defs.sensors.s3_sensor import parse_cursor, evaluate_change


# parse_cursor(cursor_str) — tolerant JSON parsing of the sensor cursor.

def test_parse_cursor_none_is_empty():
    assert parse_cursor(None) == {}


def test_parse_cursor_empty_string_is_empty():
    assert parse_cursor("") == {}


def test_parse_cursor_parses_json():
    assert parse_cursor('{"etag": "abc123"}') == {"etag": "abc123"}


def test_parse_cursor_falls_back_to_raw_etag_on_bad_json():
    assert parse_cursor("not-json-just-an-old-etag") == {"etag": "not-json-just-an-old-etag"}


# evaluate_change(current_etag, last_modified, cursor) — pure decision logic
# for whether the sensor should skip or request a run.

def test_evaluate_change_skips_when_etag_unchanged():
    result = evaluate_change("etag1", "2026-07-17T00:00:00", {"etag": "etag1"})
    assert result["action"] == "skip"
    assert "etag1" in result["reason"]


def test_evaluate_change_runs_on_first_ever_evaluation():
    # empty cursor (no prior etag recorded) should always trigger a run
    result = evaluate_change("etag1", "2026-07-17T00:00:00", {})
    assert result["action"] == "run"


def test_evaluate_change_runs_when_etag_changed():
    result = evaluate_change("etag2", "2026-07-17T00:00:00", {"etag": "etag1"})
    assert result["action"] == "run"
    assert result["run_key"] == "etag2"
    assert result["new_cursor"] == {"etag": "etag2", "last_modified": "2026-07-17T00:00:00"}
    assert result["tags"] == {
        "s3_etag": "etag2",
        "s3_last_modified": "2026-07-17T00:00:00",
        "triggered_by": "s3_sensor",
    }
