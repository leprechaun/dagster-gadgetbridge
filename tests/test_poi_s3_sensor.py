from gadgetbridge_pipeline.defs.sensors.poi_s3_sensor import parse_cursor, evaluate_change


# parse_cursor(cursor_str) — tolerant JSON parsing of the sensor cursor.

def test_parse_cursor_none_is_empty():
    assert parse_cursor(None) == {}


def test_parse_cursor_empty_string_is_empty():
    assert parse_cursor("") == {}


def test_parse_cursor_parses_json():
    assert parse_cursor('{"etag": "abc123"}') == {"etag": "abc123"}


def test_parse_cursor_ignores_bad_json():
    assert parse_cursor("not-json") == {}


# evaluate_change(current_etag, cursor) — pure decision logic for whether
# the sensor should skip or request a run.

def test_evaluate_change_skips_when_etag_unchanged():
    result = evaluate_change("etag1", {"etag": "etag1"})
    assert result["action"] == "skip"
    assert "etag1" in result["reason"]


def test_evaluate_change_runs_on_first_ever_evaluation():
    result = evaluate_change("etag1", {})
    assert result["action"] == "run"


def test_evaluate_change_runs_when_etag_changed():
    result = evaluate_change("etag2", {"etag": "etag1"})
    assert result["action"] == "run"
    assert result["run_key"] == "etag2"
    assert result["new_cursor"] == {"etag": "etag2"}
    assert result["tags"] == {"triggered_by": "poi_s3_sensor"}
