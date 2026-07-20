from gadgetbridge_pipeline.defs.sensors.medicine_s3_sensor import (
    parse_cursor,
    evaluate_change,
)


# parse_cursor(cursor_str) — tolerant JSON parsing of the sensor cursor.

def test_parse_cursor_none_is_empty():
    assert parse_cursor(None) == {}


def test_parse_cursor_parses_json():
    assert parse_cursor('{"etags": {"prescriptions": "a"}}') == {
        "etags": {"prescriptions": "a"}
    }


def test_parse_cursor_bad_json_is_empty():
    assert parse_cursor("not-json") == {}


# evaluate_change(current_etags, cursor) — pure decision logic for whether
# the sensor should skip or request a run.

def test_evaluate_change_skips_when_etags_unchanged():
    etags = {"prescriptions": "a", "skips": "b"}
    result = evaluate_change(etags, {"etags": etags})
    assert result["action"] == "skip"


def test_evaluate_change_runs_on_first_ever_evaluation():
    etags = {"prescriptions": "a", "skips": "b"}
    result = evaluate_change(etags, {})
    assert result["action"] == "run"


def test_evaluate_change_runs_when_one_etag_changes():
    previous = {"etags": {"prescriptions": "a", "skips": "b"}}
    current = {"prescriptions": "a", "skips": "c"}
    result = evaluate_change(current, previous)
    assert result["action"] == "run"
    assert result["new_cursor"] == {"etags": current}


def test_evaluate_change_run_key_combines_sorted_etags():
    current = {"skips": "b", "prescriptions": "a"}
    result = evaluate_change(current, {})
    assert result["run_key"] == "a-b"


def test_evaluate_change_tags_triggered_by():
    result = evaluate_change({"prescriptions": "a", "skips": "b"}, {})
    assert result["tags"] == {"triggered_by": "medicine_s3_sensor"}
