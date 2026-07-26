from gadgetbridge_pipeline.defs.sensors.s3_watch import (
    evaluate_object_change,
    parse_json_cursor,
    plan_prefix_run_requests,
)


# parse_json_cursor(cursor_str) — tolerant JSON parsing of the sensor cursor.

def test_parse_json_cursor_none_is_empty():
    assert parse_json_cursor(None) == {}


def test_parse_json_cursor_empty_string_is_empty():
    assert parse_json_cursor("") == {}


def test_parse_json_cursor_parses_json():
    assert parse_json_cursor('{"etags": {"a": "1"}}') == {"etags": {"a": "1"}}


def test_parse_json_cursor_bad_json_is_empty():
    assert parse_json_cursor("not-json") == {}


# evaluate_object_change(name, current_etags, cursor) — pure decision logic
# shared by every HEAD-based watch sensor.

def test_evaluate_object_change_skips_when_unchanged():
    etags = {"a": "etag1"}
    result = evaluate_object_change("some_sensor", etags, {"etags": etags})
    assert result["action"] == "skip"


def test_evaluate_object_change_runs_on_first_ever_evaluation():
    result = evaluate_object_change("some_sensor", {"a": "etag1"}, {})
    assert result["action"] == "run"


def test_evaluate_object_change_runs_when_changed():
    previous = {"etags": {"a": "etag1"}}
    current = {"a": "etag2"}
    result = evaluate_object_change("some_sensor", current, previous)
    assert result["action"] == "run"
    assert result["new_cursor"] == {"etags": current}


def test_evaluate_object_change_run_key_is_bare_etag_for_single_key():
    result = evaluate_object_change("some_sensor", {"a": "etag2"}, {})
    assert result["run_key"] == "etag2"


def test_evaluate_object_change_run_key_combines_sorted_etags_for_multiple_keys():
    result = evaluate_object_change("some_sensor", {"skips": "b", "prescriptions": "a"}, {})
    assert result["run_key"] == "a-b"


def test_evaluate_object_change_tags_triggered_by_the_given_name():
    result = evaluate_object_change("some_sensor", {"a": "etag1"}, {})
    assert result["tags"] == {"triggered_by": "some_sensor"}


# plan_prefix_run_requests(current, previous, group_key_fn, partition_key_fn)
# — pure decision logic shared by every LIST-based watch sensor.

def test_plan_prefix_default_none_when_nothing_changed():
    files = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert plan_prefix_run_requests(files, files) == []


def test_plan_prefix_default_triggers_new_file():
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    result = plan_prefix_run_requests(current, {})
    assert len(result) == 1
    assert "run_key" in result[0]
    assert "partition_key" not in result[0]


def test_plan_prefix_default_triggers_changed_etag():
    previous = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag2"}
    assert len(plan_prefix_run_requests(current, previous)) == 1


def test_plan_prefix_default_triggers_removed_file():
    previous = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert len(plan_prefix_run_requests({}, previous)) == 1


def test_plan_prefix_default_run_key_differs_for_different_etags():
    current_a = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    current_b = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag2"}
    run_key_a = plan_prefix_run_requests(current_a, {})[0]["run_key"]
    run_key_b = plan_prefix_run_requests(current_b, {})[0]["run_key"]
    assert run_key_a != run_key_b


def test_plan_prefix_default_run_key_stable_for_same_input():
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert (
        plan_prefix_run_requests(current, {})[0]["run_key"]
        == plan_prefix_run_requests(current, {})[0]["run_key"]
    )


def _month_from_key(key: str) -> str:
    return key.split("/")[-1].removesuffix(".rec")


def _partition_key(year_month: str) -> str:
    return f"{year_month}-01"


def test_plan_prefix_grouped_empty_when_nothing_changed():
    files = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    assert plan_prefix_run_requests(files, files, _month_from_key, _partition_key) == []


def test_plan_prefix_grouped_triggers_new_month():
    current = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    result = plan_prefix_run_requests(current, {}, _month_from_key, _partition_key)
    assert len(result) == 1
    assert result[0]["group"] == "2026-07"
    assert result[0]["partition_key"] == "2026-07-01"
    assert result[0]["run_key"].startswith("2026-07-01::")


def test_plan_prefix_grouped_triggers_changed_etag():
    previous = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    current = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag2"}
    result = plan_prefix_run_requests(current, previous, _month_from_key, _partition_key)
    assert [r["group"] for r in result] == ["2026-07"]


def test_plan_prefix_grouped_ignores_unaffected_group():
    previous = {
        "owntracks/raw/rec/alice/phone/2026-06.rec": "etag-june",
        "owntracks/raw/rec/alice/phone/2026-07.rec": "etag1",
    }
    current = dict(previous, **{"owntracks/raw/rec/alice/phone/2026-07.rec": "etag2"})
    result = plan_prefix_run_requests(current, previous, _month_from_key, _partition_key)
    assert [r["group"] for r in result] == ["2026-07"]


def test_plan_prefix_grouped_detects_fully_removed_group():
    previous = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    result = plan_prefix_run_requests({}, previous, _month_from_key, _partition_key)
    assert [r["group"] for r in result] == ["2026-07"]


def test_plan_prefix_grouped_sorted_and_multiple_groups():
    current = {
        "owntracks/raw/rec/alice/phone/2026-08.rec": "a",
        "owntracks/raw/rec/alice/phone/2026-07.rec": "b",
    }
    result = plan_prefix_run_requests(current, {}, _month_from_key, _partition_key)
    assert [r["group"] for r in result] == ["2026-07", "2026-08"]
