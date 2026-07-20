from gadgetbridge_pipeline.defs.sensors.owntracks_s3_sensor import (
    _month_from_key,
    _partition_key,
    plan_run_requests,
)


def test_month_from_key_extracts_year_month():
    assert _month_from_key("owntracks/raw/rec/alice/phone/2026-07.rec") == "2026-07"


def test_month_from_key_handles_bare_filename():
    assert _month_from_key("2026-01.rec") == "2026-01"


def test_partition_key_appends_day_one():
    assert _partition_key("2026-07") == "2026-07-01"


# plan_run_requests(current, previous) — pure decision logic for which
# monthly partitions need a run, given current vs. previously-seen etags.
# Each result is {"partition_key", "month", "run_key"}; run_key is a stable
# hash of that month's file/etag pairs, prefixed with the partition key.

def test_plan_run_requests_empty_when_nothing_changed():
    files = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    assert plan_run_requests(files, files) == []


def test_plan_run_requests_triggers_new_month():
    current = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    result = plan_run_requests(current, {})
    assert len(result) == 1
    assert result[0]["partition_key"] == "2026-07-01"
    assert result[0]["month"] == "2026-07"
    assert result[0]["run_key"].startswith("2026-07-01::")


def test_plan_run_requests_triggers_changed_etag():
    previous = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    current = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag2"}
    result = plan_run_requests(current, previous)
    assert [r["month"] for r in result] == ["2026-07"]


def test_plan_run_requests_ignores_unaffected_month():
    previous = {
        "owntracks/raw/rec/alice/phone/2026-06.rec": "etag-june",
        "owntracks/raw/rec/alice/phone/2026-07.rec": "etag1",
    }
    current = dict(previous, **{"owntracks/raw/rec/alice/phone/2026-07.rec": "etag2"})
    result = plan_run_requests(current, previous)
    assert [r["month"] for r in result] == ["2026-07"]


def test_plan_run_requests_sorted_and_multiple_months():
    current = {
        "owntracks/raw/rec/alice/phone/2026-08.rec": "a",
        "owntracks/raw/rec/alice/phone/2026-07.rec": "b",
    }
    result = plan_run_requests(current, {})
    assert [r["month"] for r in result] == ["2026-07", "2026-08"]


def test_plan_run_requests_run_key_differs_for_different_etags():
    current_a = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    current_b = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag2"}
    run_key_a = plan_run_requests(current_a, {})[0]["run_key"]
    run_key_b = plan_run_requests(current_b, {})[0]["run_key"]
    assert run_key_a != run_key_b


def test_plan_run_requests_run_key_stable_for_same_input():
    current = {"owntracks/raw/rec/alice/phone/2026-07.rec": "etag1"}
    assert (
        plan_run_requests(current, {})[0]["run_key"]
        == plan_run_requests(current, {})[0]["run_key"]
    )
