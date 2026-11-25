from datetime import date, datetime, timedelta, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.inferred_sleep import (
    build_inferred_nights,
    find_asleep_stretches,
)

_UTC = timezone.utc


def _minutes(*rows):
    """rows: (person_key, timestamp, activity_kind_key)"""
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "timestamp_utc": pl.Datetime("us", "UTC"),
                "activity_kind_key": pl.Int64},
        orient="row",
    )


def _t(d, h, m=0):
    return datetime(2000, 1, d, h, m, tzinfo=_UTC)


def _run(person, start, count, kind=120):
    return [(person, start + timedelta(minutes=i), kind) for i in range(count)]


def test_contiguous_asleep_minutes_become_one_stretch_ending_after_the_last_minute():
    rows = _run("person-a", _t(1, 23), 3)

    out = find_asleep_stretches(_minutes(*rows))

    assert out.row(0, named=True) == {
        "person_key": "person-a", "start_utc": _t(1, 23), "end_utc": _t(1, 23, 3),
        "asleep_minutes": 3,
    }


def test_an_awake_minute_or_a_missing_minute_splits_a_stretch_and_duplicate_minutes_count_once():
    rows = (
        _run("person-a", _t(1, 23), 2)
        + _run("person-a", _t(1, 23), 2)                 # same minutes from a second device
        + _run("person-a", _t(1, 23, 2), 1, kind=1)      # awake minute
        + _run("person-a", _t(1, 23, 3), 2)
        + _run("person-a", _t(1, 23, 6), 1)              # minute 23:05 is missing
        + _run("person-b", _t(1, 23), 1)
    )

    out = find_asleep_stretches(_minutes(*rows))

    assert out.select("person_key", "start_utc", "asleep_minutes").rows() == [
        ("person-a", _t(1, 23), 2),
        ("person-a", _t(1, 23, 3), 2),
        ("person-a", _t(1, 23, 6), 1),
        ("person-b", _t(1, 23), 1),
    ]


# build_inferred_nights

_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_TZ = pl.DataFrame(
    [(p, "UTC", _EPOCH, None) for p in ("person-a", "person-b")],
    schema={"person_key": pl.String, "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    orient="row",
)
_NO_SESSIONS = pl.DataFrame(
    schema={"person_key": pl.String, "start_utc": pl.Datetime("us", "UTC"),
            "end_utc": pl.Datetime("us", "UTC")}
)


def _stretches(*rows):
    """rows: (person, start, end) with asleep_minutes the full span"""
    return pl.DataFrame(
        [(p, s, e, int((e - s).total_seconds() // 60)) for p, s, e in rows],
        schema={"person_key": pl.String, "start_utc": pl.Datetime("us", "UTC"),
                "end_utc": pl.Datetime("us", "UTC"), "asleep_minutes": pl.Int64},
        orient="row",
    )


def test_stretches_with_a_short_waking_merge_into_one_inferred_night_with_no_score():
    stretches = _stretches(
        ("person-a", _t(1, 22), _t(2, 1)),
        ("person-a", _t(2, 2), _t(2, 7)),  # 1 h waking between
    )

    out = build_inferred_nights(stretches, _NO_SESSIONS, _TZ)

    assert out.row(0, named=True) == {
        "person_key": "person-a", "reporting_date_key": date(2000, 1, 2), "source": "inferred",
        "bedtime_utc": _t(1, 22), "wake_utc": _t(2, 7), "time_in_bed_minutes": 540,
        "total_sleep_minutes": 480, "score": None, "session_count": 0,
    }


def test_an_inferred_nap_is_dropped_and_does_not_add_to_the_inferred_night():
    stretches = _stretches(
        ("person-a", _t(1, 22), _t(2, 7)),
        ("person-a", _t(2, 14), _t(2, 14, 40)),
    )

    out = build_inferred_nights(stretches, _NO_SESSIONS, _TZ)

    assert out["total_sleep_minutes"].to_list() == [540]


def _sessions(*rows):
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "start_utc": pl.Datetime("us", "UTC"),
                "end_utc": pl.Datetime("us", "UTC")},
        orient="row",
    )


def test_still_minutes_next_to_real_sessions_neither_bridge_a_nap_and_a_night_nor_make_a_night():
    # A real night and a real afternoon nap, with 'asleep' minutes on the sofa between them.
    real = _sessions(
        ("person-a", _t(1, 22), _t(2, 7)),
        ("person-a", _t(2, 14), _t(2, 14, 40)),
    )
    sofa = _stretches(("person-a", _t(2, 9), _t(2, 12)))

    assert build_inferred_nights(sofa, real, _TZ).height == 0


def test_a_window_with_no_session_still_gets_its_inferred_night_beside_one_that_has_sessions():
    real = _sessions(("person-a", _t(1, 22), _t(2, 7)))              # Jan 2's window
    stretches = _stretches(
        ("person-a", _t(2, 9), _t(2, 12)),                           # Jan 2's window: ignored
        ("person-a", _t(2, 23), _t(3, 6)),                           # Jan 3's window: inferred
    )

    out = build_inferred_nights(stretches, real, _TZ)

    assert out["reporting_date_key"].to_list() == [date(2000, 1, 3)]


def test_no_asleep_minutes_at_all_yields_an_empty_result_with_the_derived_night_columns():
    empty = find_asleep_stretches(_minutes())

    out = build_inferred_nights(empty, _NO_SESSIONS, _TZ)

    assert empty.height == 0
    assert out.height == 0
    assert out.columns == [
        "person_key", "reporting_date_key", "source", "bedtime_utc", "wake_utc",
        "time_in_bed_minutes", "total_sleep_minutes", "score", "session_count",
    ]
