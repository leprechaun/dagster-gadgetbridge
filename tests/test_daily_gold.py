from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.daily import (
    build_derived_daily_health_snapshot,
    build_derived_sleep_schedule,
    build_derived_sleep_score_daily,
    build_derived_steps_daily,
)

_UTC = timezone.utc
_D1, _D2, _D3 = date(2000, 1, 1), date(2000, 1, 2), date(2000, 1, 3)  # Sat, Sun, Mon


def _activity(*rows):
    """rows: (person, date, minute_of_day, steps, heart_rate)"""
    return pl.DataFrame(
        [
            {"person_key": p, "date_key": d, "time_key": t, "steps": s, "heart_rate_bpm": hr,
             "timestamp_utc": datetime(d.year, d.month, d.day, tzinfo=_UTC)}
            for p, d, t, s, hr in rows
        ],
        schema={"person_key": pl.String, "date_key": pl.Date, "time_key": pl.Int64,
                "steps": pl.Int64, "heart_rate_bpm": pl.Int64,
                "timestamp_utc": pl.Datetime("us", "UTC")},
    )


def _coverage(*rows):
    """rows: (person, date, covered_date, covered_night_window)"""
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "date_key": pl.Date,
                "covered_date": pl.Boolean, "covered_night_window": pl.Boolean},
        orient="row",
    )


def test_steps_are_summed_per_person_and_local_date_on_covered_dates_only():
    activity = _activity(
        ("person-a", _D1, 0, 10, 60), ("person-a", _D1, 1, 5, 60), ("person-a", _D1, 2, None, 60),
        ("person-a", _D2, 0, 7, 60),            # uncovered date: dropped
        ("person-b", _D1, 0, 100, 60),          # another Person: never pooled
    )
    coverage = _coverage(
        ("person-a", _D1, True, True), ("person-a", _D2, False, True),
        ("person-b", _D1, True, True),
    )

    out = build_derived_steps_daily(activity, coverage).sort("person_key", "date_key")

    assert out.columns == ["person_key", "date_key", "steps", "weekday", "is_weekend"]
    assert out.rows() == [
        ("person-a", _D1, 15, 6, True),
        ("person-b", _D1, 100, 6, True),
    ]


def _samples(column, *rows):
    """rows: (person, date, value)"""
    return pl.DataFrame(
        [{"person_key": p, "date_key": d, column: v} for p, d, v in rows],
        schema={"person_key": pl.String, "date_key": pl.Date, column: pl.Float64},
    )


def test_snapshot_means_keep_their_counts_and_heart_rate_stats_ignore_no_reading_nulls():
    activity = _activity(
        ("person-a", _D1, 0, 1, 60), ("person-a", _D1, 1, 1, 70), ("person-a", _D1, 2, 1, 80),
        ("person-a", _D1, 3, 1, None),       # a nulled 255: neither min/max nor count
    )
    sources = {
        "hrv": (_samples("hrv_ms", ("person-a", _D1, 40.0), ("person-a", _D1, 50.0)), "hrv_ms"),
        "stress": (_samples("stress_score"), "stress_score"),   # no readings at all
    }
    coverage = _coverage(("person-a", _D1, True, True), ("person-a", _D2, False, True))

    out = build_derived_daily_health_snapshot(activity, sources, coverage).row(0, named=True)

    assert out["date_key"] == _D1
    assert (out["avg_hrv"], out["hrv_count"]) == (45.0, 2)
    assert (out["avg_stress"], out["stress_count"]) == (None, 0)
    assert (out["heart_rate_min"], out["heart_rate_max"], out["heart_rate_median"]) == (60, 80, 70)
    assert out["heart_rate_count"] == 3


def _sleep(*rows):
    """rows: (person, reporting_date, is_nap, start_minutes, end_minutes, score)"""
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "reporting_date_key": pl.Date, "is_nap": pl.Boolean,
                "start_minutes_from_reporting_midnight": pl.Int64,
                "end_minutes_from_reporting_midnight": pl.Int64, "score": pl.Int64},
        orient="row",
    )


def test_the_sleep_schedule_is_the_nights_signed_bedtime_and_wake_on_covered_night_windows():
    sessions = _sleep(
        ("person-a", _D2, False, -120, 60, 80),     # 22:00 the night before to 01:00
        ("person-a", _D2, False, 120, 420, 70),     # 02:00 to 07:00: same Night
        ("person-a", _D2, True, 990, 1030, 50),     # a nap: not in the schedule
        ("person-a", _D3, False, -60, 400, 90),     # uncovered night window: dropped
    )
    coverage = _coverage(("person-a", _D2, True, True), ("person-a", _D3, True, False))

    out = build_derived_sleep_schedule(sessions, coverage)

    assert out.columns == [
        "person_key", "reporting_date_key", "bedtime_minutes", "wake_minutes", "is_weekend",
    ]
    assert out.rows() == [("person-a", _D2, -120, 420, True)]   # Jan 2 is a Sunday


def test_score_stats_keep_sum_and_count_and_split_nights_from_naps():
    sessions = _sleep(
        ("person-a", _D2, False, -120, 60, 80),
        ("person-a", _D2, False, 120, 420, 60),
        ("person-a", _D2, True, 990, 1030, 100),
        ("person-b", _D2, False, -60, 400, 10),
    )
    coverage = _coverage(("person-a", _D2, True, True), ("person-b", _D2, True, True))

    out = build_derived_sleep_score_daily(sessions, coverage).sort("person_key")

    assert out.columns == [
        "person_key", "reporting_date_key", "score_sum", "score_count", "mean_score",
        "max_score", "night_session_count", "nap_count",
    ]
    assert out.row(0) == ("person-a", _D2, 240, 3, 80.0, 100, 2, 1)
    assert out.row(1)[0] == "person-b"
