from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.night_table import build_derived_night

_UTC = timezone.utc
_D1, _D2 = date(2000, 1, 1), date(2000, 1, 2)


def _dt(d, h, m=0):
    return datetime(2000, 1, d, h, m, tzinfo=_UTC)


def _sessions(*rows):
    """rows: (session_key, person, reporting_date, is_nap, start, end, score)"""
    return pl.DataFrame(
        [
            {
                "session_key": k, "person_key": p, "reporting_date_key": d, "is_nap": nap,
                "start_utc": s, "end_utc": e, "score": score,
                "length_minutes": int((e - s).total_seconds() // 60),
            }
            for k, p, d, nap, s, e, score in rows
        ],
        schema={
            "session_key": pl.Int64, "person_key": pl.String, "reporting_date_key": pl.Date,
            "is_nap": pl.Boolean, "start_utc": pl.Datetime("us", "UTC"),
            "end_utc": pl.Datetime("us", "UTC"), "score": pl.Int64, "length_minutes": pl.Int64,
        },
    )


def _stages(*rows):
    """rows: (session_key, stage, minutes)"""
    return pl.DataFrame(
        list(rows),
        schema={"session_key": pl.Int64, "sleep_stage_key": pl.String,
                "duration_minutes": pl.Int64},
        orient="row",
    )


def _coverage(*rows):
    """rows: (person, date, covered_night_window)"""
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "date_key": pl.Date, "covered_night_window": pl.Boolean},
        orient="row",
    )


def test_a_night_reports_its_block_span_and_counts_only_non_awake_stage_minutes_as_sleep():
    sessions = _sessions((1, "person-a", _D2, False, _dt(1, 22), _dt(2, 7), 80))
    stages = _stages((1, "LIGHT", 300), (1, "AWAKE", 30), (1, "DEEP", 150))

    out = build_derived_night(sessions, stages, _coverage(("person-a", _D2, True)))

    assert out.row(0, named=True) == {
        "person_key": "person-a", "reporting_date_key": _D2, "source": "session",
        "bedtime_utc": _dt(1, 22), "wake_utc": _dt(2, 7), "time_in_bed_minutes": 540,
        "total_sleep_minutes": 450, "score": 80, "session_count": 1,
    }


def test_a_gap_between_two_sessions_counts_toward_time_in_bed_but_not_toward_sleep():
    sessions = _sessions(
        (1, "person-a", _D2, False, _dt(1, 22), _dt(2, 1), 80),
        (2, "person-a", _D2, False, _dt(2, 2, 30), _dt(2, 7), 70),  # 90 min waking between
    )
    stages = _stages((1, "LIGHT", 180), (2, "LIGHT", 270))

    covered = _coverage(("person-a", _D2, True))
    out = build_derived_night(sessions, stages, covered).row(0, named=True)

    assert out["time_in_bed_minutes"] == 540
    assert out["total_sleep_minutes"] == 450
    assert out["session_count"] == 2


def test_two_sessions_in_one_night_report_the_longest_sessions_score_not_an_average_or_first():
    sessions = _sessions(
        (1, "person-a", _D2, False, _dt(1, 22), _dt(2, 0), 90),   # 2 h
        (2, "person-a", _D2, False, _dt(2, 1), _dt(2, 7), 60),    # 6 h, the longest
    )

    out = build_derived_night(sessions, _stages(), _coverage(("person-a", _D2, True)))

    assert out["score"].to_list() == [60]


def test_a_covered_date_with_no_sleep_gets_a_none_row_with_zero_minutes():
    sessions = _sessions((1, "person-a", _D2, False, _dt(1, 22), _dt(2, 7), 80))
    d3 = date(2000, 1, 3)
    coverage = _coverage(("person-a", _D2, True), ("person-a", d3, True))

    out = build_derived_night(sessions, _stages(), coverage).sort("reporting_date_key")

    assert out.row(1, named=True) == {
        "person_key": "person-a", "reporting_date_key": d3, "source": "none",
        "bedtime_utc": None, "wake_utc": None, "time_in_bed_minutes": 0,
        "total_sleep_minutes": 0, "score": None, "session_count": 0,
    }


def test_an_uncovered_date_gets_no_row_even_if_a_session_exists():
    sessions = _sessions((1, "person-a", _D2, False, _dt(1, 22), _dt(2, 7), 80))

    out = build_derived_night(sessions, _stages(), _coverage(("person-a", _D2, False)))

    assert out.height == 0


def test_naps_do_not_count_toward_the_night():
    sessions = _sessions(
        (1, "person-a", _D2, False, _dt(1, 22), _dt(2, 7), 80),
        (2, "person-a", _D2, True, _dt(2, 14), _dt(2, 15), 50),
    )
    stages = _stages((1, "LIGHT", 400), (2, "LIGHT", 60))

    covered = _coverage(("person-a", _D2, True))
    out = build_derived_night(sessions, stages, covered).row(0, named=True)

    assert (out["session_count"], out["total_sleep_minutes"], out["time_in_bed_minutes"]) == (
        1, 400, 540,
    )
