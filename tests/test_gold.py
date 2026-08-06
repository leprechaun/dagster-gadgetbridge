import datetime
import statistics
from datetime import datetime as dt

import polars as pl
import pytest

from gadgetbridge_pipeline.defs.gadgetbridge.gold import (
    _is_weekend,
    _normalize_time_of_day,
    daily_health_snapshot,
    daily_sleep_schedule,
    heart_rate_distribution_by_medication_and_weekday,
    sleep_consistency,
    sleep_score_stats,
    sleep_score_stats_checks,
    steps_per_day,
    steps_vs_stress,
)

# ---------------------------------------------------------------------------
# _is_weekend
# ---------------------------------------------------------------------------

def test_is_weekend_true_for_saturday_and_sunday_false_for_weekdays():
    df = pl.DataFrame({
        "date": [
            datetime.date(2024, 1, 15),  # Monday
            datetime.date(2024, 1, 19),  # Friday
            datetime.date(2024, 1, 20),  # Saturday
            datetime.date(2024, 1, 21),  # Sunday
        ],
    })
    result = df.select(_is_weekend("date").alias("is_weekend"))["is_weekend"].to_list()
    assert result == [False, False, True, True]


def test_daily_health_snapshot_joins_and_averages():
    def ts(d):
        return dt.fromisoformat(d)

    activity_sample = pl.DataFrame({
        "TIMESTAMP": [ts("2024-01-01 08:00"), ts("2024-01-01 20:00")], "HEART_RATE": [40.0, 60.0],
    })
    hrv = pl.DataFrame({
        "TIMESTAMP": [ts("2024-01-01 08:00"), ts("2024-01-01 20:00")], "VALUE": [40.0, 60.0],
    })
    spo2 = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 09:00")], "SPO2": [97.0]})
    stress = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 10:00")], "STRESS": [30.0]})
    temperature = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 11:00")], "TEMPERATURE": [36.6]})

    result = daily_health_snapshot(
        hrv=hrv,
        spo2=spo2,
        stress=stress,
        temperature=temperature,
        activity_sample=activity_sample,
    )

    assert result.shape[0] == 1
    assert result["avg_hrv"][0] == 50.0
    assert result["avg_spo2"][0] == 97.0


# ---------------------------------------------------------------------------
# steps_per_day
# ---------------------------------------------------------------------------

def _activity_steps(*rows):
    # each row: (timestamp_str, steps)
    return pl.DataFrame({
        "TIMESTAMP": [dt.fromisoformat(r[0]) for r in rows],
        "STEPS": [r[1] for r in rows],
    })


def test_steps_per_day_sums_steps_within_a_day():
    activity = _activity_steps(
        ("2024-01-15 08:00:00", 100),
        ("2024-01-15 09:00:00", 200),
        ("2024-01-16 08:00:00", 50),
    )
    result = steps_per_day(activity).sort("date")
    assert result["STEPS"].to_list() == [300, 50]


def test_steps_per_day_is_weekend_flag():
    activity = _activity_steps(
        ("2024-01-15 08:00:00", 100),  # Monday
        ("2024-01-20 08:00:00", 100),  # Saturday
    )
    result = steps_per_day(activity).sort("date")
    assert result["is_weekend"].to_list() == [False, True]


# ---------------------------------------------------------------------------
# steps_vs_stress
# ---------------------------------------------------------------------------

def _stress_samples(*rows):
    # each row: (timestamp_str, stress)
    return pl.DataFrame({
        "TIMESTAMP": [dt.fromisoformat(r[0]) for r in rows],
        "STRESS": [r[1] for r in rows],
    })


def test_steps_vs_stress_totals_and_stress_stats():
    activity = _activity_steps(
        ("2024-01-15 08:00:00", 100),
        ("2024-01-15 09:00:00", 200),
    )
    stress = _stress_samples(
        ("2024-01-15 08:00:00", 20.0),
        ("2024-01-15 09:00:00", 40.0),
    )
    result = steps_vs_stress(activity, stress)
    assert result["total_steps"][0] == 300
    assert result["avg_stress"][0] == 30.0
    assert result["median_stress"][0] == 30.0


def test_steps_vs_stress_inner_joins_only_days_present_in_both():
    activity = _activity_steps(
        ("2024-01-15 08:00:00", 100),
        ("2024-01-16 08:00:00", 100),
    )
    stress = _stress_samples(
        ("2024-01-15 08:00:00", 20.0),
    )
    result = steps_vs_stress(activity, stress)
    assert result["date"].to_list() == [datetime.date(2024, 1, 15)]


# ---------------------------------------------------------------------------
# heart_rate_distribution_by_medication_and_weekday
# ---------------------------------------------------------------------------

def _hr_distribution(*rows):
    # each row: (date, heart_rate, sample_count)
    return pl.DataFrame({
        "date": [r[0] for r in rows],
        "heart_rate": [r[1] for r in rows],
        "sample_count": [r[2] for r in rows],
    })


def _medicine_log(*rows):
    # each row: (date, medicine, taken)
    return pl.DataFrame({
        "date": [r[0] for r in rows],
        "medicine": [r[1] for r in rows],
        "taken": [r[2] for r in rows],
    })


def test_heart_rate_distribution_defaults_to_sober_with_no_matching_medication():
    hr = _hr_distribution((datetime.date(2024, 1, 15), 60, 10))
    med = _medicine_log((datetime.date(2024, 1, 16), "aspirin", True))
    result = heart_rate_distribution_by_medication_and_weekday(hr, med)
    assert result["medication_state"].to_list() == ["sober"]


def test_heart_rate_distribution_ignores_untaken_medicine():
    hr = _hr_distribution((datetime.date(2024, 1, 15), 60, 10))
    med = _medicine_log((datetime.date(2024, 1, 15), "aspirin", False))
    result = heart_rate_distribution_by_medication_and_weekday(hr, med)
    assert result["medication_state"].to_list() == ["sober"]


def test_heart_rate_distribution_combines_multiple_medicines_taken_same_day_sorted():
    hr = _hr_distribution((datetime.date(2024, 1, 15), 60, 10))
    med = _medicine_log(
        (datetime.date(2024, 1, 15), "zolpidem", True),
        (datetime.date(2024, 1, 15), "aspirin", True),
    )
    result = heart_rate_distribution_by_medication_and_weekday(hr, med)
    assert result["medication_state"].to_list() == ["aspirin + zolpidem"]


def test_heart_rate_distribution_aggregates_across_dates_within_same_group():
    hr = _hr_distribution(
        (datetime.date(2024, 1, 15), 60, 10),  # Monday
        (datetime.date(2024, 1, 22), 60, 5),   # next Monday
    )
    med = _medicine_log(
        (datetime.date(2024, 1, 15), "aspirin", True),
        (datetime.date(2024, 1, 22), "aspirin", True),
    )
    result = heart_rate_distribution_by_medication_and_weekday(hr, med)
    assert result["sample_count"].to_list() == [15]


def test_heart_rate_distribution_proportion_normalized_within_group():
    hr = _hr_distribution(
        (datetime.date(2024, 1, 15), 60, 30),
        (datetime.date(2024, 1, 15), 70, 10),
    )
    med = _medicine_log((datetime.date(2099, 1, 1), "placeholder", True))
    result = heart_rate_distribution_by_medication_and_weekday(hr, med).sort("heart_rate")
    assert result["proportion"].to_list() == [0.75, 0.25]


def test_heart_rate_distribution_is_weekend_flag():
    hr = _hr_distribution(
        (datetime.date(2024, 1, 15), 60, 10),  # Monday
        (datetime.date(2024, 1, 20), 60, 10),  # Saturday
    )
    med = _medicine_log(
        (datetime.date(2024, 1, 15), "aspirin", True),
        (datetime.date(2024, 1, 20), "aspirin", True),
    )
    result = heart_rate_distribution_by_medication_and_weekday(hr, med).sort("is_weekend")
    assert result["is_weekend"].to_list() == [False, True]


# ---------------------------------------------------------------------------
# daily_sleep_schedule
# ---------------------------------------------------------------------------

def _bkk(s):
    return dt.fromisoformat(s).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=7)))


# ---------------------------------------------------------------------------
# _normalize_time_of_day
# ---------------------------------------------------------------------------

def _as_bangkok(*iso_strings):
    # _normalize_time_of_day expects its input already converted to the
    # target local zone (as daily_sleep_schedule/sleep_consistency both do
    # before calling it) — a bare fixed-offset python datetime stores as UTC
    # once it round-trips through polars, so re-localize explicitly here.
    return pl.DataFrame({"t": [_bkk(s) for s in iso_strings]}).select(
        pl.col("t").dt.convert_time_zone("Asia/Bangkok")
    )


def test_normalize_time_of_day_keeps_pre_cutoff_time_on_the_common_date():
    df = _as_bangkok("2024-01-15 07:00:00")
    result = df.select(_normalize_time_of_day(pl.col("t")).alias("t"))["t"][0]
    assert result == _bkk("1900-01-01 07:00:00").replace(tzinfo=None)


def test_normalize_time_of_day_shifts_post_cutoff_time_back_a_day():
    df = _as_bangkok("2024-01-15 23:00:00")
    result = df.select(_normalize_time_of_day(pl.col("t")).alias("t"))["t"][0]
    assert result == _bkk("1899-12-31 23:00:00").replace(tzinfo=None)


def _sleep_periods(*rows):
    # each row: (reporting_date, start, end) — start/end as
    # "YYYY-MM-DD HH:MM:SS" Bangkok-local strings
    return pl.DataFrame({
        "date":           [r[0] for r in rows],
        "reporting_date": [r[0] for r in rows],
        "start":          pl.Series([_bkk(r[1]) for r in rows]),
        "end":            pl.Series([_bkk(r[2]) for r in rows]),
    }).with_columns(
        pl.col("date").str.to_date(),
        pl.col("reporting_date").str.to_date(),
    )


def test_output_columns():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_schedule(periods)
    assert set(result.columns) == {"reporting_date", "start", "end", "is_weekend"}


def test_reporting_date_formatted_as_string():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_schedule(periods)
    assert result["reporting_date"][0] == "2024-01-15"


def test_times_before_cutoff_land_on_the_common_date():
    # 07:00 wake time is before the 15:00 cutoff, so it stays on 1900-01-01
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_schedule(periods)
    assert result["end"][0] == _bkk("1900-01-01 07:00:00").replace(tzinfo=None)


def test_times_after_cutoff_are_shifted_back_a_day():
    # 23:00 start time is after the 15:00 cutoff, so it's pushed onto 1899-12-31
    # to line up on the same continuous axis as the following morning's wake time
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_schedule(periods)
    assert result["start"][0] == _bkk("1899-12-31 23:00:00").replace(tzinfo=None)


def test_is_weekend_flag_true_on_weekdays_false_on_weekends():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),  # Monday
        ("2024-01-20", "2024-01-19 23:00:00", "2024-01-20 07:00:00"),  # Saturday
    )
    result = daily_sleep_schedule(periods).sort("reporting_date")
    assert result["is_weekend"].to_list() == [False, True]


# ---------------------------------------------------------------------------
# sleep_score_stats
# ---------------------------------------------------------------------------

def _sleep_sessions(*rows):
    # each row: (rec_timestamp_str, score)
    return pl.DataFrame({
        "rec":   [dt.fromisoformat(r[0]) for r in rows],
        "score": [r[1] for r in rows],
    })


def test_sleep_score_stats_mean_max_and_session_count():
    sessions = _sleep_sessions(
        ("2024-01-15 23:00:00", 80),
        ("2024-01-15 23:30:00", 90),
    )
    result = sleep_score_stats(sessions)
    assert result.shape[0] == 1
    assert result["mean_score"][0] == 85.0
    assert result["max_score"][0] == 90
    assert result["session_count"][0] == 2


def test_sleep_score_stats_groups_by_date_of_rec():
    sessions = _sleep_sessions(
        ("2024-01-15 08:00:00", 80),
        ("2024-01-16 08:00:00", 60),
    )
    result = sleep_score_stats(sessions).sort("date")
    assert result["date"].to_list() == [datetime.date(2024, 1, 15), datetime.date(2024, 1, 16)]
    assert result["mean_score"].to_list() == [80.0, 60.0]


def test_sleep_score_stats_is_weekend_flag():
    sessions = _sleep_sessions(
        ("2024-01-15 08:00:00", 80),  # Monday
        ("2024-01-20 08:00:00", 80),  # Saturday
    )
    result = sleep_score_stats(sessions).sort("date")
    assert result["is_weekend"].to_list() == [False, True]


def test_sleep_score_stats_rolling_average_is_null_before_seven_days():
    sessions = _sleep_sessions(
        *[(f"2024-01-{d:02d} 08:00:00", 80) for d in range(1, 7)]
    )
    result = sleep_score_stats(sessions)
    assert result["score_7d_ma"].null_count() == result.shape[0]


def test_sleep_score_stats_rolling_average_after_seven_days():
    sessions = _sleep_sessions(
        *[(f"2024-01-{d:02d} 08:00:00", 70) for d in range(1, 9)]
    )
    result = sleep_score_stats(sessions).sort("date")
    assert result["score_7d_ma"][-1] == 70.0


# sleep_score_stats_checks — mean_score/max_score/score_7d_ma in [0, 100],
# session_count > 0, mean_score <= max_score

def _sleep_score_stats_df(*rows):
    # each row: (date, mean_score, max_score, session_count, score_7d_ma)
    return pl.DataFrame({
        "date":           [r[0] for r in rows],
        "mean_score":     [r[1] for r in rows],
        "max_score":      [r[2] for r in rows],
        "session_count":  [r[3] for r in rows],
        "score_7d_ma":    pl.Series([r[4] for r in rows], dtype=pl.Float64),
    })


def test_sleep_score_stats_checks_passes():
    df = _sleep_score_stats_df(
        (datetime.date(2024, 1, 15), 80.0, 90, 2, 75.0),
    )
    result = sleep_score_stats_checks(df)
    assert result.passed


def test_sleep_score_stats_checks_passes_with_null_rolling_average():
    # score_7d_ma is null for the first 6 days of data — that's expected, not a failure
    df = _sleep_score_stats_df(
        (datetime.date(2024, 1, 15), 80.0, 90, 2, None),
    )
    result = sleep_score_stats_checks(df)
    assert result.passed


def test_sleep_score_stats_checks_fails_on_score_out_of_range():
    df = _sleep_score_stats_df(
        (datetime.date(2024, 1, 15), 150.0, 90, 2, 75.0),
    )
    result = sleep_score_stats_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["column"] == "mean_score" for f in failures)


def test_sleep_score_stats_checks_fails_on_zero_session_count():
    df = _sleep_score_stats_df(
        (datetime.date(2024, 1, 15), 80.0, 90, 0, 75.0),
    )
    result = sleep_score_stats_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["column"] == "session_count" for f in failures)


def test_sleep_score_stats_checks_fails_when_mean_exceeds_max():
    df = _sleep_score_stats_df(
        (datetime.date(2024, 1, 15), 95.0, 90, 2, 75.0),
    )
    result = sleep_score_stats_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["check"] == "mean_score_le_max_score" for f in failures)


# ---------------------------------------------------------------------------
# sleep_consistency
# ---------------------------------------------------------------------------

def _daily_sleep_duration(*rows):
    # each row: (reporting_date, sleep_start, wake_time) — start/end as
    # "YYYY-MM-DD HH:MM:SS" Bangkok-local strings
    return pl.DataFrame({
        "reporting_date": [r[0] for r in rows],
        "sleep_start": pl.Series([_bkk(r[1]) for r in rows]),
        "wake_time": pl.Series([_bkk(r[2]) for r in rows]),
    })


def _sleep_score_stats_rows(*rows):
    # each row: (date, mean_score)
    return pl.DataFrame({
        "date": [r[0] for r in rows],
        "mean_score": [r[1] for r in rows],
    })


def _regular_nights(n):
    # n nights of identical 23:00 -> 07:00 sleep, dates 2024-01-01 .. 2024-01-0n
    return [
        (datetime.date(2024, 1, d), f"2024-01-{d:02d} 23:00:00", f"2024-01-{d + 1:02d} 07:00:00")
        for d in range(1, n + 1)
    ]


def test_sleep_consistency_perfectly_regular_bedtime_wake_and_score_has_zero_stddev():
    nights = _regular_nights(14)
    duration = _daily_sleep_duration(*nights)
    stats = _sleep_score_stats_rows(*[(d, 80.0) for d, _, _ in nights])

    result = sleep_consistency(duration, stats).sort("date")
    last = result.tail(1)

    assert last["sleep_start_stddev_minutes"][0] == pytest.approx(0.0, abs=1e-6)
    assert last["wake_time_stddev_minutes"][0] == pytest.approx(0.0, abs=1e-6)
    assert last["score_stddev"][0] == pytest.approx(0.0, abs=1e-6)


def test_sleep_consistency_null_before_window_fills():
    nights = _regular_nights(5)
    duration = _daily_sleep_duration(*nights)
    stats = _sleep_score_stats_rows(*[(d, 80.0) for d, _, _ in nights])

    result = sleep_consistency(duration, stats)

    assert result["sleep_start_stddev_minutes"].null_count() == result.shape[0]
    assert result["wake_time_stddev_minutes"].null_count() == result.shape[0]
    assert result["score_stddev"].null_count() == result.shape[0]


def test_sleep_consistency_matches_hand_computed_stddev():
    # wake time alternates 07:00 / 07:10 across 14 nights — a known, irregular sequence
    offsets = [0, 10] * 7
    rows = []
    for i, offset in enumerate(offsets, start=1):
        hour, minute = divmod(7 * 60 + offset, 60)
        rows.append((
            datetime.date(2024, 1, i),
            f"2024-01-{i:02d} 23:00:00",
            f"2024-01-{i + 1:02d} {hour:02d}:{minute:02d}:00",
        ))
    duration = _daily_sleep_duration(*rows)
    stats = _sleep_score_stats_rows(*[(d, 80.0) for d, _, _ in rows])

    result = sleep_consistency(duration, stats).sort("date")

    expected = statistics.stdev(offsets)
    assert result["wake_time_stddev_minutes"][-1] == pytest.approx(expected, abs=1e-6)


def test_sleep_consistency_outer_joins_dates_present_in_only_one_input():
    duration = _daily_sleep_duration(
        (datetime.date(2024, 1, 1), "2024-01-01 23:00:00", "2024-01-02 07:00:00"),
    )
    stats = _sleep_score_stats_rows(
        (datetime.date(2024, 1, 2), 80.0),
    )

    result = sleep_consistency(duration, stats).sort("date")

    assert result["date"].to_list() == [datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)]
    assert result["score_stddev"][0] is None
    assert result["sleep_start_stddev_minutes"][1] is None
