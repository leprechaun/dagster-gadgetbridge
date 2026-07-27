import datetime
import polars as pl
from datetime import datetime as dt
from gadgetbridge_pipeline.defs.assets.gadgetbridge.gold import (
    _is_weekend,
    daily_health_snapshot,
    daily_sleep_schedule,
    heart_rate_distribution_by_medication_and_weekday,
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

    activity_sample = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 08:00"), ts("2024-01-01 20:00")], "HEART_RATE": [40.0, 60.0]})
    hrv = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 08:00"), ts("2024-01-01 20:00")], "VALUE": [40.0, 60.0]})
    spo2 = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 09:00")], "SPO2": [97.0]})
    stress = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 10:00")], "STRESS": [30.0]})
    temperature = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 11:00")], "TEMPERATURE": [36.6]})

    result = daily_health_snapshot(hrv=hrv, spo2=spo2, stress=stress, temperature=temperature, activity_sample=activity_sample)

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


def _sleep_periods(*rows):
    # each row: (reporting_date, start, end) — start/end as "YYYY-MM-DD HH:MM:SS" Bangkok-local strings
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
