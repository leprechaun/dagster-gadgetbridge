import datetime
import polars as pl
from datetime import datetime as dt
from gadgetbridge_pipeline.defs.assets.gold import daily_health_snapshot, daily_sleep_schedule


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
    assert set(result.columns) == {"reporting_date", "start", "end", "weekday"}


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


def test_weekday_flag_true_on_weekdays_false_on_weekends():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),  # Monday
        ("2024-01-20", "2024-01-19 23:00:00", "2024-01-20 07:00:00"),  # Saturday
    )
    result = daily_sleep_schedule(periods).sort("reporting_date")
    assert result["weekday"].to_list() == [True, False]
