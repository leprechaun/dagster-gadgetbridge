import datetime
import pytest
import polars as pl
from gadgetbridge_pipeline.defs.assets.silver import per_minute_health_metrics, sleep_periods_based_on_activity, daily_sleep_duration, daily_sleep_duration_checks

EXPECTED_COLUMNS = {
    "MINUTE", "DEVICE_ID", "USER_ID",
    "RAW_INTENSITY", "STEPS", "RAW_KIND", "HEART_RATE",
    "SLEEP", "DEEP_SLEEP", "REM_SLEEP",
    "TEMPERATURE", "HRV", "STRESS", "SPO2",
    "RESPIRATORY_RATE", "BATTERY_LEVEL",
}

def _ts(s):
    return datetime.datetime.fromisoformat(s)


def _activity(*rows):
    return pl.DataFrame({
        "TIMESTAMP":     [_ts(r[0]) for r in rows],
        "DEVICE_ID":     [r[1] for r in rows],
        "USER_ID":       [r[2] for r in rows],
        "RAW_INTENSITY": [0] * len(rows),
        "STEPS":         [0] * len(rows),
        "RAW_KIND":      [0] * len(rows),
        "HEART_RATE":    [60] * len(rows),
        "UNKNOWN1":      [0] * len(rows),
        "SLEEP":         [0] * len(rows),
        "DEEP_SLEEP":    [0] * len(rows),
        "REM_SLEEP":     [0] * len(rows),
    })


def _temperature(*rows):
    return pl.DataFrame({
        "TIMESTAMP":            [_ts(r[0]) for r in rows],
        "DEVICE_ID":            [r[1] for r in rows],
        "USER_ID":              [r[2] for r in rows],
        "TEMPERATURE":          [float(r[3]) for r in rows],
        "TEMPERATURE_TYPE":     [2] * len(rows),
        "TEMPERATURE_LOCATION": [9] * len(rows),
    })


def _hrv(*rows):
    return pl.DataFrame({
        "TIMESTAMP": [_ts(r[0]) for r in rows],
        "DEVICE_ID": [r[1] for r in rows],
        "USER_ID":   [r[2] for r in rows],
        "VALUE":     [r[3] for r in rows],
    })


def _stress(*rows):
    return pl.DataFrame({
        "TIMESTAMP": [_ts(r[0]) for r in rows],
        "DEVICE_ID": [r[1] for r in rows],
        "USER_ID":   [r[2] for r in rows],
        "TYPE_NUM":  [1] * len(rows),
        "STRESS":    [r[3] for r in rows],
    })


def _spo2(*rows):
    return pl.DataFrame({
        "TIMESTAMP": [_ts(r[0]) for r in rows],
        "DEVICE_ID": [r[1] for r in rows],
        "USER_ID":   [r[2] for r in rows],
        "TYPE_NUM":  [1] * len(rows),
        "SPO2":      [r[3] for r in rows],
    })


def _respiratory_rate(*rows):
    return pl.DataFrame({
        "TIMESTAMP":  [_ts(r[0]) for r in rows],
        "DEVICE_ID":  [r[1] for r in rows],
        "USER_ID":    [r[2] for r in rows],
        "UTC_OFFSET": [0] * len(rows),
        "RATE":       [r[3] for r in rows],
    })


def _battery(*rows):
    return pl.DataFrame({
        "TIMESTAMP":     [_ts(r[0]) for r in rows],
        "DEVICE_ID":     [r[1] for r in rows],
        "LEVEL":         [r[2] for r in rows],
        "BATTERY_INDEX": [0] * len(rows),
    })


# Non-matching sentinel timestamp/device used as "empty" default for tables not under test
_SENTINEL_TS = "2099-01-01 00:00:00"
_S_DEV = 99
_S_USR = 99


def _call(**kwargs):
    defaults = dict(
        activity=_activity(("2024-01-01 08:00:00", 1, 1)),
        temperature=_temperature((_SENTINEL_TS, _S_DEV, _S_USR, 36.0)),
        hrv=_hrv((_SENTINEL_TS, _S_DEV, _S_USR, 50)),
        stress=_stress((_SENTINEL_TS, _S_DEV, _S_USR, 30)),
        spo2=_spo2((_SENTINEL_TS, _S_DEV, _S_USR, 97)),
        respiratory_rate=_respiratory_rate((_SENTINEL_TS, _S_DEV, _S_USR, 15)),
        battery=_battery((_SENTINEL_TS, _S_DEV, 50)),
    )
    defaults.update(kwargs)
    return per_minute_health_metrics(**defaults)


def test_row_count():
    activity = _activity(
        ("2024-01-01 08:00:00", 1, 1),
        ("2024-01-01 08:01:00", 1, 1),
        ("2024-01-01 08:02:00", 1, 1),
    )
    result = _call(activity=activity)
    assert result.shape[0] == 3


def test_minute_truncation():
    activity = _activity(
        ("2024-01-01 08:00:30", 1, 1),
        ("2024-01-01 08:01:45", 1, 1),
    )
    result = _call(activity=activity)
    for minute in result["MINUTE"]:
        assert minute.second == 0


def test_temperature_join():
    activity = _activity(
        ("2024-01-01 08:00:00", 1, 1),
        ("2024-01-01 08:01:00", 1, 1),
    )
    temperature = _temperature(("2024-01-01 08:00:00", 1, 1, 36.6))

    result = _call(activity=activity, temperature=temperature).sort("MINUTE")

    assert result["TEMPERATURE"][0] == pytest.approx(36.6)
    assert result["TEMPERATURE"][1] is None


def test_battery_join_on_device_id_only():
    activity = _activity(
        ("2024-01-01 08:00:00", 1, 1),
        ("2024-01-01 08:00:00", 2, 2),
    )
    battery = _battery(("2024-01-01 08:00:00", 1, 80))

    result = _call(activity=activity, battery=battery)

    dev1_level = result.filter(pl.col("DEVICE_ID") == 1)["BATTERY_LEVEL"][0]
    dev2_level = result.filter(pl.col("DEVICE_ID") == 2)["BATTERY_LEVEL"][0]

    assert dev1_level == pytest.approx(80.0)
    assert dev2_level is None


def test_hrv_aggregation_within_minute():
    activity = _activity(("2024-01-01 08:00:00", 1, 1))
    hrv = _hrv(
        ("2024-01-01 08:00:10", 1, 1, 30),
        ("2024-01-01 08:00:50", 1, 1, 50),
    )
    result = _call(activity=activity, hrv=hrv)
    assert result["HRV"][0] == pytest.approx(40.0)


def test_output_columns():
    result = _call()
    assert set(result.columns) == EXPECTED_COLUMNS


def test_sort_order():
    activity = _activity(
        ("2024-01-01 08:02:00", 1, 1),
        ("2024-01-01 08:00:00", 1, 1),
        ("2024-01-01 08:01:00", 1, 1),
    )
    result = _call(activity=activity)
    minutes = result["MINUTE"].to_list()
    assert minutes == sorted(minutes)


# ---------------------------------------------------------------------------
# sleep_periods_based_on_activity
# ---------------------------------------------------------------------------

SLEEP = 120
AWAKE = 0


def _ts_utc(s):
    return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc)


def _sleep_activity(*rows):
    # each row: (utc_timestamp_str, raw_kind)
    return pl.DataFrame({
        "TIMESTAMP":     pl.Series([_ts_utc(r[0]) for r in rows], dtype=pl.Datetime("us", "UTC")),
        "DEVICE_ID":     [1] * len(rows),
        "USER_ID":       [1] * len(rows),
        "RAW_INTENSITY": [0] * len(rows),
        "STEPS":         [0] * len(rows),
        "RAW_KIND":      [r[1] for r in rows],
        "HEART_RATE":    [60] * len(rows),
        "UNKNOWN1":      [0] * len(rows),
        "SLEEP":         [0] * len(rows),
        "DEEP_SLEEP":    [0] * len(rows),
        "REM_SLEEP":     [0] * len(rows),
    })


def test_output_schema():
    activity = _sleep_activity(
        ("2024-01-14 15:00:00", AWAKE),
        ("2024-01-14 16:00:00", SLEEP),
        ("2024-01-15 07:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert set(result.columns) == {"date", "reporting_date", "start", "end"}


def test_awake_sleep_awake_produces_one_period():
    activity = _sleep_activity(
        ("2024-01-14 14:00:00", AWAKE),
        ("2024-01-14 15:00:00", SLEEP),
        ("2024-01-15 07:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert result.shape[0] == 1
    # start is stored in Bangkok time; verify it represents the correct UTC instant
    assert result["start"][0].astimezone(datetime.timezone.utc) == _ts_utc("2024-01-14 15:00:00")


def test_only_sleep_is_null():
    activity = _sleep_activity(
        ("2024-01-14 10:00:00", AWAKE),
        ("2024-01-14 22:00:00", SLEEP),
    )
    result = sleep_periods_based_on_activity(activity)
    print(result)
    assert result.shape[0] == 0


def test_consecutive_sleep_rows_produce_one_period():
    # Three consecutive sleep rows should still produce a single period
    activity = _sleep_activity(
        ("2024-01-14 14:00:00", AWAKE),
        ("2024-01-14 15:00:00", SLEEP),
        ("2024-01-14 16:00:00", SLEEP),
        ("2024-01-14 17:00:00", SLEEP),
        ("2024-01-15 07:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert result.shape[0] == 1



def test_sleep_before_18_assigned_to_same_day():
    # 2024-01-14 19:00 UTC = 2024-01-15 02:00 Bangkok; 02:00 < 18:00 → reporting_date = 2024-01-15
    activity = _sleep_activity(
        ("2024-01-14 18:00:00", AWAKE),
        ("2024-01-14 19:00:00", SLEEP),
        ("2024-01-15 06:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert result["reporting_date"][0] == datetime.date(2024, 1, 15)


def test_sleep_after_18_assigned_to_next_day():
    # 2024-01-14 15:00 UTC = 2024-01-14 22:00 Bangkok; 22:00 > 18:00 → reporting_date = 2024-01-15
    activity = _sleep_activity(
        ("2024-01-14 14:00:00", AWAKE),
        ("2024-01-14 15:00:00", SLEEP),
        ("2024-01-15 07:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert result["reporting_date"][0] == datetime.date(2024, 1, 15)


def test_no_sleep_activity_returns_empty():
    activity = _sleep_activity(
        ("2024-01-14 08:00:00", AWAKE),
        ("2024-01-14 09:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    assert result.shape[0] == 0


# ---------------------------------------------------------------------------
# daily_sleep_duration
# ---------------------------------------------------------------------------

def _bkk(s):
    return datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=7)))


def _sleep_periods(*rows):
    # each row: (reporting_date, start, end) — start/end as "YYYY-MM-DD HH:MM:SS" Bangkok-local strings
    return pl.DataFrame({
        "date":            [r[0] for r in rows],
        "reporting_date":  [r[0] for r in rows],
        "start":           pl.Series([_bkk(r[1]) for r in rows]),
        "end":             pl.Series([_bkk(r[2]) for r in rows]),
    }).with_columns(
        pl.col("date").str.to_date(),
        pl.col("reporting_date").str.to_date(),
    )


def test_daily_sleep_duration_output_schema():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_duration(periods)
    assert set(result.columns) == {"reporting_date", "sleep_start", "wake_time", "total_sleep_minutes"}


def test_daily_sleep_duration_single_period_duration_start_and_wake():
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_duration(periods)
    assert result.shape[0] == 1
    assert result["total_sleep_minutes"][0] == 8 * 60
    assert result["sleep_start"][0] == _bkk("2024-01-14 23:00:00")
    assert result["wake_time"][0] == _bkk("2024-01-15 07:00:00")


def test_daily_sleep_duration_interrupted_sleep_sums_periods_and_spans_earliest_to_latest():
    # two periods the same night, with a waking gap in between
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 01:00:00"),
        ("2024-01-15", "2024-01-15 01:30:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_duration(periods)
    assert result.shape[0] == 1
    # 2h + 5.5h = 7.5h of actual sleep, not the full 8h span
    assert result["total_sleep_minutes"][0] == 7.5 * 60
    assert result["sleep_start"][0] == _bkk("2024-01-14 23:00:00")
    assert result["wake_time"][0] == _bkk("2024-01-15 07:00:00")


def test_daily_sleep_duration_multiple_nights_are_grouped_and_sorted_separately():
    periods = _sleep_periods(
        ("2024-01-16", "2024-01-15 23:00:00", "2024-01-16 06:00:00"),
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_duration(periods)
    assert result["reporting_date"].to_list() == [datetime.date(2024, 1, 15), datetime.date(2024, 1, 16)]
    assert result["total_sleep_minutes"].to_list() == [8 * 60, 7 * 60]


def test_daily_sleep_duration_total_sleep_minutes_is_a_plain_numeric_type():
    # Delta Lake has no duration/interval type — regression guard against
    # reintroducing a pl.Duration column (see commit 253eaa6).
    periods = _sleep_periods(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00"),
    )
    result = daily_sleep_duration(periods)
    assert result["total_sleep_minutes"].dtype in (pl.Int64, pl.Int32, pl.Float64)


# daily_sleep_duration_checks — total_sleep_minutes in [0, 1440], sleep_start < wake_time

def _daily_summary(*rows):
    # each row: (reporting_date, sleep_start, wake_time, total_sleep_minutes)
    return pl.DataFrame({
        "reporting_date":     [datetime.date.fromisoformat(r[0]) for r in rows],
        "sleep_start":        pl.Series([_bkk(r[1]) for r in rows]),
        "wake_time":          pl.Series([_bkk(r[2]) for r in rows]),
        "total_sleep_minutes": [r[3] for r in rows],
    })


def test_daily_sleep_duration_checks_passes():
    df = _daily_summary(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00", 480),
    )
    result = daily_sleep_duration_checks(df)
    assert result.passed


def test_daily_sleep_duration_checks_fails_on_negative_duration():
    df = _daily_summary(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00", -10),
    )
    result = daily_sleep_duration_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["check"] == "greater_than_or_equal_to(0)" for f in failures)


def test_daily_sleep_duration_checks_fails_above_24h():
    df = _daily_summary(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00", 1441),
    )
    result = daily_sleep_duration_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["check"] == "less_than_or_equal_to(1440)" for f in failures)


def test_daily_sleep_duration_checks_fails_when_start_not_before_wake():
    df = _daily_summary(
        ("2024-01-15", "2024-01-15 08:00:00", "2024-01-15 07:00:00", 480),
    )
    result = daily_sleep_duration_checks(df)
    assert not result.passed
    failures = result.metadata["failure_cases"].value
    assert any(f["check"] == "sleep_start_before_wake_time" for f in failures)


def test_daily_sleep_duration_checks_reports_multiple_failures_in_one_pass():
    # pandera's lazy validation should surface every violation, not just the first
    df = _daily_summary(
        ("2024-01-15", "2024-01-14 23:00:00", "2024-01-15 07:00:00", -10),
        ("2024-01-16", "2024-01-16 08:00:00", "2024-01-16 07:00:00", 1441),
    )
    result = daily_sleep_duration_checks(df)
    assert not result.passed
    checks_failed = {f["check"] for f in result.metadata["failure_cases"].value}
    assert checks_failed == {
        "greater_than_or_equal_to(0)",
        "less_than_or_equal_to(1440)",
        "sleep_start_before_wake_time",
    }
