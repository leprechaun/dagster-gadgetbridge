import datetime
import pytest
import polars as pl
from gadgetbridge_pipeline.defs.assets.silver import per_minute_health_metrics, sleep_periods_based_on_activity

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
    assert set(result.columns) == {"date", "reporting_date", "start", "end", "duration"}


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


def test_duration_spans_start_to_end():
    activity = _sleep_activity(
        ("2024-01-14 14:00:00", AWAKE),
        ("2024-01-14 15:00:00", SLEEP),
        ("2024-01-15 07:00:00", AWAKE),
    )
    result = sleep_periods_based_on_activity(activity)
    expected = datetime.timedelta(hours=16)
    assert result["duration"][0] == expected


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
