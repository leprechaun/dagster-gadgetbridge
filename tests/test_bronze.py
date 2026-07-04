import pytest
import polars as pl
from datetime import datetime
from gadgetbridge_pipeline.defs.assets.bronze import (
    apply_bronze_transform,
    activity_heartrate_checks,
    battery_level_checks,
    spo2_checks,
    temperature_checks,
    stress_checks,
    hrv_checks,
    respiratory_rate_checks,
)

def _hr_df(values):
    return pl.DataFrame({"HEART_RATE": values})

def test_apply_bronze_transform_seconds():
    df = pl.DataFrame({"TIMESTAMP": [0, 86400], "VALUE": [1, 2]})
    result = apply_bronze_transform(df, "s")
    assert result["TIMESTAMP"].dtype == pl.Datetime
    assert result["TIMESTAMP"][1].date() == datetime(1970, 1, 2).date()


def test_apply_bronze_transform_milliseconds():
    df = pl.DataFrame({"TIMESTAMP": [0, 86400_000], "VALUE": [1, 2]})
    result = apply_bronze_transform(df, "ms")
    assert result["TIMESTAMP"][1].date() == datetime(1970, 1, 2).date()

def test_heartrate_checks_pass():
    result = activity_heartrate_checks(_hr_df([60, 80, 120]))
    assert result.passed
    assert result.metadata["minimum"].value == 60
    assert result.metadata["maximum"].value == 120
    assert result.metadata["is_positive"].value is True
    assert result.metadata["is_255 or below"].value is True

def test_heartrate_checks_fails_on_zero():
    result = activity_heartrate_checks(_hr_df([0, 60, 120]))
    assert not result.passed
    assert result.metadata["is_positive"].value is False


def test_heartrate_checks_fails_above_255():
    result = activity_heartrate_checks(_hr_df([60, 120, 256]))
    assert not result.passed
    assert result.metadata["is_255 or below"].value is False


def test_heartrate_checks_fails_both():
    result = activity_heartrate_checks(_hr_df([0, 256]))
    assert not result.passed
    assert result.metadata["is_positive"].value is False
    assert result.metadata["is_255 or below"].value is False


# battery_level — LEVEL in [0, 100]

def test_battery_level_checks_passes():
    result = battery_level_checks(pl.DataFrame({"LEVEL": [2, 54, 100]}))
    assert result.passed
    assert result.metadata["minimum"].value == 2
    assert result.metadata["maximum"].value == 100

def test_battery_level_checks_fails_below_min():
    result = battery_level_checks(pl.DataFrame({"LEVEL": [-1, 50]}))
    assert not result.passed

def test_battery_level_checks_fails_above_max():
    result = battery_level_checks(pl.DataFrame({"LEVEL": [50, 101]}))
    assert not result.passed


# huami_spo2_sample — SPO2 in [70, 100]

def test_spo2_checks_passes():
    result = spo2_checks(pl.DataFrame({"SPO2": [79, 96, 99]}))
    assert result.passed
    assert result.metadata["minimum"].value == 79
    assert result.metadata["maximum"].value == 99

def test_spo2_checks_fails_below_min():
    result = spo2_checks(pl.DataFrame({"SPO2": [69, 95]}))
    assert not result.passed

def test_spo2_checks_fails_above_max():
    result = spo2_checks(pl.DataFrame({"SPO2": [95, 101]}))
    assert not result.passed


# generic_temperature_sample — TEMPERATURE in [15.0, 42.0]

def test_temperature_checks_passes():
    result = temperature_checks(pl.DataFrame({"TEMPERATURE": [18.12, 33.25, 40.62]}))
    assert result.passed
    assert result.metadata["minimum"].value == pytest.approx(18.12)
    assert result.metadata["maximum"].value == pytest.approx(40.62)

def test_temperature_checks_fails_below_min():
    result = temperature_checks(pl.DataFrame({"TEMPERATURE": [14.9, 33.0]}))
    assert not result.passed

def test_temperature_checks_fails_above_max():
    result = temperature_checks(pl.DataFrame({"TEMPERATURE": [33.0, 42.1]}))
    assert not result.passed


# huami_stress_sample — STRESS in [1, 100]

def test_stress_checks_passes():
    result = stress_checks(pl.DataFrame({"STRESS": [6, 35, 67]}))
    assert result.passed
    assert result.metadata["minimum"].value == 6
    assert result.metadata["maximum"].value == 67

def test_stress_checks_fails_below_min():
    result = stress_checks(pl.DataFrame({"STRESS": [0, 35]}))
    assert not result.passed

def test_stress_checks_fails_above_max():
    result = stress_checks(pl.DataFrame({"STRESS": [35, 101]}))
    assert not result.passed


# generic_hrv_value_sample — VALUE in (0, 300]

def test_hrv_checks_passes():
    result = hrv_checks(pl.DataFrame({"VALUE": [13, 29, 114]}))
    assert result.passed
    assert result.metadata["minimum"].value == 13
    assert result.metadata["maximum"].value == 114

def test_hrv_checks_fails_at_zero():
    result = hrv_checks(pl.DataFrame({"VALUE": [0, 50]}))
    assert not result.passed

def test_hrv_checks_fails_above_max():
    result = hrv_checks(pl.DataFrame({"VALUE": [50, 301]}))
    assert not result.passed


# huami_sleep_respiratory_rate_sample — RATE in [4, 60]

def test_respiratory_rate_checks_passes():
    result = respiratory_rate_checks(pl.DataFrame({"RATE": [6, 15, 22]}))
    assert result.passed
    assert result.metadata["minimum"].value == 6
    assert result.metadata["maximum"].value == 22

def test_respiratory_rate_checks_fails_below_min():
    result = respiratory_rate_checks(pl.DataFrame({"RATE": [3, 15]}))
    assert not result.passed

def test_respiratory_rate_checks_fails_above_max():
    result = respiratory_rate_checks(pl.DataFrame({"RATE": [15, 61]}))
    assert not result.passed
