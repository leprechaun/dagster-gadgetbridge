import polars as pl
from datetime import datetime
from gadgetbridge_pipeline.defs.assets.bronze import apply_bronze_transform, activity_heartrate_checks

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
