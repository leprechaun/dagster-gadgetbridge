import polars as pl
from datetime import datetime
from gadgetbridge_pipeline.defs.assets.bronze import apply_bronze_transform


def test_apply_bronze_transform_seconds():
    df = pl.DataFrame({"TIMESTAMP": [0, 86400], "VALUE": [1, 2]})
    result = apply_bronze_transform(df, "s")
    assert result["TIMESTAMP"].dtype == pl.Datetime
    assert result["TIMESTAMP"][1].date() == datetime(1970, 1, 2).date()


def test_apply_bronze_transform_milliseconds():
    df = pl.DataFrame({"TIMESTAMP": [0, 86400_000], "VALUE": [1, 2]})
    result = apply_bronze_transform(df, "ms")
    assert result["TIMESTAMP"][1].date() == datetime(1970, 1, 2).date()
