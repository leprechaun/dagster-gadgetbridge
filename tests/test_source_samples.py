from datetime import date, datetime, timezone

import polars as pl
import pytest

from gadgetbridge_pipeline.defs.gadgetbridge.bronze import _TABLES
from gadgetbridge_pipeline.defs.gadgetbridge.source_samples import (
    SOURCES,
    build_fact_source_sample,
)

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_DEVICES = pl.DataFrame(
    [("1@2000-01-01", "person-a", "1", date(2000, 1, 1), None)],
    schema={"device_key": pl.String, "person_key": pl.String, "gadgetbridge_device_id": pl.String,
            "valid_from": pl.Date, "valid_to": pl.Date},
    orient="row",
)
_TZ = pl.DataFrame(
    [("person-a", "Asia/Bangkok", _EPOCH, None)],
    schema={"person_key": pl.String, "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    orient="row",
)


def _bronze(*rows):
    """rows: (timestamp, device_id, value)"""
    return pl.DataFrame(
        [(ts, d, 1, v) for ts, d, v in rows],
        schema={"TIMESTAMP": pl.Datetime("us", "UTC"), "DEVICE_ID": pl.Int64,
                "USER_ID": pl.Int64, "VALUE": pl.Int64},
        orient="row",
    )


def test_each_source_row_becomes_one_fact_row_with_keys_utc_seconds_and_renamed_measures():
    ts = datetime(2000, 1, 1, 18, 30, 45, tzinfo=_UTC)  # 01:30 on Jan 2 at UTC+7

    fact = build_fact_source_sample(
        _bronze((ts, 1, 55)), {"hrv_ms": "VALUE"}, _DEVICES, _TZ
    )

    assert fact.columns == [
        "date_key", "time_key", "device_key", "person_key", "timestamp_utc", "hrv_ms",
    ]
    assert fact.row(0, named=True) == {
        "date_key": date(2000, 1, 2), "time_key": 90, "device_key": "1@2000-01-01",
        "person_key": "person-a", "timestamp_utc": ts, "hrv_ms": 55,
    }


@pytest.mark.parametrize("fact_name", sorted(SOURCES))
def test_every_source_names_a_real_bronze_table_and_real_measure_columns(fact_name):
    spec = SOURCES[fact_name]

    schema = _TABLES[spec["table"]]["schema"]

    assert {"TIMESTAMP", "DEVICE_ID"} <= set(schema.names())
    assert set(spec["measures"].values()) <= set(schema.names())
    assert spec["measures"], "a source fact needs at least one measure"


def test_the_six_remaining_sources_each_get_their_own_fact():
    assert sorted(SOURCES) == [
        "fact_hrv_sample", "fact_pai_sample", "fact_respiratory_rate_sample",
        "fact_spo2_sample", "fact_stress_sample", "fact_temperature_sample",
    ]
