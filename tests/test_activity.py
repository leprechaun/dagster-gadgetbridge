from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.activity import (
    build_dim_activity_kind,
    build_fact_activity_minute,
)

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)


def _bronze(*rows):
    cols = ["TIMESTAMP", "DEVICE_ID", "USER_ID", "RAW_INTENSITY", "STEPS", "RAW_KIND",
            "HEART_RATE", "UNKNOWN1", "SLEEP", "DEEP_SLEEP", "REM_SLEEP"]
    return pl.DataFrame(
        [dict(zip(cols, r, strict=True)) for r in rows],
        schema={c: pl.Datetime("us", "UTC") if c == "TIMESTAMP" else pl.Int64 for c in cols},
    )


def _row(ts, device=1, intensity=10, steps=5, kind=1, hr=70):
    return (ts, device, 1, intensity, steps, kind, hr, 0, 0, 0, 0)


def _dim_device(*rows):
    return pl.DataFrame(
        list(rows),
        schema={"device_key": pl.String, "person_key": pl.String,
                "gadgetbridge_device_id": pl.String, "valid_from": pl.Date, "valid_to": pl.Date},
        orient="row",
    )


def _tz(person="person-a", zone="UTC"):
    return pl.DataFrame(
        [(person, zone, _EPOCH, None)],
        schema={"person_key": pl.String, "timezone": pl.String,
                "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
        orient="row",
    )


_DEVICES = _dim_device(("1@2000-01-01", "person-a", "1", date(2000, 1, 1), None))


def test_dim_activity_kind_has_every_code_from_0_to_255():
    dim = build_dim_activity_kind()

    assert dim["activity_kind_key"].to_list() == list(range(256))


def test_code_255_is_the_described_invalid_row_member_and_unknown_codes_have_no_description():
    dim = build_dim_activity_kind().to_dict(as_series=False)
    description = dict(zip(dim["activity_kind_key"], dim["description"], strict=True))

    assert description[255] == "invalid row"
    assert description[0] is None


def test_only_kind_120_is_asleep():
    dim = build_dim_activity_kind()

    assert dim.filter("is_asleep")["activity_kind_key"].to_list() == [120]


# build_fact_activity_minute

def test_a_normal_row_becomes_a_fact_row_with_renamed_measures_and_no_unexplained_columns():
    ts = datetime(2000, 1, 1, 12, 30, tzinfo=_UTC)

    fact = build_fact_activity_minute(_bronze(_row(ts)), _DEVICES, _tz())

    assert fact.columns == [
        "date_key", "time_key", "device_key", "person_key", "activity_kind_key",
        "timestamp_utc", "steps", "heart_rate_bpm", "raw_intensity",
    ]
    assert fact.row(0, named=True) == {
        "date_key": date(2000, 1, 1), "time_key": 12 * 60 + 30,
        "device_key": "1@2000-01-01", "person_key": "person-a", "activity_kind_key": 1,
        "timestamp_utc": ts, "steps": 5, "heart_rate_bpm": 70, "raw_intensity": 10,
    }


def test_heart_rate_255_alone_is_nulled_and_the_row_and_other_measures_are_kept():
    ts = datetime(2000, 1, 1, 12, 30, tzinfo=_UTC)

    fact = build_fact_activity_minute(_bronze(_row(ts, hr=255, steps=7)), _DEVICES, _tz())

    assert fact["heart_rate_bpm"].to_list() == [None]
    assert fact["steps"].to_list() == [7]
    assert fact["activity_kind_key"].to_list() == [1]


def test_a_whole_row_sentinel_keeps_the_row_nulls_every_measure_and_keeps_kind_255():
    ts = datetime(2000, 1, 1, 12, 30, tzinfo=_UTC)
    bad = _row(ts, intensity=255, steps=255, kind=255, hr=255)

    fact = build_fact_activity_minute(_bronze(bad), _DEVICES, _tz())

    row = fact.row(0, named=True)
    assert (row["steps"], row["heart_rate_bpm"], row["raw_intensity"]) == (None, None, None)
    assert row["activity_kind_key"] == 255
    assert row["timestamp_utc"] == ts


def test_rows_with_no_matching_device_are_kept_with_unknown_keys():
    in_period = datetime(2000, 1, 1, 12, 30, tzinfo=_UTC)
    before_period = datetime(1999, 12, 31, 12, 30, tzinfo=_UTC)
    bronze = _bronze(_row(in_period, device=99), _row(before_period, device=1), _row(in_period))

    fact = build_fact_activity_minute(bronze, _DEVICES, _tz())

    assert fact["device_key"].to_list() == ["(unknown)", "(unknown)", "1@2000-01-01"]
    assert fact["person_key"].to_list() == ["(unknown)", "(unknown)", "person-a"]
    assert fact.height == 3
