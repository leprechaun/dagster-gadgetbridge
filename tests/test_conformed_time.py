from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_TZ_SCHEMA = {
    "person_key": pl.String,
    "timezone": pl.String,
    "valid_from": pl.Datetime("us", "UTC"),
    "valid_to": pl.Datetime("us", "UTC"),
}


def _tz(*rows):
    return pl.DataFrame(list(rows), schema=_TZ_SCHEMA, orient="row")


def _samples(*rows):
    return pl.DataFrame(
        list(rows),
        schema={"person_key": pl.String, "ts": pl.Datetime("us", "UTC")},
        orient="row",
    )


def _utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


def test_time_that_crosses_midnight_in_the_local_zone_lands_on_the_next_local_date():
    tz = _tz(("person-a", "Asia/Bangkok", _EPOCH, None))
    samples = _samples(("person-a", _utc(2000, 1, 1, 17, 30)))  # 00:30 on Jan 2 at UTC+7

    out = with_local_keys(samples, "ts", tz)

    assert out["date_key"].to_list() == [date(2000, 1, 2)]
    assert out["time_key"].to_list() == [30]


def test_daylight_saving_shifts_the_offset_between_summer_and_winter():
    tz = _tz(("person-a", "America/New_York", _EPOCH, None))
    samples = _samples(
        ("person-a", _utc(2000, 7, 1, 3, 30)),   # EDT, UTC-4 -> Jun 30 23:30
        ("person-a", _utc(2000, 1, 1, 4, 30)),   # EST, UTC-5 -> Dec 31 23:30
    )

    out = with_local_keys(samples, "ts", tz)

    assert out["date_key"].to_list() == [date(2000, 6, 30), date(1999, 12, 31)]
    assert out["time_key"].to_list() == [23 * 60 + 30, 23 * 60 + 30]


def test_each_instant_uses_the_zone_covering_it_and_rows_keep_their_order():
    tz = _tz(
        ("person-a", "Asia/Bangkok", _EPOCH, _utc(2000, 3, 1)),
        ("person-a", "UTC", _utc(2000, 3, 1), None),
        ("person-b", "Asia/Bangkok", _EPOCH, None),
    )
    samples = _samples(
        ("person-a", _utc(2000, 3, 1, 12, 0)),   # after the change: UTC, 12:00
        ("person-b", _utc(2000, 3, 1, 12, 0)),   # Bangkok, 19:00
        ("person-a", _utc(2000, 2, 1, 12, 0)),   # before the change: Bangkok, 19:00
    )

    out = with_local_keys(samples, "ts", tz)

    assert out["person_key"].to_list() == ["person-a", "person-b", "person-a"]
    assert out["time_key"].to_list() == [12 * 60, 19 * 60, 19 * 60]


def test_a_person_with_no_covering_timezone_gets_null_keys():
    tz = _tz(("person-a", "Asia/Bangkok", _EPOCH, None))
    samples = _samples(("person-b", _utc(2000, 1, 1, 12, 0)))

    out = with_local_keys(samples, "ts", tz)

    assert out["date_key"].to_list() == [None]
    assert out["time_key"].to_list() == [None]
