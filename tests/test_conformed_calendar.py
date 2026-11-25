from datetime import date, datetime, timedelta, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.calendar import (
    build_dim_date,
    build_dim_time_of_day,
    build_person_timezone,
    find_invalid_timezones,
    find_persons_without_one_covering_timezone,
)

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)


def _tz(*rows):
    return pl.DataFrame(
        list(rows),
        schema={
            "person_key": pl.String,
            "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"),
            "valid_to": pl.Datetime("us", "UTC"),
        },
        orient="row",
    )


# build_dim_date

def test_dim_date_has_one_typed_row_per_day_inclusive():
    dim = build_dim_date(date(2000, 1, 1), date(2000, 1, 3))

    assert dim["date_key"].to_list() == [date(2000, 1, 1), date(2000, 1, 2), date(2000, 1, 3)]
    assert dim.schema["date_key"] == pl.Date


def test_dim_date_attributes_for_a_known_saturday():
    dim = build_dim_date(date(2000, 1, 1), date(2000, 1, 2))  # Sat, Sun

    assert dim["weekday"].to_list() == [6, 7]
    assert dim["is_weekend"].to_list() == [True, True]
    assert dim["iso_week"].to_list() == [52, 52]  # 1999-W52
    assert (dim["year"][0], dim["month"][0]) == (2000, 1)


# build_dim_time_of_day

def test_time_of_day_has_every_minute_plus_whole_day():
    dim = build_dim_time_of_day()

    assert dim.height == 1441
    assert dim["time_key"].n_unique() == 1441
    row = dim.filter(pl.col("time_key") == 15 * 60 + 5).row(0, named=True)
    assert (row["label"], row["hour"], row["minute"]) == ("15:05", 15, 5)


def test_whole_day_member_has_key_minus_one_and_no_hour_or_minute():
    row = build_dim_time_of_day().filter(pl.col("time_key") == -1).row(0, named=True)

    assert (row["label"], row["hour"], row["minute"]) == ("Whole day", None, None)


# build_person_timezone

def test_each_person_gets_one_open_ended_bangkok_row_from_the_epoch():
    persons = pl.DataFrame({"person_id": ["person-a", "person-b"], "display_name": ["A", "B"]})

    tz = build_person_timezone(persons)

    assert tz["person_key"].to_list() == ["person-a", "person-b"]
    assert tz["timezone"].to_list() == ["Asia/Bangkok"] * 2
    assert tz["valid_from"].to_list() == [_EPOCH] * 2
    assert tz["valid_to"].to_list() == [None] * 2


# find_persons_without_one_covering_timezone

def test_a_gapless_chain_ending_open_covers_every_instant():
    tz = _tz(
        ("person-a", "Asia/Bangkok", _EPOCH, datetime(2000, 3, 1, tzinfo=_UTC)),
        ("person-a", "UTC", datetime(2000, 3, 1, tzinfo=_UTC), None),
    )

    assert find_persons_without_one_covering_timezone(["person-a"], tz) == []


def test_a_person_with_no_row_is_reported():
    tz = _tz(("person-a", "Asia/Bangkok", _EPOCH, None))

    assert find_persons_without_one_covering_timezone(["person-a", "person-b"], tz) == ["person-b"]


def test_a_gap_an_overlap_or_a_closed_end_are_reported():
    cut = datetime(2000, 3, 1, tzinfo=_UTC)
    tz = _tz(
        ("gap", "UTC", _EPOCH, cut),
        ("gap", "UTC", cut + timedelta(days=1), None),
        ("overlap", "UTC", _EPOCH, cut + timedelta(days=1)),
        ("overlap", "UTC", cut, None),
        ("closed", "UTC", _EPOCH, cut),
        ("late-start", "UTC", cut, None),
    )

    assert find_persons_without_one_covering_timezone(
        ["gap", "overlap", "closed", "late-start"], tz
    ) == ["closed", "gap", "late-start", "overlap"]


# find_invalid_timezones

def test_unknown_iana_names_are_reported():
    tz = _tz(
        ("person-a", "Asia/Bangkok", _EPOCH, None),
        ("person-b", "Mars/Olympus", _EPOCH, None),
    )

    assert find_invalid_timezones(tz) == ["Mars/Olympus"]
