from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64
from gadgetbridge_pipeline.defs.owntracks.location_fact import build_fact_location_fix

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)

_PHONES = pl.DataFrame(
    [("user-a/phone-a", "person-a", date(2000, 1, 1), None)],
    schema={"phone_key": pl.String, "person_key": pl.String,
            "valid_from": pl.Date, "valid_to": pl.Date},
    orient="row",
)
_TZ = pl.DataFrame(
    [("person-a", "Asia/Bangkok", _EPOCH, None)],
    schema={"person_key": pl.String, "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    orient="row",
)


def _records(*rows):
    """rows: (user, device, timestamp, lat, lon, acc)"""
    return pl.DataFrame(
        [
            {"id": None, "user": u, "device": d, "year_month": date(2000, 1, 1),
             "timestamp": ts, "lat": lat, "lon": lon, "acc": acc, "alt": 1}
            for u, d, ts, lat, lon, acc in rows
        ],
        schema={"id": pl.String, "user": pl.String, "device": pl.String,
                "year_month": pl.Date, "timestamp": pl.Datetime("us", "UTC"),
                "lat": pl.Float64, "lon": pl.Float64, "acc": pl.Int64, "alt": pl.Int64},
    )


def test_a_record_becomes_a_fix_keyed_by_phone_and_timestamp_with_local_date_and_time_keys():
    ts = datetime(2000, 1, 1, 18, 30, tzinfo=_UTC)  # 01:30 on Jan 2 at UTC+7

    fact = build_fact_location_fix(_records(("user-a", "phone-a", ts, 0.5, 0.25, 12)), _PHONES, _TZ)

    assert fact.row(0, named=True) == {
        "fix_key": stable_hash64("user-a/phone-a", "2000-01-01T18:30:00+00:00"),
        "date_key": date(2000, 1, 2), "time_key": 90,
        "phone_key": "user-a/phone-a", "person_key": "person-a",
        "timestamp_utc": ts, "lat": 0.5, "lon": 0.25, "accuracy_m": 12,
        "year_month": date(2000, 1, 1),
    }


def test_a_record_from_an_unlisted_phone_or_outside_its_period_is_kept_with_unknown_keys():
    inside = datetime(2000, 1, 1, 18, 30, tzinfo=_UTC)
    before = datetime(1999, 12, 31, 18, 30, tzinfo=_UTC)
    records = _records(
        ("user-x", "phone-x", inside, 0.0, 0.0, 5),
        ("user-a", "phone-a", before, 0.0, 0.0, 5),
        ("user-a", "phone-a", inside, 0.0, 0.0, 5),
    )

    fact = build_fact_location_fix(records, _PHONES, _TZ)

    assert fact["phone_key"].to_list() == ["(unknown)", "(unknown)", "user-a/phone-a"]
    assert fact["person_key"].to_list() == ["(unknown)", "(unknown)", "person-a"]
    assert fact["date_key"].to_list()[:2] == [None, None]


def test_an_empty_month_gives_an_empty_fact_with_the_fact_columns():
    fact = build_fact_location_fix(_records(), _PHONES, _TZ)

    assert fact.height == 0
    assert fact.columns == [
        "fix_key", "date_key", "time_key", "phone_key", "person_key",
        "timestamp_utc", "lat", "lon", "accuracy_m", "year_month",
    ]
