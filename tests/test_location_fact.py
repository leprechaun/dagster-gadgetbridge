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
    """rows: (user, device, timestamp, lat, lon, acc[, created_at_minute, id])"""
    def row(u, d, ts, lat, lon, acc, created=0, rid=None):
        return {"id": rid, "user": u, "device": d, "year_month": date(2000, 1, 1),
                "timestamp": ts, "created_at": datetime(2000, 1, 1, 0, created, tzinfo=_UTC),
                "lat": lat, "lon": lon, "acc": acc, "alt": 1}
    return pl.DataFrame(
        [row(*r) for r in rows],
        schema={"id": pl.String, "user": pl.String, "device": pl.String,
                "year_month": pl.Date, "timestamp": pl.Datetime("us", "UTC"),
                "created_at": pl.Datetime("us", "UTC"),
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

    fact = fact.sort("phone_key", "timestamp_utc")
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


def test_two_unlisted_phones_at_the_same_instant_get_different_keys_not_a_collision():
    ts = datetime(2000, 1, 1, 18, 30, tzinfo=_UTC)
    records = _records(
        ("user-x", "phone-x", ts, 0.0, 0.0, 5), ("user-y", "phone-y", ts, 0.0, 0.0, 5)
    )

    fact = build_fact_location_fix(records, _PHONES, _TZ)

    assert fact["fix_key"].n_unique() == 2
    assert fact["phone_key"].to_list() == ["(unknown)", "(unknown)"]


def test_a_resent_record_collapses_to_one_fix():
    ts = datetime(2000, 1, 1, 18, 30, tzinfo=_UTC)
    same = ("user-a", "phone-a", ts, 0.5, 0.25, 12)

    fact = build_fact_location_fix(_records(same, same), _PHONES, _TZ)

    assert fact.height == 1


def test_two_records_in_the_same_second_keep_the_most_accurate_then_the_latest_created():
    ts = datetime(2000, 1, 1, 18, 30, tzinfo=_UTC)
    records = _records(
        ("user-a", "phone-a", ts, 1.0, 1.0, 50, 0, "a"),   # coarse
        ("user-a", "phone-a", ts, 2.0, 2.0, 10, 1, "b"),   # most accurate, created earlier
        ("user-a", "phone-a", ts, 3.0, 3.0, 10, 5, "c"),   # equally accurate, created later: wins
    )

    fact = build_fact_location_fix(records, _PHONES, _TZ)

    assert fact["lat"].to_list() == [3.0]
    assert fact["accuracy_m"].to_list() == [10]
