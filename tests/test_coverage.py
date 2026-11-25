from datetime import date, datetime, timedelta, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.coverage import (
    build_dim_heart_rate_bin,
    build_heart_rate_distribution_daily,
    build_wear_coverage,
)


def _fact(*rows):
    """rows: (person_key, date_key, time_key, heart_rate_bpm)"""
    return pl.DataFrame(
        [
            {
                "person_key": p, "date_key": d, "time_key": t, "heart_rate_bpm": hr,
                "timestamp_utc": datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
                + timedelta(minutes=t),
            }
            for p, d, t, hr in rows
        ],
        schema={
            "person_key": pl.String, "date_key": pl.Date, "time_key": pl.Int64,
            "heart_rate_bpm": pl.Int64, "timestamp_utc": pl.Datetime("us", "UTC"),
        },
    )


_D1 = date(2000, 1, 1)


def test_heart_rate_bins_are_5_bpm_wide_keyed_by_lower_bound_up_to_254():
    dim = build_dim_heart_rate_bin()

    assert dim["heart_rate_bin_key"].to_list() == list(range(0, 255, 5))
    assert dim["width_bpm"].unique().to_list() == [5]


# build_heart_rate_distribution_daily

def test_readings_are_counted_per_person_date_and_bin_and_never_pooled_across_persons():
    fact = _fact(
        ("person-a", _D1, 0, 62), ("person-a", _D1, 1, 64), ("person-a", _D1, 2, 65),
        ("person-b", _D1, 0, 63),
    )

    out = build_heart_rate_distribution_daily(fact).sort("person_key", "heart_rate_bin_key")

    assert out.select("person_key", "date_key", "heart_rate_bin_key", "sample_count").rows() == [
        ("person-a", _D1, 60, 2), ("person-a", _D1, 65, 1), ("person-b", _D1, 60, 1),
    ]


# build_wear_coverage

def test_only_minutes_with_a_heart_rate_count_as_worn_per_calendar_day():
    fact = _fact(
        ("person-a", _D1, 0, 60), ("person-a", _D1, 1, 61), ("person-a", _D1, 2, None),
    )

    out = build_wear_coverage(fact).filter(pl.col("date_key") == _D1)

    assert out["minutes_worn_date"].to_list() == [2]


def test_night_window_runs_from_1500_the_day_before_to_1500_on_the_date():
    d2 = date(2000, 1, 2)
    fact = _fact(
        ("person-a", _D1, 14 * 60 + 59, 60),  # 14:59 -> Night of Jan 1
        ("person-a", _D1, 15 * 60, 60),       # 15:00 -> Night of Jan 2
    )

    out = build_wear_coverage(fact).sort("date_key")

    assert out.select("date_key", "minutes_worn_date", "minutes_worn_night_window").rows() == [
        (_D1, 2, 1),
        (d2, 0, 1),
    ]


def test_an_off_wrist_gap_straddling_midnight_fails_the_night_window_while_both_days_pass():
    d2, d3 = date(2000, 1, 2), date(2000, 1, 3)
    # Worn all day on Jan 1-3 except 22:00 on Jan 1 to 03:00 on Jan 2 (5 hours).
    minutes = [
        ("person-a", d, t, 60)
        for d in (_D1, d2, d3)
        for t in range(1440)
        if not ((d == _D1 and t >= 22 * 60) or (d == d2 and t < 3 * 60))
    ]

    out = build_wear_coverage(_fact(*minutes)).filter(pl.col("date_key") == d2).row(0, named=True)

    assert out["minutes_worn_date"] == 21 * 60                  # 21 h: day passes
    assert out["minutes_worn_night_window"] == 19 * 60          # 19 h: Night window fails
    assert out["covered_date"] is True
    assert out["covered_night_window"] is False
