from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.night_rule import apply_night_rule

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)


def _tz(*people, zone="UTC"):
    return pl.DataFrame(
        [(p, zone, _EPOCH, None) for p in people],
        schema={"person_key": pl.String, "timezone": pl.String,
                "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
        orient="row",
    )


def _sessions(*rows):
    """rows: (person_key, (y, m, d, h, min) start, (y, m, d, h, min) end)"""
    return pl.DataFrame(
        [(p, datetime(*s, tzinfo=_UTC), datetime(*e, tzinfo=_UTC)) for p, s, e in rows],
        schema={"person_key": pl.String, "start_utc": pl.Datetime("us", "UTC"),
                "end_utc": pl.Datetime("us", "UTC")},
        orient="row",
    )


def _apply(sessions, *people, zone="UTC"):
    return apply_night_rule(sessions, _tz(*(people or ("person-a",)), zone=zone))


def test_an_overnight_session_is_that_dates_night():
    s = _sessions(("person-a", (2000, 1, 1, 22, 0), (2000, 1, 2, 7, 0)))

    out = _apply(s).row(0, named=True)

    assert out["reporting_date_key"] == date(2000, 1, 2)
    assert out["is_nap"] is False


def test_midpoint_at_1459_stays_on_the_date_and_at_1500_moves_to_the_next_window():
    s = _sessions(
        ("person-a", (2000, 1, 1, 14, 0), (2000, 1, 1, 15, 58)),  # midpoint 14:59
        ("person-b", (2000, 1, 1, 14, 0), (2000, 1, 1, 16, 0)),   # midpoint 15:00
    )

    out = _apply(s, "person-a", "person-b").sort("person_key")

    assert out["reporting_date_key"].to_list() == [date(2000, 1, 1), date(2000, 1, 2)]


def test_a_waking_shorter_than_three_hours_keeps_both_sessions_in_one_block():
    # Alone, the first session (midpoint 13:15) would be Jan 1's; merged, the block's
    # midpoint (17:00) puts both on Jan 2.
    s = _sessions(
        ("person-a", (2000, 1, 1, 12, 0), (2000, 1, 1, 14, 30)),
        ("person-a", (2000, 1, 1, 16, 0), (2000, 1, 1, 22, 0)),  # 1.5 h waking
    )

    out = _apply(s)

    assert out["reporting_date_key"].to_list() == [date(2000, 1, 2), date(2000, 1, 2)]


def test_a_short_nap_after_a_night_is_a_nap_dated_by_its_end_and_the_night_is_the_longest():
    s = _sessions(
        ("person-a", (2000, 1, 1, 22, 0), (2000, 1, 2, 7, 0)),
        ("person-a", (2000, 1, 2, 14, 0), (2000, 1, 2, 14, 40)),  # 7 h after waking
    )

    out = _apply(s).sort("start_utc")

    assert out["is_nap"].to_list() == [False, True]
    assert out["reporting_date_key"].to_list() == [date(2000, 1, 2), date(2000, 1, 2)]


def test_a_nap_that_ends_after_the_boundary_belongs_to_its_own_end_date_not_the_nights():
    s = _sessions(
        ("person-a", (2000, 1, 1, 22, 0), (2000, 1, 2, 7, 0)),
        ("person-a", (2000, 1, 2, 15, 30), (2000, 1, 2, 16, 10)),  # midpoint 15:50 -> Jan 3 window
        ("person-a", (2000, 1, 2, 23, 0), (2000, 1, 3, 7, 0)),
    )

    out = _apply(s).sort("start_utc")

    assert out["is_nap"].to_list() == [False, True, False]
    assert out["reporting_date_key"].to_list() == [
        date(2000, 1, 2), date(2000, 1, 2), date(2000, 1, 3),
    ]


def test_equal_length_blocks_in_one_window_make_the_earliest_the_night():
    s = _sessions(
        ("person-a", (2000, 1, 1, 20, 0), (2000, 1, 1, 22, 0)),
        ("person-a", (2000, 1, 2, 2, 0), (2000, 1, 2, 4, 0)),
        ("person-a", (2000, 1, 2, 9, 0), (2000, 1, 2, 11, 0)),
    )

    out = _apply(s).sort("start_utc")

    assert out["is_nap"].to_list() == [False, True, True]


def test_a_long_block_straddling_the_boundary_goes_by_its_midpoint_not_its_start_or_end():
    # 10:00 to 22:00 midpoint 16:00 -> the next window, though it starts before 15:00.
    s = _sessions(("person-a", (2000, 1, 1, 10, 0), (2000, 1, 1, 22, 0)))

    assert _apply(s)["reporting_date_key"].to_list() == [date(2000, 1, 2)]


def test_persons_are_never_merged_into_each_others_blocks():
    s = _sessions(
        ("person-a", (2000, 1, 1, 22, 0), (2000, 1, 2, 7, 0)),
        ("person-b", (2000, 1, 2, 7, 30), (2000, 1, 2, 9, 0)),  # 30 min after person-a wakes
    )

    out = _apply(s, "person-a", "person-b").sort("person_key")

    assert out["is_nap"].to_list() == [False, False]  # each is their own Person's Night
