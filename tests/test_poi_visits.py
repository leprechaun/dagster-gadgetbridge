from datetime import date, datetime, timedelta, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.constants import VISIT_GAP
from gadgetbridge_pipeline.defs.owntracks.visits import (
    build_derived_fix_time_allocation,
    build_derived_poi_visit,
)

_UTC = timezone.utc


def _t(minute, day=1):
    return datetime(2000, 1, day, tzinfo=_UTC) + timedelta(minutes=minute)


def _fixes(*rows):
    """rows: (fix_key, phone_key, person_key, minute_offset)"""
    return pl.DataFrame(
        [(k, ph, pe, _t(m)) for k, ph, pe, m in rows],
        schema={"fix_key": pl.Int64, "phone_key": pl.String, "person_key": pl.String,
                "timestamp_utc": pl.Datetime("us", "UTC")},
        orient="row",
    )


def test_a_fix_owns_the_time_to_the_next_fix_of_its_phone_and_the_last_fix_owns_none():
    fixes = _fixes(
        (1, "phone-a", "person-a", 0), (2, "phone-a", "person-a", 5),
        (3, "phone-a", "person-a", 7),
        (4, "phone-b", "person-b", 0), (5, "phone-b", "person-b", 1),
    )

    out = build_derived_fix_time_allocation(fixes).sort("fix_key")

    assert out.columns == ["fix_key", "phone_key", "person_key", "seconds_owned"]
    assert out["seconds_owned"].to_list() == [300, 120, 0, 60, 0]
    assert out["phone_key"].to_list() == ["phone-a"] * 3 + ["phone-b"] * 2


def test_a_gap_over_the_threshold_caps_ownership_but_a_gap_of_exactly_the_threshold_counts():
    gap = int(VISIT_GAP.total_seconds() // 60)
    fixes = _fixes(
        (1, "phone-a", "person-a", 0),
        (2, "phone-a", "person-a", gap),            # exactly the threshold after fix 1
        (3, "phone-a", "person-a", gap + gap + 1),  # one minute over the threshold after fix 2
        (4, "phone-a", "person-a", gap + gap + 2),
    )

    out = build_derived_fix_time_allocation(fixes).sort("fix_key")

    assert out["seconds_owned"].to_list() == [gap * 60, 0, 60, 0]
    assert out["seconds_owned"].max() <= VISIT_GAP.total_seconds()


_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_TZ = pl.DataFrame(
    [("person-a", "UTC", _EPOCH, None)],
    schema={"person_key": pl.String, "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    orient="row",
)


def _bridge(*rows):
    """rows: (fix_key, poi_key); weights are irrelevant to visits"""
    return pl.DataFrame(
        [(k, p, 1.0) for k, p in rows],
        schema={"fix_key": pl.Int64, "poi_key": pl.String, "weight": pl.Float64},
        orient="row",
    )


def test_a_visit_runs_over_consecutive_fixes_in_a_poi_and_ends_on_an_outside_fix_or_a_long_gap():
    fixes = _fixes(*[
        (i, "phone-a", "person-a", m)
        for i, m in enumerate([0, 5, 10, 20, 30, 35, 100, 101], start=1)
    ])
    bridge = _bridge(
        (1, "example-cafe"), (2, "example-cafe"), (3, "example-cafe"),
        (4, "(none)"),                                           # outside: ends the visit
        (5, "example-cafe"), (6, "example-cafe"),                # a new visit
        (7, "example-cafe"), (8, "example-cafe"),                # 65 min later: a new visit
    )

    out = build_derived_poi_visit(fixes, bridge, _TZ).sort("start_utc")

    assert out.columns == [
        "phone_key", "person_key", "poi_key", "start_date_key", "end_date_key",
        "start_utc", "end_utc", "duration_minutes",
    ]
    assert out.select("phone_key", "person_key", "poi_key", "duration_minutes").rows() == [
        ("phone-a", "person-a", "example-cafe", 10),
        ("phone-a", "person-a", "example-cafe", 5),
        ("phone-a", "person-a", "example-cafe", 1),
    ]
    assert out["start_date_key"].to_list() == [date(2000, 1, 1)] * 3


def test_overlapping_pois_each_get_their_own_visit_and_none_only_fixes_make_no_visit():
    fixes = _fixes(*[(i, "phone-a", "person-a", i * 5) for i in (1, 2, 3)])
    bridge = _bridge(
        (1, "example-cafe-a"), (1, "example-cafe-b"),
        (2, "example-cafe-a"), (2, "example-cafe-b"),
        (3, "(none)"),
    )

    out = build_derived_poi_visit(fixes, bridge, _TZ).sort("poi_key")

    assert out["poi_key"].to_list() == ["example-cafe-a", "example-cafe-b"]
    assert out["duration_minutes"].to_list() == [5, 5]
