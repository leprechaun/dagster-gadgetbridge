from datetime import date, datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.medicine.day import build_derived_medicine_day

_UTC = timezone.utc
_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_NOW = datetime(2000, 1, 5, 12, 0, tzinfo=_UTC)  # Jan 5 in UTC, Jan 5 19:00 in Bangkok


def _d(day):
    return date(2000, 1, day)


def _prescriptions(*rows):
    """rows: (person, medicine, valid_from, valid_to, dose)"""
    return pl.DataFrame(
        [
            (f"{p}/{m}@{vf}", p, m, dose, vf, vt)
            for p, m, vf, vt, dose in rows
        ],
        schema={"prescription_key": pl.String, "person_key": pl.String,
                "medicine_key": pl.String, "dosage_mg": pl.Float64,
                "valid_from": pl.Date, "valid_to": pl.Date},
        orient="row",
    )


def _skips(*rows):
    """rows: (person, date, medicine)"""
    return pl.DataFrame(
        [(d, p, m, -1) for p, d, m in rows],
        schema={"date_key": pl.Date, "person_key": pl.String,
                "medicine_key": pl.String, "time_key": pl.Int64},
        orient="row",
    )


def _checkins(*rows):
    """rows: (person, complete_through)"""
    return pl.DataFrame(
        [(p, datetime(2000, 1, 1, tzinfo=_UTC), t) for p, t in rows],
        schema={"person_key": pl.String, "checked_at_utc": pl.Datetime("us", "UTC"),
                "skips_complete_through": pl.Date},
        orient="row",
    )


def _tz(*people, zone="UTC"):
    return pl.DataFrame(
        [(p, zone, _EPOCH, None) for p in people],
        schema={"person_key": pl.String, "timezone": pl.String,
                "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
        orient="row",
    )


def _build(prescriptions, skips=None, checkins=None, tz=None, now=_NOW):
    people = prescriptions["person_key"].unique().to_list()
    return build_derived_medicine_day(
        prescriptions,
        skips if skips is not None else _skips(),
        checkins if checkins is not None else _checkins(),
        tz if tz is not None else _tz(*people),
        now,
    )


def test_each_prescription_day_up_to_the_persons_today_is_assumed_taken_at_the_prescribed_dose():
    p = _prescriptions(("person-a", "medicine-a", _d(3), None, 10.0))

    out = _build(p).sort("date_key")

    assert out.columns == [
        "date_key", "person_key", "prescription_key", "status", "effective_dosage_mg",
    ]
    assert out.select("date_key", "status", "effective_dosage_mg").rows() == [
        (_d(3), "assumed_taken", 10.0), (_d(4), "assumed_taken", 10.0),
        (_d(5), "assumed_taken", 10.0),
    ]


def test_an_all_skip_covers_every_medicine_of_that_person_only():
    p = _prescriptions(
        ("person-a", "medicine-a", _d(4), _d(4), 10.0),
        ("person-a", "medicine-b", _d(4), _d(4), 5.0),
        ("person-b", "medicine-a", _d(4), _d(4), 10.0),
    )

    skips = _skips(("person-a", _d(4), "(all)"))

    out = _build(p, skips=skips).sort("person_key", "prescription_key")

    assert out.select("person_key", "status", "effective_dosage_mg").rows() == [
        ("person-a", "skipped", 0.0), ("person-a", "skipped", 0.0),
        ("person-b", "assumed_taken", 10.0),
    ]


def test_a_skip_naming_one_medicine_covers_only_that_medicine():
    p = _prescriptions(
        ("person-a", "medicine-a", _d(4), _d(4), 10.0),
        ("person-a", "medicine-b", _d(4), _d(4), 5.0),
    )

    out = _build(p, skips=_skips(("person-a", _d(4), "medicine-a"))).sort("prescription_key")

    assert out["status"].to_list() == ["skipped", "assumed_taken"]


def test_status_flips_from_confirmed_to_assumed_after_the_watermark_and_totals_can_filter_it():
    p = _prescriptions(("person-a", "medicine-a", _d(1), None, 10.0))
    checkins = _checkins(("person-a", _d(2)), ("person-a", _d(3)))  # watermark = latest = Jan 3

    out = _build(p, skips=_skips(("person-a", _d(2), "(all)")), checkins=checkins).sort("date_key")

    assert out["status"].to_list() == [
        "confirmed_taken", "skipped", "confirmed_taken", "assumed_taken", "assumed_taken",
    ]
    confirmed_total = out.filter(pl.col("status") == "confirmed_taken")["effective_dosage_mg"].sum()
    assert confirmed_total == 20.0


def test_overlapping_prescriptions_for_one_medicine_each_get_their_own_row_per_day():
    p = _prescriptions(
        ("person-a", "medicine-a", _d(4), None, 10.0),    # a dose change taking effect on Jan 4
        ("person-a", "medicine-a", _d(3), None, 5.0),     # the old strength, still open
    )

    out = _build(p).filter(pl.col("date_key") == _d(4)).sort("prescription_key")

    assert out["prescription_key"].to_list() == [
        "person-a/medicine-a@2000-01-03", "person-a/medicine-a@2000-01-04",
    ]
    assert out["effective_dosage_mg"].to_list() == [5.0, 10.0]


def test_today_is_each_persons_own_local_date_not_the_utc_date_or_a_fixed_zone():
    late = datetime(2000, 1, 5, 18, 0, tzinfo=_UTC)    # Jan 5 UTC, Jan 6 01:00 in Bangkok
    p = _prescriptions(
        ("person-a", "medicine-a", _d(5), None, 10.0),   # Bangkok
        ("person-b", "medicine-a", _d(5), None, 10.0),   # UTC
    )
    tz = pl.concat([_tz("person-a", zone="Asia/Bangkok"), _tz("person-b", zone="UTC")])

    out = _build(p, tz=tz, now=late).sort("person_key", "date_key")

    assert out.select("person_key", "date_key").rows() == [
        ("person-a", _d(5)), ("person-a", _d(6)), ("person-b", _d(5)),
    ]
