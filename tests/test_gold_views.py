from datetime import date

import polars as pl

from gadgetbridge_pipeline.defs.medicine.adherence import build_derived_medicine_adherence_daily
from gadgetbridge_pipeline.defs.owntracks.poi_time import build_derived_poi_time_daily

_D1 = date(2000, 1, 1)


def test_time_per_poi_is_weighted_so_overlapping_pois_split_it_and_unweighted_counts_presence():
    fixes = pl.DataFrame({
        "fix_key": [1, 2, 3], "person_key": ["person-a", "person-a", "person-b"],
        "date_key": [_D1, _D1, _D1],
    })
    bridge = pl.DataFrame({
        "fix_key": [1, 1, 2, 3], "poi_key": ["cafe-a", "cafe-b", "(none)", "cafe-a"],
        "weight": [0.5, 0.5, 1.0, 1.0],
    })
    allocation = pl.DataFrame({"fix_key": [1, 2, 3], "seconds_owned": [600, 300, 120]})

    out = build_derived_poi_time_daily(fixes, bridge, allocation).sort("person_key", "poi_key")

    assert out.columns == [
        "person_key", "date_key", "poi_key", "weighted_seconds", "unweighted_seconds", "fix_count",
    ]
    assert out.rows() == [
        ("person-a", _D1, "(none)", 300.0, 300, 1),
        ("person-a", _D1, "cafe-a", 300.0, 600, 1),
        ("person-a", _D1, "cafe-b", 300.0, 600, 1),
        ("person-b", _D1, "cafe-a", 120.0, 120, 1),
    ]
    person_a = out.filter(pl.col("person_key") == "person-a")
    assert person_a["weighted_seconds"].sum() == 900.0   # a partition: equals total owned time


def test_daily_adherence_separates_confirmed_assumed_and_skipped_doses_per_person():
    days = pl.DataFrame({
        "date_key": [_D1] * 4, "person_key": ["person-a"] * 3 + ["person-b"],
        "prescription_key": ["a/m1", "a/m2", "a/m3", "b/m1"],
        "status": ["confirmed_taken", "assumed_taken", "skipped", "confirmed_taken"],
        "effective_dosage_mg": [10.0, 5.0, 0.0, 20.0],
    })
    prescriptions = pl.DataFrame({
        "prescription_key": ["a/m1", "a/m2", "a/m3", "b/m1"],
        "dosage_mg": [10.0, 5.0, 2.0, 20.0],
    })

    out = build_derived_medicine_adherence_daily(days, prescriptions).sort("person_key")

    assert out.columns == [
        "person_key", "date_key", "prescribed_mg", "confirmed_mg", "assumed_mg", "skipped_count",
    ]
    assert out.rows() == [
        ("person-a", _D1, 17.0, 10.0, 5.0, 1),
        ("person-b", _D1, 20.0, 20.0, 0.0, 0),
    ]
