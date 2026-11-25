from datetime import date

import polars as pl
import pytest

from gadgetbridge_pipeline.defs.medicine.dimensions import (
    build_dim_medicine,
    build_dim_prescription,
    find_reserved_medicine_names,
    find_unknown_person_ids,
    require_person_id,
)


def test_a_csv_without_a_person_id_column_is_rejected_not_defaulted():
    frame = pl.DataFrame({"date": [date(2000, 1, 1)]})

    with pytest.raises(ValueError, match="person_id"):
        require_person_id(frame, "medicine_skips.csv")


def test_a_csv_with_a_person_id_column_passes_through_unchanged():
    frame = pl.DataFrame({"person_id": ["person-a"], "date": [date(2000, 1, 1)]})

    assert require_person_id(frame, "medicine_skips.csv").equals(frame)


def test_null_and_unlisted_person_ids_are_reported_and_known_ones_are_not():
    persons = pl.DataFrame({"person_id": ["person-a", "person-b"]})
    frame = pl.DataFrame({"person_id": ["person-a", None, "person-z", "person-z"]})

    assert find_unknown_person_ids(frame, persons) == [None, "person-z"]


def test_medicine_names_that_collide_with_reserved_members_are_reported():
    prescriptions = pl.DataFrame({"medicine": ["medicine-a", "(all)", "(unknown)"]})

    assert find_reserved_medicine_names(prescriptions) == ["(all)", "(unknown)"]


def test_dim_medicine_has_each_medicine_once_plus_the_reserved_all_and_unknown_members():
    prescriptions = pl.DataFrame({"medicine": ["medicine-b", "medicine-a", "medicine-b"]})

    dim = build_dim_medicine(prescriptions)

    assert dim["medicine_key"].to_list() == ["(all)", "(unknown)", "medicine-a", "medicine-b"]


def test_dim_prescription_key_is_person_medicine_and_valid_from_and_carries_the_dose():
    prescriptions = pl.DataFrame({
        "person_id": ["person-a", "person-a"],
        "medicine": ["medicine-a", "medicine-a"],
        "start_date": [date(2000, 1, 1), date(2000, 6, 1)],
        "end_date": [date(2000, 5, 31), None],
        "dosage_mg": [10.0, 20.0],
    })

    dim = build_dim_prescription(prescriptions)

    assert dim.columns == [
        "prescription_key", "person_key", "medicine_key", "dosage_mg", "valid_from", "valid_to",
    ]
    assert dim["prescription_key"].to_list() == [
        "person-a/medicine-a@2000-01-01", "person-a/medicine-a@2000-06-01",
    ]
    assert dim["dosage_mg"].to_list() == [10.0, 20.0]
    assert dim["valid_to"].to_list() == [date(2000, 5, 31), None]
