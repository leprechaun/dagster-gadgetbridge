import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_person_device_mismatches,
    find_unresolved_keys,
)


def test_rows_sharing_the_grain_columns_are_reported_once_each():
    fact = pl.DataFrame({
        "device_key": ["d1", "d1", "d1", "d2"],
        "minute": [1, 1, 2, 1],
    })

    assert find_duplicate_grain(fact, ["device_key", "minute"]) == [("d1", 1)]


def test_keys_that_are_null_unknown_or_absent_from_the_dimension_are_unresolved():
    fact = pl.DataFrame({"device_key": ["d1", "(unknown)", None, "d9"]})
    dim = pl.DataFrame({"device_key": ["d1", "d2"]})

    assert find_unresolved_keys(fact, "device_key", dim) == [None, "(unknown)", "d9"]


def test_all_keys_resolving_reports_nothing():
    fact = pl.DataFrame({"device_key": ["d1", "d1", "d2"]})
    dim = pl.DataFrame({"device_key": ["d1", "d2"]})

    assert find_unresolved_keys(fact, "device_key", dim) == []


def test_a_row_whose_person_differs_from_its_devices_owner_is_reported():
    fact = pl.DataFrame({
        "device_key": ["d1", "d1", "d2"],
        "person_key": ["person-a", "person-b", "person-b"],
    })
    dim = pl.DataFrame({"device_key": ["d1", "d2"], "person_key": ["person-a", "person-b"]})

    assert find_person_device_mismatches(fact, dim) == [("d1", "person-b")]
