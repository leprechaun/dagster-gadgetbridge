from datetime import date

import polars as pl

from gadgetbridge_pipeline.defs.conformed.dimensions import (
    build_dim_device,
    build_dim_person,
    build_dim_phone,
)
from gadgetbridge_pipeline.defs.conformed.validation import (
    find_duplicate_ids,
    find_invalid_kinds,
    find_multi_owner_devices,
    find_overlapping_periods,
)

_DEVICE_SCHEMA = {
    "person_id": pl.String,
    "kind": pl.String,
    "natural_key": pl.String,
    "valid_from": pl.Date,
    "valid_to": pl.Date,
}


def _devices(*rows):
    return pl.DataFrame(list(rows), schema=_DEVICE_SCHEMA, orient="row")


# build_dim_device

def test_device_key_includes_valid_from_so_a_reused_id_is_a_different_device():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), date(2000, 6, 30)),
        ("person-b", "wearable", "1", date(2000, 7, 1), None),
    )

    dim = build_dim_device(devices)

    assert dim["device_key"].to_list() == ["1@2000-01-01", "1@2000-07-01"]
    assert dim["person_key"].to_list() == ["person-a", "person-b"]


def test_dim_device_excludes_phones():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), None),
        ("person-a", "phone", "user-a/phone-a", date(2000, 1, 1), None),
    )

    assert build_dim_device(devices)["device_key"].to_list() == ["1@2000-01-01"]


# build_dim_phone

def test_dim_phone_keys_on_owntracks_user_device_and_excludes_wearables():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), None),
        ("person-a", "phone", "user-a/phone-a", date(2000, 1, 1), None),
    )

    dim = build_dim_phone(devices)

    assert dim["phone_key"].to_list() == ["user-a/phone-a"]
    assert dim["person_key"].to_list() == ["person-a"]


# build_dim_person

def test_dim_person_key_is_the_person_id_slug():
    persons = pl.DataFrame(
        {"person_id": ["person-a", "person-b"], "display_name": ["Person A", "Person B"]}
    )

    dim = build_dim_person(persons)

    assert dim["person_key"].to_list() == ["person-a", "person-b"]
    assert dim["display_name"].to_list() == ["Person A", "Person B"]


# find_overlapping_periods

def test_same_natural_key_with_overlapping_periods_is_reported():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), date(2000, 6, 30)),
        ("person-a", "wearable", "1", date(2000, 6, 30), None),
    )

    assert find_overlapping_periods(devices) == ["1"]


def test_back_to_back_periods_do_not_overlap():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), date(2000, 6, 30)),
        ("person-a", "wearable", "1", date(2000, 7, 1), None),
    )

    assert find_overlapping_periods(devices) == []


def test_open_ended_period_overlaps_any_later_period_of_the_same_key():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), None),
        ("person-a", "wearable", "1", date(2005, 1, 1), None),
    )

    assert find_overlapping_periods(devices) == ["1"]


def test_different_natural_keys_never_overlap_each_other():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), None),
        ("person-b", "wearable", "2", date(2000, 1, 1), None),
    )

    assert find_overlapping_periods(devices) == []


# find_multi_owner_devices

def test_natural_key_owned_by_two_persons_is_reported():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), date(2000, 6, 30)),
        ("person-b", "wearable", "1", date(2000, 7, 1), None),
    )

    assert find_multi_owner_devices(devices) == ["1"]


def test_same_owner_across_several_periods_is_fine():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), date(2000, 6, 30)),
        ("person-a", "wearable", "1", date(2000, 7, 1), None),
    )

    assert find_multi_owner_devices(devices) == []


# find_invalid_kinds

def test_kind_other_than_wearable_or_phone_or_missing_is_reported():
    devices = _devices(
        ("person-a", "wearable", "1", date(2000, 1, 1), None),
        ("person-a", "watch", "2", date(2000, 1, 1), None),
        ("person-a", None, "3", date(2000, 1, 1), None),
    )

    assert find_invalid_kinds(devices) == ["2", "3"]


# find_duplicate_ids

def test_duplicate_ids_are_reported_once():
    ids = pl.Series(["person-a", "person-b", "person-a", "person-a"])

    assert find_duplicate_ids(ids) == ["person-a"]


def test_unique_ids_report_nothing():
    assert find_duplicate_ids(pl.Series(["person-a", "person-b"])) == []
