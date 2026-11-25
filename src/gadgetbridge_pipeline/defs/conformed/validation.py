from __future__ import annotations

from itertools import pairwise

import polars as pl

ALLOWED_KINDS = {"wearable", "phone"}


def find_overlapping_periods(devices: pl.DataFrame) -> list[str]:
    """natural_keys with two periods of use that overlap. `valid_to` is the
    last day of use (inclusive); a null `valid_to` means still in use.
    """
    bad: set[str] = set()
    for key, group in devices.group_by("natural_key"):
        periods = sorted(
            group.select("valid_from", "valid_to").iter_rows(),
            key=lambda p: p[0],
        )
        for (_, prev_to), (next_from, _) in pairwise(periods):
            if prev_to is None or next_from <= prev_to:
                bad.add(key[0])
    return sorted(bad)


def find_multi_owner_devices(devices: pl.DataFrame) -> list[str]:
    """natural_keys listed under more than one person: a Device never changes owner."""
    counts = devices.group_by("natural_key").agg(pl.col("person_id").n_unique().alias("owners"))
    return sorted(counts.filter(pl.col("owners") > 1)["natural_key"].to_list())


def find_invalid_kinds(devices: pl.DataFrame) -> list[str]:
    """natural_keys whose kind is missing or not one of ALLOWED_KINDS."""
    bad = devices.filter(pl.col("kind").is_null() | ~pl.col("kind").is_in(ALLOWED_KINDS))
    return sorted(bad["natural_key"].to_list())


def find_duplicate_ids(ids: pl.Series) -> list[str]:
    return sorted(ids.filter(ids.is_duplicated()).unique().to_list())
