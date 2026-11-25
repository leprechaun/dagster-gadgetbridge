"""Pure blocking-check logic shared by every fact build."""

from __future__ import annotations

import polars as pl


def find_duplicate_grain(fact: pl.DataFrame, grain: list[str]) -> list[tuple]:
    """Grain values that appear on more than one row."""
    dupes = fact.filter(pl.struct(grain).is_duplicated()).select(grain).unique().sort(grain)
    return [tuple(r) for r in dupes.iter_rows()]


def find_unresolved_keys(fact: pl.DataFrame, key_col: str, dim: pl.DataFrame) -> list:
    """Distinct values of `key_col` that are null, `(unknown)`, or not a key
    in `dim` (which carries the same-named column).
    """
    known = set(dim[key_col].to_list())
    unresolved = [
        k for k in fact[key_col].unique().to_list()
        if k is None or k == "(unknown)" or k not in known
    ]
    return sorted(unresolved, key=lambda k: (k is not None, str(k)))


def find_person_device_mismatches(fact: pl.DataFrame, dim_device: pl.DataFrame) -> list[tuple]:
    """(device_key, person_key) pairs on the fact that disagree with the
    Device's owner in `dim_device`. Devices missing from the dimension are
    left to the key-resolution check.
    """
    owner = dim_device.select("device_key", pl.col("person_key").alias("_owner"))
    bad = (
        fact.select("device_key", "person_key").unique()
        .join(owner, on="device_key", how="inner")
        .filter(pl.col("person_key") != pl.col("_owner"))
        .sort("device_key", "person_key")
    )
    return [(r[0], r[1]) for r in bad.iter_rows()]
