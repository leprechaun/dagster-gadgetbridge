from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.devices import resolve_phone
from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_person_device_mismatches,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys
from gadgetbridge_pipeline.defs.owntracks.bronze import owntracks_partitions


def build_fact_location_fix(
    location_records: pl.DataFrame, dim_phone: pl.DataFrame, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """One row per Fix: observations only, because this fact is monthly-partitioned.

    The key is a stable hash of phone and timestamp; the record `id` is absent
    in older data so it can't be the key. A Fix is one position at an instant, so
    records sharing a phone and a second (resends, or several location providers)
    collapse to the most accurate one, then the latest created, then by `id`.
    """
    one_per_instant = location_records.sort(
        ["user", "device", "timestamp", "acc", "created_at", "id"],
        descending=[False, False, False, False, True, False],
        nulls_last=True,
    ).unique(subset=["user", "device", "timestamp"], keep="first", maintain_order=True)
    with_phone = one_per_instant.with_columns(
        (pl.col("user") + "/" + pl.col("device")).alias("_phone")
    )
    resolved = resolve_phone(with_phone, "_phone", "timestamp", dim_phone).select(
        "_phone", "phone_key", "person_key",
        pl.col("timestamp").alias("timestamp_utc"),
        "lat", "lon",
        pl.col("acc").alias("accuracy_m"),
        "year_month",
    )
    keyed = with_local_keys(resolved, "timestamp_utc", person_timezone)
    # Hash the raw `user/device`, not the resolved phone_key: two different unlisted
    # phones are both `(unknown)` there and would collide. For a listed phone the
    # two are identical, so its keys are unchanged.
    fix_keys = [
        stable_hash64(phone, ts.isoformat())
        for phone, ts in zip(keyed["_phone"], keyed["timestamp_utc"], strict=True)
    ]
    return keyed.with_columns(pl.Series("fix_key", fix_keys, dtype=pl.Int64)).select(
        "fix_key", "date_key", "time_key", "phone_key", "person_key",
        "timestamp_utc", "lat", "lon", "accuracy_m", "year_month",
    )


_KEY = dg.AssetKey(["owntracks", "gold", "fact_location_fix"])
_CONFORMED = ["conformed", "gold"]
_CHECKS = ["fix_key_unique", "fix_keys_resolve", "fix_person_matches_phone"]


@dg.asset(
    name="fact_location_fix",
    group_name="owntracks",
    key_prefix=["owntracks", "gold"],
    io_manager_key="owntracks_deltalake_io_manager",
    kinds={"deltalake"},
    partitions_def=owntracks_partitions,
    metadata={"partition_expr": "year_month"},
    description=(
        "One Fix: a phone's position at an instant. Monthly-partitioned and observations "
        "only, so POI matching and other policy live in derived tables."
    ),
    ins={
        "location_records": dg.AssetIn(key=["owntracks", "bronze", "location_records"]),
        "dim_phone": dg.AssetIn(key=[*_CONFORMED, "dim_phone"]),
        "person_timezone": dg.AssetIn(key=[*_CONFORMED, "person_timezone"]),
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
        "dim_time_of_day": dg.AssetIn(key=[*_CONFORMED, "dim_time_of_day"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
    },
    check_specs=[dg.AssetCheckSpec(name=n, asset=_KEY, blocking=True) for n in _CHECKS],
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_location_fix(
    location_records: pl.DataFrame,
    dim_phone: pl.DataFrame,
    person_timezone: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_time_of_day: pl.DataFrame,
    dim_person: pl.DataFrame,
):
    fact = build_fact_location_fix(location_records, dim_phone, person_timezone)
    yield dg.Output(
        fact,
        metadata={
            "records_in": location_records.height,
            "fixes_out": fact.height,
            "duplicates_collapsed": location_records.height - fact.height,
        },
    )

    dupes = find_duplicate_grain(fact, ["fix_key"])
    yield dg.AssetCheckResult(
        check_name="fix_key_unique", passed=not dupes,
        metadata={"duplicate_keys": str(dupes[:20])},
    )
    unresolved = {
        key: find_unresolved_keys(fact, key, dim)[:20]
        for key, dim in {
            "date_key": dim_date, "time_key": dim_time_of_day,
            "phone_key": dim_phone, "person_key": dim_person,
        }.items()
    }
    yield dg.AssetCheckResult(
        check_name="fix_keys_resolve", passed=not any(unresolved.values()),
        metadata={"unresolved": str(unresolved)},
    )
    bad = find_person_device_mismatches(fact, dim_phone, key_col="phone_key")
    yield dg.AssetCheckResult(
        check_name="fix_person_matches_phone", passed=not bad,
        metadata={"mismatches": str(bad[:20])},
    )


defs = dg.Definitions(assets=[fact_location_fix])
