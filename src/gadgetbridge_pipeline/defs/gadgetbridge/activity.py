from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.devices import resolve_device
from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_person_device_mismatches,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys

ASLEEP_KIND = 120
HEART_RATE_NO_READING = 255  # "no reading", not a real value
UNKNOWN_KEY = "(unknown)"
INVALID_ROW_KIND = 255  # device sentinel: the whole row carries no data


def _invalid_row() -> pl.Expr:
    """The device's whole-row sentinel: every value column is 255 at once."""
    return (
        (pl.col("STEPS") == 255) & (pl.col("RAW_INTENSITY") == 255)
        & (pl.col("RAW_KIND") == INVALID_ROW_KIND) & (pl.col("HEART_RATE") == 255)
    )


def build_dim_activity_kind() -> pl.DataFrame:
    return pl.DataFrame({"activity_kind_key": pl.int_range(0, 256, eager=True)}).with_columns(
        pl.when(pl.col("activity_kind_key") == INVALID_ROW_KIND)
        .then(pl.lit("invalid row"))
        .alias("description"),
        (pl.col("activity_kind_key") == ASLEEP_KIND).alias("is_asleep"),
    )


def build_fact_activity_minute(
    bronze: pl.DataFrame, dim_device: pl.DataFrame, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """One row per Device and minute, from `huami_extended_activity_sample`."""
    joined = resolve_device(bronze, "DEVICE_ID", "TIMESTAMP", dim_device).select(
        pl.col("TIMESTAMP").alias("timestamp_utc"),
        "device_key", "person_key",
        pl.col("RAW_KIND").alias("activity_kind_key"),
        pl.when(~_invalid_row()).then(pl.col("STEPS")).alias("steps"),
        pl.when((pl.col("HEART_RATE") != HEART_RATE_NO_READING) & ~_invalid_row())
        .then(pl.col("HEART_RATE")).alias("heart_rate_bpm"),
        pl.when(~_invalid_row()).then(pl.col("RAW_INTENSITY")).alias("raw_intensity"),
    )
    return with_local_keys(joined, "timestamp_utc", person_timezone).select(
        "date_key", "time_key", "device_key", "person_key", "activity_kind_key",
        "timestamp_utc", "steps", "heart_rate_bpm", "raw_intensity",
    )


_FACT_KEY = dg.AssetKey(["gadgetbridge", "gold", "fact_activity_minute"])
_CONFORMED = ["conformed", "gold"]


@dg.asset(
    name="dim_activity_kind",
    group_name="gadgetbridge",
    key_prefix=["gadgetbridge", "gold"],
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description="Every RAW_KIND code 0..255; 255 is the described 'invalid row' member.",
    automation_condition=dg.AutomationCondition.on_missing(),
)
def dim_activity_kind() -> pl.DataFrame:
    return build_dim_activity_kind()


@dg.asset(
    name="fact_activity_minute",
    group_name="gadgetbridge",
    key_prefix=["gadgetbridge", "gold"],
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One row per Device and minute. Heart rate 255 is nulled; the whole-row "
        "invalid run keeps its rows with measures nulled. Unexplained columns are excluded."
    ),
    ins={
        "huami_extended_activity_sample": dg.AssetIn(
            key=["gadgetbridge", "bronze", "huami_extended_activity_sample"]
        ),
        "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
        "person_timezone": dg.AssetIn(key=[*_CONFORMED, "person_timezone"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_activity_minute(
    huami_extended_activity_sample: pl.DataFrame,
    dim_device: pl.DataFrame,
    person_timezone: pl.DataFrame,
) -> pl.DataFrame:
    return build_fact_activity_minute(huami_extended_activity_sample, dim_device, person_timezone)


@dg.asset_check(asset=_FACT_KEY, blocking=True, name="activity_minute_grain_unique")
def activity_minute_grain_unique(fact_activity_minute: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(fact_activity_minute, ["device_key", "timestamp_utc"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])})


@dg.asset_check(
    asset=_FACT_KEY,
    blocking=True,
    name="activity_minute_keys_resolve",
    additional_ins={
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
        "dim_time_of_day": dg.AssetIn(key=[*_CONFORMED, "dim_time_of_day"]),
        "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
        "dim_activity_kind": dg.AssetIn(key=["gadgetbridge", "gold", "dim_activity_kind"]),
    },
)
def activity_minute_keys_resolve(
    fact_activity_minute: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_time_of_day: pl.DataFrame,
    dim_device: pl.DataFrame,
    dim_person: pl.DataFrame,
    dim_activity_kind: pl.DataFrame,
) -> dg.AssetCheckResult:
    unresolved = {
        key: find_unresolved_keys(fact_activity_minute, key, dim)[:20]
        for key, dim in {
            "date_key": dim_date,
            "time_key": dim_time_of_day,
            "device_key": dim_device,
            "person_key": dim_person,
            "activity_kind_key": dim_activity_kind,
        }.items()
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


@dg.asset_check(
    asset=_FACT_KEY,
    blocking=True,
    name="activity_minute_person_matches_device",
    additional_ins={"dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"])},
)
def activity_minute_person_matches_device(
    fact_activity_minute: pl.DataFrame, dim_device: pl.DataFrame
) -> dg.AssetCheckResult:
    bad = find_person_device_mismatches(fact_activity_minute, dim_device)
    return dg.AssetCheckResult(passed=not bad, metadata={"mismatches": str(bad[:20])})


defs = dg.Definitions(
    assets=[dim_activity_kind, fact_activity_minute],
    asset_checks=[
        activity_minute_grain_unique,
        activity_minute_keys_resolve,
        activity_minute_person_matches_device,
    ],
)
