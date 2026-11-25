from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.constants import (
    COVERAGE_MIN_HOURS,
    NIGHT_BOUNDARY,
)
from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_unresolved_keys,
)

BIN_WIDTH_BPM = 5
COVERAGE_MIN_MINUTES = COVERAGE_MIN_HOURS * 60
NIGHT_BOUNDARY_MINUTE = NIGHT_BOUNDARY.hour * 60 + NIGHT_BOUNDARY.minute


def build_dim_heart_rate_bin() -> pl.DataFrame:
    """Bins keyed by lower bound; 255 is the no-reading sentinel, so the last bin is 250."""
    keys = pl.int_range(0, 255, BIN_WIDTH_BPM, eager=True)
    return pl.DataFrame({"heart_rate_bin_key": keys}).with_columns(
        pl.lit(BIN_WIDTH_BPM).alias("width_bpm")
    )


def build_heart_rate_distribution_daily(fact_activity_minute: pl.DataFrame) -> pl.DataFrame:
    """Minutes per Person, Local Date and 5-bpm bin. `sample_count` is additive,
    so rows roll up exactly; Persons are never pooled.
    """
    return (
        fact_activity_minute.filter(pl.col("heart_rate_bpm").is_not_null())
        .with_columns(
            (pl.col("heart_rate_bpm") // BIN_WIDTH_BPM * BIN_WIDTH_BPM).alias("heart_rate_bin_key")
        )
        .group_by("date_key", "person_key", "heart_rate_bin_key")
        .agg(pl.len().cast(pl.Int64).alias("sample_count"))
    )


def build_wear_coverage(fact_activity_minute: pl.DataFrame) -> pl.DataFrame:
    """Worn minutes per Person and Local Date. A minute is worn only if it
    has a non-null heart rate.
    """
    worn = fact_activity_minute.filter(pl.col("heart_rate_bpm").is_not_null()).with_columns(
        pl.when(pl.col("time_key") >= NIGHT_BOUNDARY_MINUTE)
        .then(pl.col("date_key") + pl.duration(days=1))
        .otherwise(pl.col("date_key"))
        .alias("_night_date")
    )
    by_date = worn.group_by("person_key", "date_key").agg(
        pl.col("timestamp_utc").n_unique().cast(pl.Int64).alias("minutes_worn_date")
    )
    by_night = worn.group_by("person_key", pl.col("_night_date").alias("date_key")).agg(
        pl.col("timestamp_utc").n_unique().cast(pl.Int64).alias("minutes_worn_night_window")
    )
    return (
        by_date.join(by_night, on=["person_key", "date_key"], how="full", coalesce=True)
        .with_columns(pl.col("minutes_worn_date", "minutes_worn_night_window").fill_null(0))
        .with_columns(
            (pl.col("minutes_worn_date") >= COVERAGE_MIN_MINUTES).alias("covered_date"),
            (pl.col("minutes_worn_night_window") >= COVERAGE_MIN_MINUTES)
            .alias("covered_night_window"),
        )
    )


_GOLD = ["gadgetbridge", "gold"]
_FACT = dg.AssetKey([*_GOLD, "fact_activity_minute"])


def _derived(name: str, description: str, builder):
    @dg.asset(
        name=name,
        group_name="gadgetbridge",
        key_prefix=_GOLD,
        io_manager_key="deltalake_io_manager",
        kinds={"deltalake"},
        description=description,
        ins={"fact_activity_minute": dg.AssetIn(key=_FACT)},
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _asset(fact_activity_minute: pl.DataFrame) -> pl.DataFrame:
        return builder(fact_activity_minute)

    return _asset


derived_wear_coverage = _derived(
    "derived_wear_coverage",
    "Worn minutes per Person and Local Date, over the calendar day and the Night window, "
    "against the shared coverage threshold. A minute is worn only with a heart rate.",
    build_wear_coverage,
)
derived_heart_rate_distribution_daily = _derived(
    "derived_heart_rate_distribution_daily",
    "Minutes per Person, Local Date and 5-bpm bin. sample_count is additive.",
    build_heart_rate_distribution_daily,
)


@dg.asset(
    name="dim_heart_rate_bin",
    group_name="gadgetbridge",
    key_prefix=_GOLD,
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description="5-bpm heart-rate bins keyed by lower bound.",
    automation_condition=dg.AutomationCondition.on_missing(),
)
def dim_heart_rate_bin() -> pl.DataFrame:
    return build_dim_heart_rate_bin()


def _unique_result(table: pl.DataFrame, grain: list[str]) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(table, grain)
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])})


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "derived_wear_coverage"]),
    blocking=True,
    name="derived_wear_coverage_grain_unique",
)
def derived_wear_coverage_grain_unique(derived_wear_coverage: pl.DataFrame) -> dg.AssetCheckResult:
    return _unique_result(derived_wear_coverage, ["person_key", "date_key"])


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "derived_heart_rate_distribution_daily"]),
    blocking=True,
    name="derived_heart_rate_distribution_daily_grain_unique",
)
def derived_distribution_grain_unique(
    derived_heart_rate_distribution_daily: pl.DataFrame,
) -> dg.AssetCheckResult:
    return _unique_result(
        derived_heart_rate_distribution_daily,
        ["person_key", "date_key", "heart_rate_bin_key"],
    )


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "derived_heart_rate_distribution_daily"]),
    blocking=True,
    name="derived_heart_rate_distribution_daily_keys_resolve",
    additional_ins={
        "dim_heart_rate_bin": dg.AssetIn(key=[*_GOLD, "dim_heart_rate_bin"]),
        "dim_date": dg.AssetIn(key=["conformed", "gold", "dim_date"]),
        "dim_person": dg.AssetIn(key=["conformed", "gold", "dim_person"]),
    },
)
def distribution_keys_resolve(
    derived_heart_rate_distribution_daily: pl.DataFrame,
    dim_heart_rate_bin: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_person: pl.DataFrame,
) -> dg.AssetCheckResult:
    table = derived_heart_rate_distribution_daily
    unresolved = {
        key: find_unresolved_keys(table, key, dim)[:20]
        for key, dim in {
            "heart_rate_bin_key": dim_heart_rate_bin,
            "date_key": dim_date,
            "person_key": dim_person,
        }.items()
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


defs = dg.Definitions(
    assets=[dim_heart_rate_bin, derived_wear_coverage, derived_heart_rate_distribution_daily],
    asset_checks=[
        derived_wear_coverage_grain_unique,
        derived_distribution_grain_unique,
        distribution_keys_resolve,
    ],
)
