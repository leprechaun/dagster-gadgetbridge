from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain


def build_derived_night(
    fact_sleep_session: pl.DataFrame,
    fact_sleep_stage: pl.DataFrame,
    wear_coverage: pl.DataFrame,
) -> pl.DataFrame:
    """One row per Person and reporting date: the Night's Sleep Block."""
    sessions = fact_sleep_session.filter(~pl.col("is_nap"))
    asleep = (
        fact_sleep_stage.filter(pl.col("sleep_stage_key") != "AWAKE")
        .group_by("session_key").agg(pl.col("duration_minutes").sum().alias("_asleep"))
    )
    nights = (
        sessions.join(asleep, on="session_key", how="left")
        .group_by("person_key", "reporting_date_key")
        .agg(
            pl.lit("session").alias("source"),
            pl.col("start_utc").min().alias("bedtime_utc"),
            pl.col("end_utc").max().alias("wake_utc"),
            pl.col("_asleep").sum().cast(pl.Int64).alias("total_sleep_minutes"),
            pl.col("score").sort_by("length_minutes", "start_utc", descending=[True, False])
            .first().alias("score"),
            pl.len().cast(pl.Int64).alias("session_count"),
        )
        .with_columns(
            ((pl.col("wake_utc") - pl.col("bedtime_utc")).dt.total_minutes())
            .alias("time_in_bed_minutes")
        )
    )
    covered = wear_coverage.filter(pl.col("covered_night_window")).select(
        "person_key", pl.col("date_key").alias("reporting_date_key")
    )
    return (
        covered.join(nights, on=["person_key", "reporting_date_key"], how="left")
        .with_columns(
            pl.col("source").fill_null("none"),
            pl.col("time_in_bed_minutes", "total_sleep_minutes", "session_count")
            .fill_null(0),
        )
        .select(
            "person_key", "reporting_date_key", "source", "bedtime_utc", "wake_utc",
            "time_in_bed_minutes", "total_sleep_minutes", "score", "session_count",
        )
    )


_GOLD = ["gadgetbridge", "gold"]
_NIGHT_KEY = dg.AssetKey([*_GOLD, "derived_night"])


@dg.asset(
    name="derived_night",
    group_name="gadgetbridge",
    key_prefix=_GOLD,
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One Night per Person and reporting date, for every date whose Night window is "
        "covered. Time in bed is the block span; sleep counts non-AWAKE stage minutes; the "
        "score is the Night's longest session's, never an average."
    ),
    ins={
        "fact_sleep_session": dg.AssetIn(key=[*_GOLD, "fact_sleep_session"]),
        "fact_sleep_stage": dg.AssetIn(key=[*_GOLD, "fact_sleep_stage"]),
        "derived_wear_coverage": dg.AssetIn(key=[*_GOLD, "derived_wear_coverage"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_night(
    fact_sleep_session: pl.DataFrame,
    fact_sleep_stage: pl.DataFrame,
    derived_wear_coverage: pl.DataFrame,
) -> pl.DataFrame:
    return build_derived_night(fact_sleep_session, fact_sleep_stage, derived_wear_coverage)


@dg.asset_check(asset=_NIGHT_KEY, blocking=True, name="derived_night_grain_unique")
def derived_night_grain_unique(derived_night: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_night, ["person_key", "reporting_date_key"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])})


defs = dg.Definitions(assets=[derived_night], asset_checks=[derived_night_grain_unique])
