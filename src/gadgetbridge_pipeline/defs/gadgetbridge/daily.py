"""Daily gold tables rebuilt from the dimensional facts, beside the old assets.

Each table splits by Person and keeps only Covered Dates (a missing value on an
uncovered date is "no data", never zero).
"""

from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain


def _covered(wear_coverage: pl.DataFrame, flag: str = "covered_date") -> pl.DataFrame:
    return wear_coverage.filter(pl.col(flag)).select("person_key", "date_key")


def build_derived_steps_daily(
    fact_activity_minute: pl.DataFrame, wear_coverage: pl.DataFrame
) -> pl.DataFrame:
    """Steps per Person and Local Date. Null steps (invalid rows) are ignored."""
    return (
        fact_activity_minute.group_by("person_key", "date_key")
        .agg(pl.col("steps").sum().cast(pl.Int64).alias("steps"))
        .join(_covered(wear_coverage), on=["person_key", "date_key"], how="inner")
        .with_columns(pl.col("date_key").dt.weekday().alias("weekday"))
        .with_columns((pl.col("weekday") >= 6).alias("is_weekend"))
        .select("person_key", "date_key", "steps", "weekday", "is_weekend")
        .sort("person_key", "date_key")
    )



def build_derived_daily_health_snapshot(
    fact_activity_minute: pl.DataFrame,
    source_facts: dict[str, tuple[pl.DataFrame, str]],
    wear_coverage: pl.DataFrame,
) -> pl.DataFrame:
    """One row per Person and Covered Date: the mean of each source measure with
    its count (so means roll up), and heart-rate spread. `source_facts` maps a
    short name to `(fact frame, measure column)`; each gets `avg_<name>` and
    `<name>_count`. The heart-rate percentiles are not rollable.
    """
    spine = _covered(wear_coverage)
    result = spine
    for name, (frame, column) in source_facts.items():
        agg = frame.group_by("person_key", "date_key").agg(
            pl.col(column).mean().alias(f"avg_{name}"),
            pl.col(column).count().cast(pl.Int64).alias(f"{name}_count"),
        )
        result = result.join(agg, on=["person_key", "date_key"], how="left").with_columns(
            pl.col(f"{name}_count").fill_null(0)
        )
    heart = fact_activity_minute.group_by("person_key", "date_key").agg(
        pl.col("heart_rate_bpm").min().alias("heart_rate_min"),
        pl.col("heart_rate_bpm").max().alias("heart_rate_max"),
        pl.col("heart_rate_bpm").median().alias("heart_rate_median"),
        pl.col("heart_rate_bpm").quantile(0.1).alias("heart_rate_p10"),
        pl.col("heart_rate_bpm").quantile(0.9).alias("heart_rate_p90"),
        pl.col("heart_rate_bpm").count().cast(pl.Int64).alias("heart_rate_count"),
    )
    return (
        result.join(heart, on=["person_key", "date_key"], how="left")
        .with_columns(pl.col("heart_rate_count").fill_null(0))
        .sort("person_key", "date_key")
    )


def build_derived_sleep_schedule(
    fact_sleep_session: pl.DataFrame, wear_coverage: pl.DataFrame
) -> pl.DataFrame:
    """The Night's bedtime and wake as signed minutes from reporting midnight
    (so a night stays continuous across midnight and bedtimes average validly),
    for Persons and dates whose Night window is covered.
    """
    return (
        fact_sleep_session.filter(~pl.col("is_nap"))
        .group_by("person_key", "reporting_date_key")
        .agg(
            pl.col("start_minutes_from_reporting_midnight").min().alias("bedtime_minutes"),
            pl.col("end_minutes_from_reporting_midnight").max().alias("wake_minutes"),
        )
        .join(
            _covered(wear_coverage, "covered_night_window"),
            left_on=["person_key", "reporting_date_key"], right_on=["person_key", "date_key"],
            how="inner",
        )
        .with_columns((pl.col("reporting_date_key").dt.weekday() >= 6).alias("is_weekend"))
        .select("person_key", "reporting_date_key", "bedtime_minutes", "wake_minutes", "is_weekend")
        .sort("person_key", "reporting_date_key")
    )


def build_derived_sleep_score_daily(
    fact_sleep_session: pl.DataFrame, wear_coverage: pl.DataFrame
) -> pl.DataFrame:
    """Device sleep scores per Person and reporting date, over covered Night
    windows. The mean keeps its sum and count so it can be rolled up.
    """
    return (
        fact_sleep_session.group_by("person_key", "reporting_date_key")
        .agg(
            pl.col("score").sum().cast(pl.Int64).alias("score_sum"),
            pl.col("score").count().cast(pl.Int64).alias("score_count"),
            pl.col("score").mean().alias("mean_score"),
            pl.col("score").max().alias("max_score"),
            (~pl.col("is_nap")).sum().cast(pl.Int64).alias("night_session_count"),
            pl.col("is_nap").sum().cast(pl.Int64).alias("nap_count"),
        )
        .join(
            _covered(wear_coverage, "covered_night_window"),
            left_on=["person_key", "reporting_date_key"], right_on=["person_key", "date_key"],
            how="inner",
        )
        .sort("person_key", "reporting_date_key")
    )


_GOLD = ["gadgetbridge", "gold"]
_COVERAGE = dg.AssetIn(key=[*_GOLD, "derived_wear_coverage"])


def _grain_result(table: pl.DataFrame, grain: list[str]) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(table, grain)
    return dg.AssetCheckResult(
        check_name="grain_unique", passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])}
    )


def _derived(name: str, description: str, ins: dict):
    return dg.asset(
        name=name,
        group_name="gadgetbridge",
        key_prefix=_GOLD,
        io_manager_key="deltalake_io_manager",
        kinds={"deltalake"},
        description=description,
        ins=ins,
        check_specs=[
            dg.AssetCheckSpec(name="grain_unique", asset=dg.AssetKey([*_GOLD, name]), blocking=True)
        ],
        automation_condition=dg.AutomationCondition.eager(),
    )


@_derived(
    "derived_steps_daily",
    "Steps per Person and Local Date, Covered Dates only (the old steps_per_day, rebuilt).",
    {"fact_activity_minute": dg.AssetIn(key=[*_GOLD, "fact_activity_minute"]),
     "derived_wear_coverage": _COVERAGE},
)
def derived_steps_daily(fact_activity_minute: pl.DataFrame, derived_wear_coverage: pl.DataFrame):
    table = build_derived_steps_daily(fact_activity_minute, derived_wear_coverage)
    yield dg.Output(table)
    yield _grain_result(table, ["person_key", "date_key"])


@_derived(
    "derived_daily_health_snapshot",
    "Per Person and Covered Date: mean (with count) of HRV, SpO2, stress and temperature, and "
    "heart-rate spread with the no-reading value excluded.",
    {
        "fact_activity_minute": dg.AssetIn(key=[*_GOLD, "fact_activity_minute"]),
        "fact_hrv_sample": dg.AssetIn(key=[*_GOLD, "fact_hrv_sample"]),
        "fact_spo2_sample": dg.AssetIn(key=[*_GOLD, "fact_spo2_sample"]),
        "fact_stress_sample": dg.AssetIn(key=[*_GOLD, "fact_stress_sample"]),
        "fact_temperature_sample": dg.AssetIn(key=[*_GOLD, "fact_temperature_sample"]),
        "derived_wear_coverage": _COVERAGE,
    },
)
def derived_daily_health_snapshot(
    fact_activity_minute: pl.DataFrame,
    fact_hrv_sample: pl.DataFrame,
    fact_spo2_sample: pl.DataFrame,
    fact_stress_sample: pl.DataFrame,
    fact_temperature_sample: pl.DataFrame,
    derived_wear_coverage: pl.DataFrame,
):
    table = build_derived_daily_health_snapshot(
        fact_activity_minute,
        {
            "hrv": (fact_hrv_sample, "hrv_ms"),
            "spo2": (fact_spo2_sample, "spo2_percent"),
            "stress": (fact_stress_sample, "stress_score"),
            "temperature": (fact_temperature_sample, "temperature_celsius"),
        },
        derived_wear_coverage,
    )
    yield dg.Output(table)
    yield _grain_result(table, ["person_key", "date_key"])


_SESSION_INS = {
    "fact_sleep_session": dg.AssetIn(key=[*_GOLD, "fact_sleep_session"]),
    "derived_wear_coverage": _COVERAGE,
}


@_derived(
    "derived_sleep_schedule",
    "The Night's bedtime and wake as signed minutes from reporting midnight, per Person, over "
    "covered Night windows (replaces the 15:00 fold in daily_sleep_schedule).",
    _SESSION_INS,
)
def derived_sleep_schedule(fact_sleep_session: pl.DataFrame, derived_wear_coverage: pl.DataFrame):
    table = build_derived_sleep_schedule(fact_sleep_session, derived_wear_coverage)
    yield dg.Output(table)
    yield _grain_result(table, ["person_key", "reporting_date_key"])


@_derived(
    "derived_sleep_score_daily",
    "Device sleep scores per Person and reporting date over covered Night windows; the mean "
    "keeps its sum and count.",
    _SESSION_INS,
)
def derived_sleep_score_daily(
    fact_sleep_session: pl.DataFrame, derived_wear_coverage: pl.DataFrame
):
    table = build_derived_sleep_score_daily(fact_sleep_session, derived_wear_coverage)
    yield dg.Output(table)
    yield _grain_result(table, ["person_key", "reporting_date_key"])


defs = dg.Definitions(
    assets=[
        derived_steps_daily,
        derived_daily_health_snapshot,
        derived_sleep_schedule,
        derived_sleep_score_daily,
    ]
)
