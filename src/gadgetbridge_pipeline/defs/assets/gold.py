import polars as pl
import dagster as dg
import datetime
from dagster import AssetExecutionContext, AutomationCondition, Definitions

@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "daily_heart_rate_distribution": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "silver", "daily_heart_rate_distribution"])),
    },
    automation_condition=AutomationCondition.eager(),
)
def weekday_heart_rate_distribution_before_and_after(
    context: AssetExecutionContext,
    daily_heart_rate_distribution: pl.DataFrame,
) -> pl.DataFrame:
    START_DATE = datetime.date(2026, 5, 24)

    return (
        daily_heart_rate_distribution
        .with_columns(
            pl.col("date").dt.weekday().alias("weekday"),
            (pl.col("date") > START_DATE).alias("after"),
        )
        .filter(pl.col("weekday") < 6)
        .group_by(["after", "heart_rate"])
        .agg(pl.col("sample_count").sum())
        .pivot("after", values="sample_count")
        .with_columns(
            pl.col("false") / pl.col("false").sum(),
            pl.col("true") / pl.col("true").sum(),
        )
        .unpivot(index="heart_rate")
        .rename({"variable": "after"})
    )

@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "activity": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
)
def steps_per_day(activity):
    return (
        activity.select(['TIMESTAMP', 'STEPS'])
        .group_by([
            pl.col("TIMESTAMP").dt.date().alias("date")
        ])
        .agg(
            pl.col("STEPS").sum()
        ).sort(
            by=['date']
        ).with_columns(
            pl.col("date").dt.weekday().alias("weekday"),
        ).with_columns(
            (pl.col("weekday") > 5).alias("is_weekend")
        )
    )


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "activity_sample": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
        "hrv":             dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "generic_hrv_value_sample"])),
        "spo2":            dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_spo2_sample"])),
        "stress":          dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"])),
        "temperature":     dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "generic_temperature_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
)
def daily_health_snapshot(
    activity_sample: pl.DataFrame,
    hrv: pl.DataFrame,
    spo2: pl.DataFrame,
    stress: pl.DataFrame,
    temperature: pl.DataFrame,
) -> pl.DataFrame:
    def by_day(df: pl.DataFrame, col: str, alias: str) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
            .group_by("date")
            .agg(pl.col(col).mean().alias(alias))
        )

    frames = [
        by_day(hrv,         "VALUE",       "avg_hrv"),
        by_day(spo2,        "SPO2",        "avg_spo2"),
        by_day(stress,      "STRESS",      "avg_stress"),
        by_day(temperature, "TEMPERATURE", "avg_temperature_c"),
    ]

    frames.append(
        activity_sample.with_columns(
            pl.col("TIMESTAMP").dt.date().alias("date")
        ).group_by(
            ["date"]
        ).agg(
            pl.col("HEART_RATE").min().alias("heart_rate_min"),
            pl.col("HEART_RATE").max().alias("heart_rate_max"),
            pl.col("HEART_RATE").quantile(0.5).alias("heart_rate_median"),
            pl.col("HEART_RATE").quantile(0.1).alias("heart_rate_p10"),
            pl.col("HEART_RATE").quantile(0.9).alias("heart_rate_p90"),
        )
    )

    result = frames[0]

    for f in frames[1:]:
        result = result.join(f, on="date", how="full", coalesce=True)

    sorted_df = result.sort("date")

    return sorted_df

@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "activity": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
        "stress":   dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Daily step totals joined with average stress score, for correlation analysis",
)
def steps_vs_stress(activity: pl.DataFrame, stress: pl.DataFrame) -> pl.DataFrame:
    daily_steps = (
        activity.select(["TIMESTAMP", "STEPS"])
        .with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.col("STEPS").sum().alias("total_steps"))
    )

    daily_stress = (
        stress.select(["TIMESTAMP", "STRESS"])
        .with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
        .group_by("date")
        .agg(
            pl.col("STRESS").mean().alias("avg_stress"),
            pl.col("STRESS").median().alias("median_stress"),
        )
    )

    return (
        daily_steps.join(daily_stress, on="date", how="inner")
        .sort("date")
        .with_columns(
            pl.col("date").dt.weekday().alias("weekday"),
            (pl.col("date").dt.weekday() >= 5).alias("is_weekend"),
        )
    )


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "daily_heart_rate_distribution": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "silver", "daily_heart_rate_distribution"])),
        "medicine_log": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Heart rate distribution normalized within each (medication_state × weekday/weekend) group",
)
def heart_rate_distribution_by_medication_and_weekday(
    daily_heart_rate_distribution: pl.DataFrame,
    medicine_log: pl.DataFrame,
) -> pl.DataFrame:
    medication_by_date = (
        medicine_log
        .filter(pl.col("taken"))
        .group_by("date")
        .agg(pl.col("medicine").sort().str.join(" + ").alias("medication_state"))
    )

    return (
        daily_heart_rate_distribution
        .join(medication_by_date, on="date", how="left")
        .with_columns(
            pl.col("medication_state").fill_null("sober"),
            (pl.col("date").dt.weekday() >= 5).alias("is_weekend"),
        )
        .group_by(["heart_rate", "medication_state", "is_weekend"])
        .agg(pl.col("sample_count").sum())
        .with_columns(
            (pl.col("sample_count") / pl.col("sample_count").sum().over(["medication_state", "is_weekend"]))
            .alias("proportion")
        )
        .sort(["medication_state", "is_weekend", "heart_rate"])
    )


defs = Definitions(assets=[daily_health_snapshot, weekday_heart_rate_distribution_before_and_after, steps_per_day, steps_vs_stress, heart_rate_distribution_by_medication_and_weekday])
