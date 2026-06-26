import polars as pl
import dagster as dg
import datetime
from dagster import AssetExecutionContext, AutomationCondition, Definitions

@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={
        "activity": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
)
def weekday_heart_rate_distribution_before_and_after(context: AssetExecutionContext, activity: pl.DataFrame) -> pl.DataFrame:
    # MAGIC DATE
    START_DATE = datetime.date(2026,5,24)
    BIN_SIZE = 5

    context.log.info("Shape: %s:%s" % (activity.shape[0], activity.shape[1]))
    context.log.info("Columns: " + ",".join(activity.columns))

    columns = ["TIMESTAMP","RAW_INTENSITY", "HEART_RATE", "STEPS"]
    return activity.select(columns).with_columns(
        (pl.col("TIMESTAMP") > START_DATE).alias("after"),
        pl.col("TIMESTAMP").dt.weekday().alias("weekday"),
    ).filter(
        pl.col("weekday") < 6
    ).with_columns(
        pl.col("HEART_RATE") // BIN_SIZE * BIN_SIZE
    ).group_by(
        ['after', 'HEART_RATE']
    ).agg(
        pl.len()
    ).pivot(
        ['after'], values=['len']
    ).with_columns(
        pl.col('false') / pl.col('false').sum(),
        pl.col('true') / pl.col('true').sum()
    ).unpivot(
        index='HEART_RATE'
    ).rename({"variable":"after"}).filter(
        pl.col("HEART_RATE") < 160,
        pl.col("HEART_RATE") >= 40
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

defs = Definitions(assets=[daily_health_snapshot, weekday_heart_rate_distribution_before_and_after])
