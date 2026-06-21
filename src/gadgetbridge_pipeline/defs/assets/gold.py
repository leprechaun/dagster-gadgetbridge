import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext


@dg.asset(
    group_name="gadgetbridge_gold",
    io_manager_key="gadgetbridge_gold_io_manager",
    key_prefix="gadgetbridge-gold",
    ins={
        "activity_sample":         dg.AssetIn(key=dg.AssetKey(["gadgetbridge-silver", "huami_extended_activity_sample"])),
        "hrv":         dg.AssetIn(key=dg.AssetKey(["gadgetbridge-silver", "generic_hrv_value_sample"])),
        "spo2":        dg.AssetIn(key=dg.AssetKey(["gadgetbridge-silver", "huami_spo2_sample"])),
        "stress":      dg.AssetIn(key=dg.AssetKey(["gadgetbridge-silver", "huami_stress_sample"])),
        "temperature": dg.AssetIn(key=dg.AssetKey(["gadgetbridge-silver", "generic_temperature_sample"])),
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

defs = Definitions(assets=[daily_health_snapshot])
