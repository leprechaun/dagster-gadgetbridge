import polars as pl
import dagster as dg
import datetime

from dagster import AutomationCondition, Definitions


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "silver"],
    ins={
        "activity":        dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
    }
)
def sleep_periods_based_on_activity(activity: pl.DataFrame):
    sleep_periods = (
        activity.with_columns(
            pl.col("TIMESTAMP").dt.convert_time_zone("Asia/Bangkok")
        )
        .select(
            ["TIMESTAMP", "RAW_KIND"]
        )
        .filter(pl.col("RAW_KIND").diff() != 0)
        .rename({"TIMESTAMP":"start"})
        .with_columns(
            pl.col("start").shift(-1).alias("end")
        )
        .filter(pl.col("RAW_KIND") == 120)
        .drop(["RAW_KIND"])
        .with_columns(
            pl.col("start").dt.date().alias("date")
        ).with_columns(
            pl.when(pl.col("start").dt.time() > datetime.time(18, 0)).then(
                pl.col("start").dt.date() + pl.duration(days=1)
            ).otherwise(
                pl.col("start").dt.date()
            ).alias("reporting_date")
        )
        .select(["date", "reporting_date", "start", "end"])
    )

    return sleep_periods


def _by_minute(df: pl.DataFrame, col: str, alias: str, group_by: list[str]) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("TIMESTAMP").dt.truncate("1m").alias("MINUTE"))
        .group_by(group_by)
        .agg(pl.col(col).mean().alias(alias))
    )


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "silver"],
    ins={
        "activity":        dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
        "temperature":     dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "generic_temperature_sample"])),
        "hrv":             dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "generic_hrv_value_sample"])),
        "stress":          dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"])),
        "spo2":            dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_spo2_sample"])),
        "respiratory_rate": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_sleep_respiratory_rate_sample"])),
        "battery":         dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "battery_level"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Wide per-minute join of all bronze health metrics",
)
def per_minute_health_metrics(
    activity: pl.DataFrame,
    temperature: pl.DataFrame,
    hrv: pl.DataFrame,
    stress: pl.DataFrame,
    spo2: pl.DataFrame,
    respiratory_rate: pl.DataFrame,
    battery: pl.DataFrame,
) -> pl.DataFrame:
    base = (
        activity
        .with_columns(
            pl.col("TIMESTAMP").dt.truncate("1m").alias("MINUTE"),
            pl.when(pl.col("HEART_RATE") == 255).then(None).otherwise(pl.col("HEART_RATE")).alias("HEART_RATE"),
        )
        .drop(["TIMESTAMP", "UNKNOWN1"])
    )

    temp_min   = _by_minute(temperature,     "TEMPERATURE", "TEMPERATURE",      ["MINUTE", "DEVICE_ID", "USER_ID"])
    hrv_min    = _by_minute(hrv,             "VALUE",       "HRV",              ["MINUTE", "DEVICE_ID", "USER_ID"])
    stress_min = _by_minute(stress,          "STRESS",      "STRESS",           ["MINUTE", "DEVICE_ID", "USER_ID"])
    spo2_min   = _by_minute(spo2,            "SPO2",        "SPO2",             ["MINUTE", "DEVICE_ID", "USER_ID"])
    resp_min   = _by_minute(respiratory_rate,"RATE",        "RESPIRATORY_RATE", ["MINUTE", "DEVICE_ID", "USER_ID"])
    batt_min   = _by_minute(battery,         "LEVEL",       "BATTERY_LEVEL",    ["MINUTE", "DEVICE_ID"])

    return (
        base
        .join(temp_min,   on=["MINUTE", "DEVICE_ID", "USER_ID"], how="left")
        .join(hrv_min,    on=["MINUTE", "DEVICE_ID", "USER_ID"], how="left")
        .join(stress_min, on=["MINUTE", "DEVICE_ID", "USER_ID"], how="left")
        .join(spo2_min,   on=["MINUTE", "DEVICE_ID", "USER_ID"], how="left")
        .join(resp_min,   on=["MINUTE", "DEVICE_ID", "USER_ID"], how="left")
        .join(batt_min,   on=["MINUTE", "DEVICE_ID"],            how="left")
        .sort("MINUTE")
    )


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "silver"],
    ins={
        "activity": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Daily distribution of binned heart rates (5 bpm bins, 40-160 range)",
)
def daily_heart_rate_distribution(activity: pl.DataFrame) -> pl.DataFrame:
    BIN_SIZE = 5
    return (
        activity
        .select(["TIMESTAMP", "HEART_RATE"])
        .filter(pl.col("HEART_RATE") != 255)
        .with_columns(
            pl.col("TIMESTAMP").dt.date().alias("date"),
            (pl.col("HEART_RATE") // BIN_SIZE * BIN_SIZE).cast(pl.Int32).alias("heart_rate"),
        )
        .group_by(["date", "heart_rate"])
        .agg(pl.len().alias("sample_count"))
        .sort(["date", "heart_rate"])
    )


defs = Definitions(assets=[
    per_minute_health_metrics,
    daily_heart_rate_distribution,
    sleep_periods_based_on_activity
])
