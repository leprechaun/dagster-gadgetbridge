from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl
import dagster as dg
from dagster import AssetCheckResult, AutomationCondition, Definitions

_PRESCRIPTIONS_PATH = Path(__file__).parents[4] / "data" / "prescriptions.csv"
_SKIPS_PATH = Path(__file__).parents[4] / "data" / "medicine_skips.csv"

KNOWN_MEDICINES: frozenset[str] = frozenset({
    "medicine_a",
    "medicine_b",
    "medicine_c",
})

_EMPTY_SCHEMA = {
    "date": pl.Date,
    "medicine": pl.String,
    "dosage_mg": pl.Float64,
    "taken": pl.Boolean,
    "day_of_week": pl.Int8,
    "is_weekend": pl.Boolean,
    "adherence_streak": pl.Int32,
}


def build_medicine_log(
    prescriptions: pl.DataFrame,
    skips: pl.DataFrame,
    today: date,
) -> pl.DataFrame:
    skip_dates = set(skips["date"].to_list())
    rows: list[dict] = []

    for row in prescriptions.sort("start_date").iter_rows(named=True):
        start: date = row["start_date"]
        end: date | None = row["end_date"]
        if end is None or end > today:
            end = today
        d = start
        while d <= end:
            rows.append({"date": d, "medicine": row["medicine"], "dosage_mg": row["dosage_mg"]})
            d += timedelta(days=1)

    if not rows:
        return pl.DataFrame(schema=_EMPTY_SCHEMA)

    df = (
        pl.DataFrame(rows)
        .sort("date")
        .with_columns(
            (~pl.col("date").is_in(list(skip_dates))).alias("taken"),
            pl.col("date").dt.weekday().cast(pl.Int8).alias("day_of_week"),
            (pl.col("date").dt.weekday() >= 5).alias("is_weekend"),
        )
    )

    # Streak breaks when taken changes OR when dates are not consecutive (gap between prescriptions).
    # Cast Date to Int32 (days since epoch) for safe consecutive-day arithmetic.
    df = (
        df.with_columns(
            (
                (pl.col("taken") != pl.col("taken").shift(1))
                | (pl.col("date").cast(pl.Int32) != pl.col("date").shift(1).cast(pl.Int32) + 1)
            )
            .fill_null(True)
            .cum_sum()
            .alias("_streak_group")
        )
        .with_columns(
            pl.when(pl.col("taken"))
            .then(pl.col("taken").cast(pl.Int32).cum_sum().over("_streak_group"))
            .otherwise(pl.lit(0))
            .cast(pl.Int32)
            .alias("adherence_streak")
        )
        .drop("_streak_group")
    )

    return df


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "bronze"],
    description="Daily medication adherence log derived from prescriptions and skip records",
)
def medicine_log(context) -> pl.DataFrame:
    context.log.info("Running medicine_log")

    context.log.info("prescriptions: %s" % str(_PRESCRIPTIONS_PATH))
    prescriptions = pl.read_csv(
        _PRESCRIPTIONS_PATH,
        schema_overrides={"start_date": pl.Date, "end_date": pl.Date, "dosage_mg": pl.Float64},
        null_values=[""],
    )
    context.log.info(print(prescriptions))

    context.log.info("skips: %s" % str(_SKIPS_PATH))
    skips = pl.read_csv(_SKIPS_PATH, schema_overrides={"date": pl.Date})
    context.log.info(print(skips))

    df = build_medicine_log(prescriptions, skips, today=date.today())
    context.log.info(print(df))
    context.log.info(f"Generated {df.shape[0]} medicine log rows")
    return df


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"]),
    blocking=True,
    name="medicine_log_known_medicine_names",
)
def medicine_log_known_medicine_names(medicine_log: pl.DataFrame) -> AssetCheckResult:
    found = set(medicine_log["medicine"].unique().to_list())
    unknown = found - KNOWN_MEDICINES
    checks = {"no_unknown_medicines": len(unknown) == 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"unknown_medicines": str(unknown)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"]),
    blocking=True,
    name="medicine_log_dosage_positive",
)
def medicine_log_dosage_positive(medicine_log: pl.DataFrame) -> AssetCheckResult:
    minimum = float(medicine_log["dosage_mg"].min())
    checks = {"is_positive": minimum > 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum},
    )


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"]),
    blocking=True,
    name="medicine_log_skips_within_prescriptions",
)
def medicine_log_skips_within_prescriptions(medicine_log: pl.DataFrame) -> AssetCheckResult:
    skips_df = pl.read_csv(_SKIPS_PATH, schema_overrides={"date": pl.Date})
    if skips_df.is_empty():
        return AssetCheckResult(passed=True, metadata={"skip_count": 0, "orphaned_skips": "[]"})
    log_dates = set(medicine_log["date"].to_list())
    orphaned = [str(d) for d in skips_df["date"].to_list() if d not in log_dates]
    checks = {"all_skips_within_prescriptions": len(orphaned) == 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"orphaned_skips": str(orphaned)},
    )


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "gold"],
    ins={"medicine_log": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"]))},
    automation_condition=AutomationCondition.eager(),
    description="Daily medication adherence at the gold layer for joining with health metrics",
)
def daily_medicine_adherence(medicine_log: pl.DataFrame) -> pl.DataFrame:
    return medicine_log


defs = Definitions(
    assets=[medicine_log, daily_medicine_adherence],
    asset_checks=[
        medicine_log_known_medicine_names,
        medicine_log_dosage_positive,
        medicine_log_skips_within_prescriptions,
    ],
)
