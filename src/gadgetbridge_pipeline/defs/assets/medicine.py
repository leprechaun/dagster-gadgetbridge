from __future__ import annotations

import os
from datetime import date, timedelta

import polars as pl
import dagster as dg
from dagster import AssetCheckResult, Definitions
from gadgetbridge_pipeline.defs.resources import S3ClientResource

_MEDICINE_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PRESCRIPTIONS_KEY = "gadgetbridge/raw/prescriptions.csv"
_SKIPS_KEY = "gadgetbridge/raw/medicine_skips.csv"

_EMPTY_SCHEMA = {
    "date": pl.Date,
    "medicine": pl.String,
    "dosage_mg": pl.Float64,
    "taken": pl.Boolean,
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
            (~pl.col("date").is_in(list(skip_dates))).alias("taken")
        ).with_columns(
            (pl.col("taken").cast(int) * pl.col("dosage_mg")).alias("effective_dosage")
        )
    )

    return df


def _download_csv(client, key: str, local_path: str) -> None:
    client.download_file(_MEDICINE_BUCKET, key, local_path)


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "bronze"],
    description="Daily medication adherence log derived from prescriptions and skip records",
)
def medicine_log(context, s3: S3ClientResource) -> pl.DataFrame:
    client = s3.get_client()

    _download_csv(client, _PRESCRIPTIONS_KEY, "/tmp/medicine_prescriptions.csv")
    prescriptions = pl.read_csv(
        "/tmp/medicine_prescriptions.csv",
        schema_overrides={"start_date": pl.Date, "end_date": pl.Date, "dosage_mg": pl.Float64},
        null_values=[""],
    )

    _download_csv(client, _SKIPS_KEY, "/tmp/medicine_skips.csv")
    skips = pl.read_csv(
        "/tmp/medicine_skips.csv",
        schema_overrides={"date": pl.Date}
    )

    df = build_medicine_log(prescriptions, skips, today=date.today())
    context.log.info(f"Generated {df.shape[0]} medicine log rows")
    return df


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
def medicine_log_skips_within_prescriptions(
    medicine_log: pl.DataFrame,
    s3: S3ClientResource,
) -> AssetCheckResult:
    s3.get_client().download_file(_MEDICINE_BUCKET, _SKIPS_KEY, "/tmp/medicine_skips_check.csv")
    skips_df = pl.read_csv("/tmp/medicine_skips_check.csv", schema_overrides={"date": pl.Date})
    if skips_df.is_empty():
        return AssetCheckResult(passed=True, metadata={"skip_count": 0, "orphaned_skips": "[]"})
    log_dates = set(medicine_log["date"].to_list())
    orphaned = [str(d) for d in skips_df["date"].to_list() if d not in log_dates]
    checks = {"all_skips_within_prescriptions": len(orphaned) == 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"orphaned_skips": str(orphaned)},
    )


defs = Definitions(
    assets=[medicine_log],
    asset_checks=[
        medicine_log_dosage_positive,
        medicine_log_skips_within_prescriptions,
    ],
)
