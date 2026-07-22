from __future__ import annotations

import io
import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import polars as pl
import dagster as dg
from dagster import AssetCheckResult, AssetIn, AssetKey, AutomationCondition, Definitions
from gadgetbridge_pipeline.defs.resources import S3ClientResource

_MEDICINE_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PRESCRIPTIONS_KEY = "gadgetbridge/raw/prescriptions.csv"
_SKIPS_KEY = "gadgetbridge/raw/medicine_skips.csv"
_TZ = ZoneInfo("Asia/Bangkok")


def _today() -> date:
    """'Today' in Asia/Bangkok, not the container's (usually UTC) system clock.

    The medicine schedule fires at 00:05 Bangkok, which is 17:05 UTC the
    previous day — date.today() there would return yesterday's date.
    """
    return datetime.now(_TZ).date()

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


def _read_s3_csv(s3: S3ClientResource, key: str, **read_csv_kwargs) -> pl.DataFrame:
    buffer = io.BytesIO()
    s3.get_client().download_fileobj(_MEDICINE_BUCKET, key, buffer)
    buffer.seek(0)
    return pl.read_csv(buffer, try_parse_dates=True, **read_csv_kwargs)


@dg.asset(
    name="prescriptions",
    group_name="gadgetbridge",
    key_prefix=["gadgetbridge", "bronze"],
    io_manager_key="deltalake_io_manager",
    description="Prescriptions CSV mirrored from S3, versioned in Delta Lake.",
)
def prescriptions(s3: S3ClientResource) -> pl.DataFrame:
    return _read_s3_csv(s3, _PRESCRIPTIONS_KEY, schema_overrides={"dosage_mg": pl.Float64})


@dg.asset(
    name="medicine_skips",
    group_name="gadgetbridge",
    key_prefix=["gadgetbridge", "bronze"],
    io_manager_key="deltalake_io_manager",
    description="Medicine skip records mirrored from S3, versioned in Delta Lake.",
)
def medicine_skips(s3: S3ClientResource) -> pl.DataFrame:
    return _read_s3_csv(s3, _SKIPS_KEY)


@dg.asset(
    group_name="gadgetbridge",
    io_manager_key="deltalake_io_manager",
    key_prefix=["gadgetbridge", "bronze"],
    description="Daily medication adherence log derived from prescriptions and skip records",
    automation_condition=AutomationCondition.eager(),
)
def medicine_log(context, prescriptions: pl.DataFrame, medicine_skips: pl.DataFrame) -> pl.DataFrame:
    df = build_medicine_log(prescriptions, medicine_skips, today=_today())
    context.log.info(f"Generated {df.shape[0]} medicine log rows")
    return df


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "medicine_log"]),
    blocking=True,
    name="medicine_log_dosage_positive",
)
def medicine_log_dosage_positive(medicine_log: pl.DataFrame) -> AssetCheckResult:
    if medicine_log.is_empty():
        return AssetCheckResult(
            passed=False,
            description="No dosage data to check (empty table)",
            metadata={"row_count": 0},
        )

    minimum = float(medicine_log["dosage_mg"].min())
    checks = {"is_positive": minimum > 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum},
    )


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "prescriptions"]),
    blocking=True,
    name="medicine_skips_within_prescriptions",
    additional_ins={
        "medicine_skips": AssetIn(key=AssetKey(["gadgetbridge", "bronze", "medicine_skips"])),
    },
)
def medicine_skips_within_prescriptions(
    prescriptions: pl.DataFrame,
    medicine_skips: pl.DataFrame,
) -> AssetCheckResult:
    if medicine_skips.is_empty():
        return AssetCheckResult(passed=True, metadata={"skip_count": 0, "orphaned_skips": "[]"})
    ranges = list(zip(prescriptions["start_date"].to_list(), prescriptions["end_date"].to_list()))
    orphaned = [
        str(d) for d in medicine_skips["date"].to_list()
        if not any(start <= d and (end is None or d <= end) for start, end in ranges)
    ]
    checks = {"all_skips_within_prescriptions": len(orphaned) == 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"orphaned_skips": str(orphaned)},
    )


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "medicine_skips"]),
    blocking=True,
    name="medicine_skips_not_in_future",
)
def medicine_skips_not_in_future(medicine_skips: pl.DataFrame) -> AssetCheckResult:
    today = _today()
    future = [str(d) for d in medicine_skips["date"].to_list() if d > today]
    checks = {"no_future_skips": len(future) == 0}
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"future_skips": str(future)},
    )


defs = Definitions(
    assets=[prescriptions, medicine_skips, medicine_log],
    asset_checks=[
        medicine_log_dosage_positive,
        medicine_skips_within_prescriptions,
        medicine_skips_not_in_future,
    ],
)
