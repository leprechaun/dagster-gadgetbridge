from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.medicine.bronze import _read_s3_csv
from gadgetbridge_pipeline.defs.medicine.dimensions import ALL_MEDICINES, require_person_id
from gadgetbridge_pipeline.defs.resources import S3ClientResource, column

WHOLE_DAY_KEY = -1  # dim_time_of_day's `Whole day` member


def build_fact_skipped_dose(skips: pl.DataFrame) -> pl.DataFrame:
    """Factless fact: a row's existence is the Skipped Dose. The skips file
    records no medicine or time, so each row means every Medicine, all day.
    """
    require_person_id(skips, "medicine_skips")
    return skips.select(
        pl.col("date").alias("date_key"),
        pl.col("person_id").alias("person_key"),
        pl.lit(ALL_MEDICINES).alias("medicine_key"),
        pl.lit(WHOLE_DAY_KEY, dtype=pl.Int64).alias("time_key"),
    )


def build_fact_medicine_checkin(raw: pl.DataFrame) -> pl.DataFrame:
    """One row per Check-in. Parsing is strict: a bad timestamp or date fails
    the build instead of becoming a null that would silently lower a watermark.
    """
    return raw.select(
        pl.col("person_id").alias("person_key"),
        pl.col("checked_at")
        .str.to_datetime("%Y-%m-%dT%H:%M:%S%:z", time_zone="UTC", strict=True)
        .alias("checked_at_utc"),
        pl.col("skips_complete_through").str.to_date("%Y-%m-%d", strict=True),
    )


def latest_watermarks(checkins: pl.DataFrame) -> pl.DataFrame:
    """Each Person's watermark: the latest `skips_complete_through` ever confirmed."""
    return checkins.group_by("person_key").agg(
        pl.col("skips_complete_through").max()
    ).sort("person_key")


_BRONZE = ["medicine", "bronze"]
_GOLD = ["medicine", "gold"]
_CONFORMED = ["conformed", "gold"]
CHECKINS_KEY = "medicine/raw/medicine_checkins.csv"


@dg.asset(
    name="medicine_checkins",
    group_name="medicine",
    key_prefix=_BRONZE,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"s3", "deltalake"},
    description=(
        "Append-only Check-ins mirrored from S3 as raw text; each row confirms a Person's "
        "Skipped Doses are complete through a date. Parsed strictly in fact_medicine_checkin."
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("person_id", "Person who checked in; a person_id in persons"),
        column("checked_at", "When the check-in was made, ISO 8601 with offset"),
        column("skips_complete_through", "Last date whose skips are confirmed complete"),
    ])},
)
def medicine_checkins(s3: S3ClientResource) -> pl.DataFrame:
    frame = _read_s3_csv(
        s3,
        CHECKINS_KEY,
        schema_overrides={
            "person_id": pl.String, "checked_at": pl.String, "skips_complete_through": pl.String,
        },
    )
    return require_person_id(frame, "medicine_checkins.csv")


@dg.asset(
    name="fact_skipped_dose",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "Factless fact: one row per Skipped Dose. Legacy rows mean every Medicine "
        "((all)) for the Whole day (time key -1)."
    ),
    ins={"medicine_skips": dg.AssetIn(key=[*_BRONZE, "medicine_skips"])},
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_skipped_dose(medicine_skips: pl.DataFrame) -> pl.DataFrame:
    return build_fact_skipped_dose(medicine_skips)


@dg.asset(
    name="fact_medicine_checkin",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One Check-in. History is never lost; a Person's watermark is derived as the latest "
        "skips_complete_through."
    ),
    ins={"medicine_checkins": dg.AssetIn(key=[*_BRONZE, "medicine_checkins"])},
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_medicine_checkin(medicine_checkins: pl.DataFrame) -> pl.DataFrame:
    return build_fact_medicine_checkin(medicine_checkins)


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "fact_skipped_dose"]),
    blocking=True,
    name="skipped_dose_keys_resolve",
    additional_ins={
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
        "dim_time_of_day": dg.AssetIn(key=[*_CONFORMED, "dim_time_of_day"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
        "dim_medicine": dg.AssetIn(key=[*_GOLD, "dim_medicine"]),
    },
)
def skipped_dose_keys_resolve(
    fact_skipped_dose: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_time_of_day: pl.DataFrame,
    dim_person: pl.DataFrame,
    dim_medicine: pl.DataFrame,
) -> dg.AssetCheckResult:
    unresolved = {
        key: find_unresolved_keys(fact_skipped_dose, key, dim)[:20]
        for key, dim in {
            "date_key": dim_date, "time_key": dim_time_of_day,
            "person_key": dim_person, "medicine_key": dim_medicine,
        }.items()
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "fact_medicine_checkin"]),
    blocking=True,
    name="checkin_keys_resolve",
    additional_ins={
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
    },
)
def checkin_keys_resolve(
    fact_medicine_checkin: pl.DataFrame, dim_person: pl.DataFrame, dim_date: pl.DataFrame
) -> dg.AssetCheckResult:
    unresolved = {
        "person_key": find_unresolved_keys(fact_medicine_checkin, "person_key", dim_person)[:20],
        "skips_complete_through": find_unresolved_keys(
            fact_medicine_checkin.rename({"skips_complete_through": "date_key"}),
            "date_key", dim_date,
        )[:20],
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "fact_medicine_checkin"]),
    blocking=True,
    name="checkin_unique",
)
def checkin_unique(fact_medicine_checkin: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(fact_medicine_checkin, ["person_key", "checked_at_utc"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_checkins": str(dupes[:20])})


defs = dg.Definitions(
    assets=[medicine_checkins, fact_skipped_dose, fact_medicine_checkin],
    asset_checks=[skipped_dose_keys_resolve, checkin_keys_resolve, checkin_unique],
)
