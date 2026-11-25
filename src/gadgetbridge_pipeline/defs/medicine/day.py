from __future__ import annotations

from datetime import UTC, datetime

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys
from gadgetbridge_pipeline.defs.medicine.dimensions import ALL_MEDICINES
from gadgetbridge_pipeline.defs.medicine.facts import latest_watermarks


def _persons_today(
    person_keys: list[str], person_timezone: pl.DataFrame, now_utc: datetime
) -> pl.DataFrame:
    """Each Person's own Local Date at `now_utc`."""
    now = pl.DataFrame(
        {"person_key": person_keys, "now_utc": [now_utc] * len(person_keys)},
        schema={"person_key": pl.String, "now_utc": pl.Datetime("us", "UTC")},
    )
    return with_local_keys(now, "now_utc", person_timezone).select(
        "person_key", pl.col("date_key").alias("today")
    )


def build_derived_medicine_day(
    dim_prescription: pl.DataFrame,
    fact_skipped_dose: pl.DataFrame,
    fact_medicine_checkin: pl.DataFrame,
    person_timezone: pl.DataFrame,
    now_utc: datetime,
) -> pl.DataFrame:
    """One row per Prescription and date, up to the Person's own today.

    `now_utc` is passed in so the build is a pure function of its inputs.
    """
    today = _persons_today(
        dim_prescription["person_key"].unique().to_list(), person_timezone, now_utc
    )
    watermarks = latest_watermarks(fact_medicine_checkin)
    # A skip names one Medicine, or `(all)` for every Medicine of that Person.
    skipped = (
        dim_prescription.select("person_key", "medicine_key").unique()
        .join(
            fact_skipped_dose.select(
                "person_key", "date_key", pl.col("medicine_key").alias("_m")
            ),
            on="person_key",
        )
        .filter((pl.col("_m") == ALL_MEDICINES) | (pl.col("_m") == pl.col("medicine_key")))
        .select("person_key", "date_key", "medicine_key", pl.lit(True).alias("_skipped"))
        .unique(["person_key", "date_key", "medicine_key"])
    )
    return (
        dim_prescription.join(today, on="person_key", how="left")
        .with_columns(
            pl.min_horizontal(pl.col("valid_to").fill_null(pl.col("today")), pl.col("today"))
            .alias("_last")
        )
        .filter(pl.col("valid_from") <= pl.col("_last"))
        .with_columns(pl.date_ranges("valid_from", "_last").alias("date_key"))
        .explode("date_key")
        .join(watermarks, on="person_key", how="left")
        .join(skipped, on=["person_key", "date_key", "medicine_key"], how="left")
        .with_columns(
            pl.when(pl.col("_skipped").is_not_null())
            .then(pl.lit("skipped"))
            .when(pl.col("date_key") <= pl.col("skips_complete_through"))
            .then(pl.lit("confirmed_taken"))
            .otherwise(pl.lit("assumed_taken"))
            .alias("status"),
        )
        .with_columns(
            pl.when(pl.col("status") == "skipped").then(0.0).otherwise(pl.col("dosage_mg"))
            .alias("effective_dosage_mg"),
        )
        .select(
            "date_key", "person_key", "prescription_key", "status", "effective_dosage_mg"
        )
        .sort("person_key", "prescription_key", "date_key")
    )


_GOLD = ["medicine", "gold"]
_CONFORMED = ["conformed", "gold"]
_KEY = dg.AssetKey([*_GOLD, "derived_medicine_day"])


@dg.asset(
    name="derived_medicine_day",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One row per date per Prescription, up to each Person's own today. Status is skipped, "
        "confirmed_taken (on or before the watermark) or assumed_taken (after it). Rebuilt "
        "daily because 'today' advances even when no CSV changes."
    ),
    ins={
        "dim_prescription": dg.AssetIn(key=[*_GOLD, "dim_prescription"]),
        "fact_skipped_dose": dg.AssetIn(key=[*_GOLD, "fact_skipped_dose"]),
        "fact_medicine_checkin": dg.AssetIn(key=[*_GOLD, "fact_medicine_checkin"]),
        "person_timezone": dg.AssetIn(key=[*_CONFORMED, "person_timezone"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_medicine_day(
    dim_prescription: pl.DataFrame,
    fact_skipped_dose: pl.DataFrame,
    fact_medicine_checkin: pl.DataFrame,
    person_timezone: pl.DataFrame,
) -> pl.DataFrame:
    return build_derived_medicine_day(
        dim_prescription, fact_skipped_dose, fact_medicine_checkin, person_timezone,
        datetime.now(UTC),
    )


@dg.asset_check(asset=_KEY, blocking=True, name="medicine_day_grain_unique")
def medicine_day_grain_unique(derived_medicine_day: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_medicine_day, ["prescription_key", "date_key"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])})


@dg.asset_check(
    asset=_KEY,
    blocking=True,
    name="medicine_day_keys_resolve",
    additional_ins={
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
        "dim_prescription": dg.AssetIn(key=[*_GOLD, "dim_prescription"]),
    },
)
def medicine_day_keys_resolve(
    derived_medicine_day: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_person: pl.DataFrame,
    dim_prescription: pl.DataFrame,
) -> dg.AssetCheckResult:
    unresolved = {
        key: find_unresolved_keys(derived_medicine_day, key, dim)[:20]
        for key, dim in {
            "date_key": dim_date, "person_key": dim_person, "prescription_key": dim_prescription,
        }.items()
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


defs = dg.Definitions(
    assets=[derived_medicine_day],
    asset_checks=[medicine_day_grain_unique, medicine_day_keys_resolve],
)
