from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain


def build_derived_medicine_adherence_daily(
    derived_medicine_day: pl.DataFrame, dim_prescription: pl.DataFrame
) -> pl.DataFrame:
    """Daily adherence per Person. Confirmed and assumed doses are kept apart so
    totals that must not include guesses can use `confirmed_mg` alone.
    """
    return (
        derived_medicine_day.join(
            dim_prescription.select("prescription_key", "dosage_mg"), on="prescription_key",
            how="left",
        )
        .group_by("person_key", "date_key")
        .agg(
            pl.col("dosage_mg").sum().alias("prescribed_mg"),
            pl.when(pl.col("status") == "confirmed_taken")
            .then(pl.col("effective_dosage_mg")).otherwise(0.0).sum().alias("confirmed_mg"),
            pl.when(pl.col("status") == "assumed_taken")
            .then(pl.col("effective_dosage_mg")).otherwise(0.0).sum().alias("assumed_mg"),
            (pl.col("status") == "skipped").sum().cast(pl.Int64).alias("skipped_count"),
        )
        .sort("person_key", "date_key")
    )


_GOLD = ["medicine", "gold"]


@dg.asset(
    name="derived_medicine_adherence_daily",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "Daily adherence per Person: prescribed, confirmed, assumed and skipped doses, from "
        "derived_medicine_day. Confirmed and assumed are kept apart."
    ),
    ins={
        "derived_medicine_day": dg.AssetIn(key=[*_GOLD, "derived_medicine_day"]),
        "dim_prescription": dg.AssetIn(key=[*_GOLD, "dim_prescription"]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="grain_unique",
            asset=dg.AssetKey([*_GOLD, "derived_medicine_adherence_daily"]), blocking=True,
        )
    ],
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_medicine_adherence_daily(
    derived_medicine_day: pl.DataFrame, dim_prescription: pl.DataFrame
):
    table = build_derived_medicine_adherence_daily(derived_medicine_day, dim_prescription)
    yield dg.Output(table)
    dupes = find_duplicate_grain(table, ["person_key", "date_key"])
    yield dg.AssetCheckResult(
        check_name="grain_unique", passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])}
    )


defs = dg.Definitions(assets=[derived_medicine_adherence_daily])
