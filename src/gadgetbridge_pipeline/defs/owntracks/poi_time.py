from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain


def build_derived_poi_time_daily(
    fact_location_fix: pl.DataFrame, bridge: pl.DataFrame, allocation: pl.DataFrame
) -> pl.DataFrame:
    """Time per Person, Local Date and POI from per-Fix time allocation.

    `weighted_seconds` multiplies each Fix's owned time by its bridge weight, so
    overlapping POIs split it and a Person's total is a partition of their time.
    `unweighted_seconds` credits the full time to every POI the Fix is in (presence
    per place) and double counts if summed across POIs, so say which you use.
    """
    return (
        fact_location_fix.select("fix_key", "person_key", "date_key")
        .join(bridge, on="fix_key")
        .join(allocation.select("fix_key", "seconds_owned"), on="fix_key")
        .group_by("person_key", "date_key", "poi_key")
        .agg(
            (pl.col("seconds_owned") * pl.col("weight")).sum().alias("weighted_seconds"),
            pl.col("seconds_owned").sum().cast(pl.Int64).alias("unweighted_seconds"),
            pl.len().cast(pl.Int64).alias("fix_count"),
        )
        .sort("person_key", "date_key", "poi_key")
    )


_GOLD = ["owntracks", "gold"]


@dg.asset(
    name="derived_poi_time_daily",
    group_name="owntracks",
    key_prefix=_GOLD,
    io_manager_key="owntracks_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "Time per Person, Local Date and POI from per-Fix allocation: weighted_seconds "
        "partitions a Person's time, unweighted_seconds counts presence per place."
    ),
    ins={
        "fact_location_fix": dg.AssetIn(
            key=[*_GOLD, "fact_location_fix"], partition_mapping=dg.AllPartitionMapping()
        ),
        "derived_bridge_fix_poi": dg.AssetIn(key=[*_GOLD, "derived_bridge_fix_poi"]),
        "derived_fix_time_allocation": dg.AssetIn(key=[*_GOLD, "derived_fix_time_allocation"]),
    },
    check_specs=[
        dg.AssetCheckSpec(
            name="grain_unique", asset=dg.AssetKey([*_GOLD, "derived_poi_time_daily"]),
            blocking=True,
        )
    ],
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_poi_time_daily(
    fact_location_fix: pl.DataFrame,
    derived_bridge_fix_poi: pl.DataFrame,
    derived_fix_time_allocation: pl.DataFrame,
):
    table = build_derived_poi_time_daily(
        fact_location_fix, derived_bridge_fix_poi, derived_fix_time_allocation
    )
    yield dg.Output(table)
    dupes = find_duplicate_grain(table, ["person_key", "date_key", "poi_key"])
    yield dg.AssetCheckResult(
        check_name="grain_unique", passed=not dupes, metadata={"duplicate_grain": str(dupes[:20])}
    )


defs = dg.Definitions(assets=[derived_poi_time_daily])
