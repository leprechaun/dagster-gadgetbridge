from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.constants import VISIT_GAP
from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys
from gadgetbridge_pipeline.defs.owntracks.silver import build_poi_visit_spans
from gadgetbridge_pipeline.defs.poi.dimensions import NO_POI


def build_derived_fix_time_allocation(fixes: pl.DataFrame) -> pl.DataFrame:
    """The one place time is attributed: a Fix owns the time until the next Fix
    of the same phone, if that is no more than the visit gap. Past the gap
    tracking is treated as paused, so the Fix owns none, and the last Fix of a
    phone owns none. Ownership never exceeds the gap.
    """
    ordered = fixes.sort("phone_key", "timestamp_utc", "fix_key")
    delta = (
        pl.col("timestamp_utc").shift(-1).over("phone_key") - pl.col("timestamp_utc")
    ).dt.total_seconds()
    return ordered.select(
        "fix_key", "phone_key", "person_key",
        pl.when(delta.is_not_null() & (delta <= VISIT_GAP.total_seconds()))
        .then(delta).otherwise(0).cast(pl.Int64).alias("seconds_owned"),
    )


def build_derived_poi_visit(
    fixes: pl.DataFrame, bridge: pl.DataFrame, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """One row per Visit: a contiguous stay of one phone inside one POI.

    The visit rule itself is `build_poi_visit_spans` (no second implementation);
    this adapts Fix keys to it. A Fix in `(none)` is in no POI, so it ends a
    Visit but never starts one. A phone owned by more than one Person (it should
    not be) takes the first Person alphabetically, and the Person-agreement
    check on the Fix fact would already have failed.
    """
    pois_per_fix = (
        bridge.filter(pl.col("poi_key") != NO_POI)
        .group_by("fix_key").agg(pl.col("poi_key").sort().alias("point_of_interest"))
    )
    pings = (
        fixes.join(pois_per_fix, on="fix_key", how="left")
        .with_columns(pl.col("point_of_interest").fill_null([]))
        .select(
            pl.col("phone_key").alias("user"),
            pl.lit("").alias("device"),
            pl.col("timestamp_utc").alias("timestamp"),
            "point_of_interest",
        )
    )
    spans = build_poi_visit_spans(pings, VISIT_GAP)
    owner = (
        fixes.group_by("phone_key").agg(pl.col("person_key").min())
    )
    visits = spans.select(
        pl.col("user").alias("phone_key"),
        pl.col("point_of_interest").alias("poi_key"),
        pl.col("start").alias("start_utc"),
        pl.col("end").alias("end_utc"),
        "duration_minutes",
    ).join(owner, on="phone_key", how="left")
    start = with_local_keys(visits, "start_utc", person_timezone).rename(
        {"date_key": "start_date_key"}
    ).drop("time_key")
    both = with_local_keys(start, "end_utc", person_timezone).rename(
        {"date_key": "end_date_key"}
    ).drop("time_key")
    return both.select(
        "phone_key", "person_key", "poi_key", "start_date_key", "end_date_key",
        "start_utc", "end_utc", "duration_minutes",
    ).sort("phone_key", "poi_key", "start_utc")


_GOLD = ["owntracks", "gold"]
_ALL_FIXES = dg.AssetIn(
    key=[*_GOLD, "fact_location_fix"], partition_mapping=dg.AllPartitionMapping()
)
_VISIT = dg.AssetKey([*_GOLD, "derived_poi_visit"])
_ALLOCATION = dg.AssetKey([*_GOLD, "derived_fix_time_allocation"])


@dg.asset(
    name="derived_poi_visit",
    group_name="owntracks",
    key_prefix=_GOLD,
    io_manager_key="owntracks_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One Visit: a contiguous stay of one phone inside one POI, over all history. Ends on "
        "a fix outside the POI or a gap over the shared visit-gap constant."
    ),
    ins={
        "fact_location_fix": _ALL_FIXES,
        "derived_bridge_fix_poi": dg.AssetIn(key=[*_GOLD, "derived_bridge_fix_poi"]),
        "person_timezone": dg.AssetIn(key=["conformed", "gold", "person_timezone"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_poi_visit(
    fact_location_fix: pl.DataFrame,
    derived_bridge_fix_poi: pl.DataFrame,
    person_timezone: pl.DataFrame,
) -> pl.DataFrame:
    return build_derived_poi_visit(fact_location_fix, derived_bridge_fix_poi, person_timezone)


@dg.asset(
    name="derived_fix_time_allocation",
    group_name="owntracks",
    key_prefix=_GOLD,
    io_manager_key="owntracks_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "Seconds each Fix owns: the time to the next Fix of its phone if within the visit "
        "gap, else none. The only place time is attributed."
    ),
    ins={"fact_location_fix": _ALL_FIXES},
    automation_condition=dg.AutomationCondition.eager(),
)
def derived_fix_time_allocation(fact_location_fix: pl.DataFrame) -> pl.DataFrame:
    return build_derived_fix_time_allocation(fact_location_fix)


@dg.asset_check(asset=_VISIT, blocking=True, name="poi_visit_grain_unique_and_ordered")
def poi_visit_grain_unique_and_ordered(derived_poi_visit: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_poi_visit, ["phone_key", "poi_key", "start_utc"])
    backwards = derived_poi_visit.filter(pl.col("end_utc") < pl.col("start_utc")).height
    return dg.AssetCheckResult(
        passed=not dupes and backwards == 0,
        metadata={"duplicate_grain": str(dupes[:20]), "end_before_start": backwards},
    )


@dg.asset_check(asset=_ALLOCATION, blocking=True, name="fix_time_allocation_valid")
def fix_time_allocation_valid(derived_fix_time_allocation: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_fix_time_allocation, ["fix_key"])
    out_of_range = derived_fix_time_allocation.filter(
        (pl.col("seconds_owned") < 0) | (pl.col("seconds_owned") > VISIT_GAP.total_seconds())
    ).height
    return dg.AssetCheckResult(
        passed=not dupes and out_of_range == 0,
        metadata={"duplicate_fix_keys": str(dupes[:20]), "out_of_range_rows": out_of_range},
    )


defs = dg.Definitions(
    assets=[derived_poi_visit, derived_fix_time_allocation],
    asset_checks=[poi_visit_grain_unique_and_ordered, fix_time_allocation_valid],
)
