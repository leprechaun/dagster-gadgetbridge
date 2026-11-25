"""POI membership of Fixes: derived, non-partitioned, rebuilt over all history
whenever the POI file changes (the POI is hand-edited, so storing the match on
the partitioned fix fact would leave old partitions matched against old shapes).
"""

from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.owntracks.silver import join_location_poi
from gadgetbridge_pipeline.defs.poi.dimensions import NO_POI


def _matched(fixes: pl.DataFrame, poi: pl.DataFrame) -> pl.DataFrame:
    return join_location_poi(fixes.select("fix_key", "lat", "lon"), poi)


def build_derived_fix_poi_tiers(fixes: pl.DataFrame, poi: pl.DataFrame) -> pl.DataFrame:
    """One row per Fix: the area, region and territory it falls in, `(none)` for
    each tier it is outside. Same-kind rectangles never overlap, so each tier
    is a single key.
    """
    return _matched(fixes, poi).select(
        "fix_key",
        pl.col("area").fill_null(NO_POI).alias("area_key"),
        pl.col("region").fill_null(NO_POI).alias("region_key"),
        pl.col("territory").fill_null(NO_POI).alias("territory_key"),
    )


def build_derived_bridge_fix_poi(fixes: pl.DataFrame, poi: pl.DataFrame) -> pl.DataFrame:
    """One row per Fix and matched point-of-interest, weighted 1/N across the N
    matches so per-POI counts can be summed without double counting. A Fix in no
    circle has one row for `(none)` at weight 1; `(unknown)` is never used.
    """
    matched = _matched(fixes, poi).select(
        "fix_key",
        pl.when(pl.col("point_of_interest").list.len() == 0)
        .then(pl.lit([NO_POI]))
        .otherwise(pl.col("point_of_interest"))
        .alias("poi_key"),
    )
    return (
        matched.explode("poi_key")
        .with_columns((1.0 / pl.len().over("fix_key")).alias("weight"))
    )


def find_fixes_with_bad_weight(
    fixes: pl.DataFrame, bridge: pl.DataFrame, tolerance: float = 1e-9
) -> list[int]:
    """fix_keys whose bridge weights do not sum to 1 within `tolerance`. Starts
    from the Fixes and left-joins the bridge, so a Fix with no rows fails
    instead of being invisible.
    """
    sums = bridge.group_by("fix_key").agg(pl.col("weight").sum().alias("_total"))
    bad = (
        fixes.select("fix_key").unique()
        .join(sums, on="fix_key", how="left")
        .filter(pl.col("_total").is_null() | ((pl.col("_total") - 1.0).abs() > tolerance))
        .sort("fix_key")
    )
    return bad["fix_key"].to_list()


_GOLD = ["owntracks", "gold"]
_FIX = dg.AssetKey([*_GOLD, "fact_location_fix"])
_POI = dg.AssetKey(["poi", "bronze", "points_of_interest"])
_DIM_POI = dg.AssetKey(["poi", "gold", "dim_poi"])
_TIERS = dg.AssetKey([*_GOLD, "derived_fix_poi_tiers"])
_BRIDGE = dg.AssetKey([*_GOLD, "derived_bridge_fix_poi"])
_ALL_FIXES = dg.AssetIn(key=_FIX, partition_mapping=dg.AllPartitionMapping())


def _derived_asset(name: str, description: str):
    return dg.asset(
        name=name,
        group_name="owntracks",
        key_prefix=_GOLD,
        io_manager_key="owntracks_deltalake_io_manager",
        kinds={"deltalake"},
        description=description,
        ins={"fact_location_fix": _ALL_FIXES, "points_of_interest": dg.AssetIn(key=_POI)},
        automation_condition=dg.AutomationCondition.eager(),
    )


@_derived_asset(
    "derived_fix_poi_tiers",
    "One row per Fix: the area, region and territory it falls in. Not partitioned; rebuilt "
    "over all history when the POI file changes.",
)
def derived_fix_poi_tiers(
    fact_location_fix: pl.DataFrame, points_of_interest: pl.DataFrame
) -> pl.DataFrame:
    return build_derived_fix_poi_tiers(fact_location_fix, points_of_interest)


@_derived_asset(
    "derived_bridge_fix_poi",
    "One row per Fix and matched point-of-interest, weighted 1/N; (none) at weight 1 for a Fix "
    "in no circle. Not partitioned; rebuilt over all history when the POI file changes.",
)
def derived_bridge_fix_poi(
    fact_location_fix: pl.DataFrame, points_of_interest: pl.DataFrame
) -> pl.DataFrame:
    return build_derived_bridge_fix_poi(fact_location_fix, points_of_interest)


@dg.asset_check(
    asset=_BRIDGE,
    blocking=True,
    name="bridge_weights_sum_to_one",
    additional_ins={"fact_location_fix": _ALL_FIXES},
)
def bridge_weights_sum_to_one(
    derived_bridge_fix_poi: pl.DataFrame, fact_location_fix: pl.DataFrame
) -> dg.AssetCheckResult:
    bad = find_fixes_with_bad_weight(fact_location_fix, derived_bridge_fix_poi)
    return dg.AssetCheckResult(
        passed=not bad, metadata={"bad_fix_count": len(bad), "sample": str(bad[:20])}
    )


@dg.asset_check(
    asset=_BRIDGE,
    blocking=True,
    name="bridge_grain_unique_and_poi_resolves",
    additional_ins={"dim_poi": dg.AssetIn(key=_DIM_POI)},
)
def bridge_grain_unique_and_poi_resolves(
    derived_bridge_fix_poi: pl.DataFrame, dim_poi: pl.DataFrame
) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_bridge_fix_poi, ["fix_key", "poi_key"])
    unresolved = find_unresolved_keys(derived_bridge_fix_poi, "poi_key", dim_poi)[:20]
    return dg.AssetCheckResult(
        passed=not dupes and not unresolved,
        metadata={"duplicate_grain": str(dupes[:20]), "unresolved_poi": str(unresolved)},
    )


@dg.asset_check(
    asset=_TIERS,
    blocking=True,
    name="tiers_grain_unique_and_keys_resolve",
    additional_ins={"dim_poi": dg.AssetIn(key=_DIM_POI)},
)
def tiers_grain_unique_and_keys_resolve(
    derived_fix_poi_tiers: pl.DataFrame, dim_poi: pl.DataFrame
) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(derived_fix_poi_tiers, ["fix_key"])
    unresolved = {
        key: find_unresolved_keys(derived_fix_poi_tiers, key, dim_poi.rename({"poi_key": key}))[:20]
        for key in ("area_key", "region_key", "territory_key")
    }
    return dg.AssetCheckResult(
        passed=not dupes and not any(unresolved.values()),
        metadata={"duplicate_fix_keys": str(dupes[:20]), "unresolved": str(unresolved)},
    )


defs = dg.Definitions(
    assets=[derived_fix_poi_tiers, derived_bridge_fix_poi],
    asset_checks=[
        bridge_weights_sum_to_one,
        bridge_grain_unique_and_poi_resolves,
        tiers_grain_unique_and_keys_resolve,
    ],
)
