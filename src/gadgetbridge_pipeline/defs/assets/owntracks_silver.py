import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions

from gadgetbridge_pipeline.defs.assets.owntracks_bronze import owntracks_partitions

_EARTH_RADIUS_M = 6_371_000


def _haversine_distance_m(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    """Great-circle distance in meters between two lat/lon points."""
    lat1_r, lon1_r, lat2_r, lon2_r = lat1.radians(), lon1.radians(), lat2.radians(), lon2.radians()
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = (dlat / 2).sin() ** 2 + lat1_r.cos() * lat2_r.cos() * (dlon / 2).sin() ** 2
    return _EARTH_RADIUS_M * 2 * a.sqrt().arcsin()


def join_location_waypoints(location_records: pl.DataFrame, waypoints: pl.DataFrame) -> pl.DataFrame:
    """Attach the names of any waypoints each location record falls within
    (haversine distance from the record to the waypoint <= the waypoint's
    radius_m).

    Waypoint geofences can overlap, and the same physical waypoint can be
    synced from multiple devices, so matches are aggregated into a
    deduplicated, sorted `waypoint_names` list per location record rather
    than a single name. Dedup is by name (assumed unique for now, not by
    waypoint id). Waypoints without a name (no `desc` set) are excluded —
    they can't be represented in a `waypoint_names` list.
    """
    if location_records.is_empty():
        return location_records.with_columns(
            pl.Series("waypoint_names", [], dtype=pl.List(pl.String))
        )

    loc = location_records.with_row_index("_row_idx")

    wp = waypoints.filter(pl.col("name").is_not_null()).select(
        pl.col("lat").alias("_wp_lat"),
        pl.col("lon").alias("_wp_lon"),
        pl.col("radius_m").alias("_wp_radius_m"),
        pl.col("name").alias("_wp_name"),
    )

    matches = (
        loc.select("_row_idx", "lat", "lon")
        .join(wp, how="cross")
        .with_columns(
            _haversine_distance_m(
                pl.col("lat"), pl.col("lon"), pl.col("_wp_lat"), pl.col("_wp_lon")
            ).alias("_distance_m")
        )
        .filter(pl.col("_distance_m") <= pl.col("_wp_radius_m"))
        .group_by("_row_idx")
        .agg(pl.col("_wp_name").unique().sort().alias("waypoint_names"))
    )

    return (
        loc.join(matches, on="_row_idx", how="left")
        .with_columns(pl.col("waypoint_names").fill_null([]))
        .drop("_row_idx")
    )


@dg.asset(
    group_name="owntracks",
    io_manager_key="owntracks_deltalake_io_manager",
    key_prefix=["owntracks", "silver"],
    partitions_def=owntracks_partitions,
    metadata={"partition_expr": "year_month"},
    ins={
        "location_records": dg.AssetIn(key=dg.AssetKey(["owntracks", "bronze", "location_records"])),
        "waypoints": dg.AssetIn(key=dg.AssetKey(["owntracks", "bronze", "waypoints"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Location records annotated with the names of any waypoints they fall within (haversine distance <= radius_m).",
)
def location_records_with_waypoints(
    location_records: pl.DataFrame, waypoints: pl.DataFrame
) -> pl.DataFrame:
    return join_location_waypoints(location_records, waypoints)


defs = Definitions(assets=[location_records_with_waypoints])
