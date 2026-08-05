from datetime import timedelta

import dagster as dg
import polars as pl
from dagster import AssetCheckResult, AutomationCondition, Definitions

from gadgetbridge_pipeline.defs.owntracks.bronze import owntracks_partitions

_EARTH_RADIUS_M = 6_371_000

# How long a gap between pings can be before a visit to the same POI is
# considered to have ended rather than just under-sampled. OwnTracks pings
# on movement plus a periodic heartbeat, so a gap past this is treated as
# "tracking paused" rather than "still there" — tune to your device's
# actual reporting interval.
_POI_VISIT_GAP_THRESHOLD = timedelta(minutes=30)

_POI_VISIT_SPAN_SCHEMA = pl.Schema({
    "user": pl.String,
    "device": pl.String,
    "point_of_interest": pl.String,
    "start": pl.Datetime(time_unit="us", time_zone="UTC"),
    "end": pl.Datetime(time_unit="us", time_zone="UTC"),
    "duration_minutes": pl.Int64,
})


def _haversine_distance_m(lat1: pl.Expr, lon1: pl.Expr, lat2: pl.Expr, lon2: pl.Expr) -> pl.Expr:
    """Great-circle distance in meters between two lat/lon points."""
    lat1_r, lon1_r, lat2_r, lon2_r = lat1.radians(), lon1.radians(), lat2.radians(), lon2.radians()
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = (dlat / 2).sin() ** 2 + lat1_r.cos() * lat2_r.cos() * (dlon / 2).sin() ** 2
    return _EARTH_RADIUS_M * 2 * a.sqrt().arcsin()


# point-of-interest circles can overlap each other (not checked anywhere), so that
# kind keeps a list column. Region/area/territory are rectangles, and
# `poi_no_same_kind_overlap` guarantees at most one same-kind rectangle match per
# location, so those kinds get a single nullable scalar column.
_LIST_KIND_COLUMNS = {"point-of-interest": "point_of_interest"}
_SCALAR_KIND_COLUMNS = {"area": "area", "region": "region", "territory": "territory"}


def join_location_poi(location_records: pl.DataFrame, poi: pl.DataFrame) -> pl.DataFrame:
    """Attach the name of any POI (circle or axis-aligned rectangle) each
    location record falls within, one column per POI kind.

    Circles: haversine distance from the record to the POI <= radius_m.
    Rectangles: the record's lat/lon fall within
    [lon_min, lon_max] x [lat_min, lat_max] (inclusive).

    POI kinds can nest (a point-of-interest inside an area inside a region),
    so a location can match several POIs of different kinds at once. Each
    kind gets its own column (`point_of_interest`, `area`, `region`,
    `territory`); unmatched kinds are null (or an empty list, for
    `point_of_interest`).
    """
    if location_records.is_empty():
        return location_records.with_columns(
            [
                pl.Series(column, [], dtype=pl.List(pl.String))
                for column in _LIST_KIND_COLUMNS.values()
            ]
            + [
                pl.Series(column, [], dtype=pl.String)
                for column in _SCALAR_KIND_COLUMNS.values()
            ]
        )

    loc = location_records.with_row_index("_row_idx")
    loc_pts = loc.select("_row_idx", "lat", "lon")

    circles = poi.filter(pl.col("geometry_type") == "circle").select(
        pl.col("name").alias("_poi_name"),
        pl.col("kind").alias("_kind"),
        pl.col("lat").alias("_poi_lat"),
        pl.col("lon").alias("_poi_lon"),
        pl.col("radius_m").alias("_poi_radius_m"),
    )
    rectangles = poi.filter(pl.col("geometry_type") == "rectangle").select(
        pl.col("name").alias("_poi_name"),
        pl.col("kind").alias("_kind"),
        pl.col("lon_min").alias("_poi_lon_min"),
        pl.col("lon_max").alias("_poi_lon_max"),
        pl.col("lat_min").alias("_poi_lat_min"),
        pl.col("lat_max").alias("_poi_lat_max"),
    )

    circle_matches = (
        loc_pts.join(circles, how="cross")
        .with_columns(
            _haversine_distance_m(
                pl.col("lat"), pl.col("lon"), pl.col("_poi_lat"), pl.col("_poi_lon")
            ).alias("_distance_m")
        )
        .filter(pl.col("_distance_m") <= pl.col("_poi_radius_m"))
        .select("_row_idx", "_poi_name", "_kind")
    )

    rectangle_matches = (
        loc_pts.join(rectangles, how="cross")
        .filter(
            pl.col("lon").is_between(pl.col("_poi_lon_min"), pl.col("_poi_lon_max"))
            & pl.col("lat").is_between(pl.col("_poi_lat_min"), pl.col("_poi_lat_max"))
        )
        .select("_row_idx", "_poi_name", "_kind")
    )

    matches = pl.concat([circle_matches, rectangle_matches])

    result = loc
    for kind, column in _LIST_KIND_COLUMNS.items():
        agg = (
            matches.filter(pl.col("_kind") == kind)
            .group_by("_row_idx")
            .agg(pl.col("_poi_name").unique().sort().alias(column))
        )
        result = result.join(agg, on="_row_idx", how="left").with_columns(
            pl.col(column).fill_null([])
        )

    for kind, column in _SCALAR_KIND_COLUMNS.items():
        agg = (
            matches.filter(pl.col("_kind") == kind)
            .group_by("_row_idx")
            .agg(pl.col("_poi_name").sort().first().alias(column))
        )
        result = result.join(agg, on="_row_idx", how="left")

    return result.drop("_row_idx")


@dg.asset(
    group_name="owntracks",
    io_manager_key="owntracks_deltalake_io_manager",
    key_prefix=["owntracks", "silver"],
    partitions_def=owntracks_partitions,
    metadata={"partition_expr": "year_month"},
    ins={
        "location_records": dg.AssetIn(
            key=dg.AssetKey(["owntracks", "bronze", "location_records"])
        ),
        "poi": dg.AssetIn(key=dg.AssetKey(["poi", "bronze", "points_of_interest"])),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "Location records annotated with the POI (circle or rectangle) "
        "each falls within, one column per kind."
    ),
)
def location_records_with_poi(
    location_records: pl.DataFrame, poi: pl.DataFrame
) -> pl.DataFrame:
    return join_location_poi(location_records, poi).drop(
        "conn", "ssid", "bssid", "vac", "batt", "bs"
    )


def build_poi_visit_spans(
    location_records_with_poi: pl.DataFrame,
    gap_threshold: timedelta,
) -> pl.DataFrame:
    """Contiguous visit spans to each named point-of-interest.

    A visit continues across consecutive pings that are both inside the same
    named POI, as long as the gap between them doesn't exceed `gap_threshold`;
    a ping outside that POI, or a longer gap, ends it. Point-of-interest
    circles can overlap each other (unlike area/region/territory, which
    `poi_no_same_kind_overlap` guarantees are mutually exclusive per kind),
    so there's no single "current location" timeline — visits are computed
    independently per named POI instead.
    """
    if location_records_with_poi.is_empty():
        return pl.DataFrame(schema=_POI_VISIT_SPAN_SCHEMA)

    pings = (
        location_records_with_poi
        .with_row_index("_ping_idx")
        .select("_ping_idx", "user", "device", "timestamp", "point_of_interest")
    )

    hits = (
        pings
        .select("_ping_idx", "user", "device", "point_of_interest")
        .explode("point_of_interest")
        .drop_nulls("point_of_interest")
        .rename({"point_of_interest": "name"})
    )

    if hits.is_empty():
        return pl.DataFrame(schema=_POI_VISIT_SPAN_SCHEMA)

    names = hits.select("user", "device", "name").unique()

    flagged = (
        names.join(pings.drop("point_of_interest"), on=["user", "device"], how="left")
        .join(
            hits.select("_ping_idx", "name", pl.lit(True).alias("is_inside")),
            on=["_ping_idx", "name"],
            how="left",
        )
        .with_columns(pl.col("is_inside").fill_null(False))
        .sort(["user", "device", "name", "timestamp"])
        .with_columns(
            pl.col("is_inside").shift(1).over(["user", "device", "name"]).alias("_prev_inside"),
            pl.col("timestamp").shift(1).over(["user", "device", "name"]).alias("_prev_timestamp"),
        )
        .with_columns(
            (
                pl.col("_prev_inside").is_null()
                | (pl.col("is_inside") != pl.col("_prev_inside"))
                | ((pl.col("timestamp") - pl.col("_prev_timestamp")) > gap_threshold)
            ).alias("_new_span")
        )
        .with_columns(
            pl.col("_new_span").cast(pl.Int32).cum_sum().over(["user", "device", "name"])
            .alias("_span_id")
        )
    )

    return (
        flagged
        .filter(pl.col("is_inside"))
        .group_by(["user", "device", "name", "_span_id"])
        .agg(
            pl.col("timestamp").min().alias("start"),
            pl.col("timestamp").max().alias("end"),
        )
        .with_columns(
            (pl.col("end") - pl.col("start")).dt.total_minutes().alias("duration_minutes")
        )
        .rename({"name": "point_of_interest"})
        .select(["user", "device", "point_of_interest", "start", "end", "duration_minutes"])
        .sort(["user", "device", "point_of_interest", "start"])
    )


@dg.asset(
    group_name="owntracks",
    io_manager_key="owntracks_deltalake_io_manager",
    key_prefix=["owntracks", "silver"],
    ins={
        "location_records_with_poi": dg.AssetIn(
            key=dg.AssetKey(["owntracks", "silver", "location_records_with_poi"]),
            partition_mapping=dg.AllPartitionMapping(),
        ),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "Contiguous visit spans to each named point-of-interest, computed over the full "
        "location history rather than per month — unlike its upstream, this asset isn't "
        "partitioned, since a single visit (e.g. being at 'home') routinely spans month "
        "partition boundaries."
    ),
)
def poi_visit_spans(location_records_with_poi: pl.DataFrame) -> pl.DataFrame:
    return build_poi_visit_spans(location_records_with_poi, _POI_VISIT_GAP_THRESHOLD)


@dg.asset_check(
    asset=dg.AssetKey(["owntracks", "silver", "poi_visit_spans"]),
    blocking=True,
    name="poi_visit_spans_start_before_end",
)
def poi_visit_spans_start_before_end(poi_visit_spans: pl.DataFrame) -> AssetCheckResult:
    if poi_visit_spans.is_empty():
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    bad = poi_visit_spans.filter(pl.col("start") > pl.col("end")).height
    return AssetCheckResult(passed=bad == 0, metadata={"invalid_row_count": bad})


defs = Definitions(
    assets=[location_records_with_poi, poi_visit_spans],
    asset_checks=[poi_visit_spans_start_before_end],
)
