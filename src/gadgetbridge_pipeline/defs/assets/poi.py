import io
import json
import os

import polars as pl
import dagster as dg
from dagster import AssetCheckResult, Definitions

from gadgetbridge_pipeline.defs.resources import S3ClientResource

_POI_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_POI_KEY = "poi/raw/poi.geojson"

_ALLOWED_KINDS = {"point-of-interest", "area", "region", "territory"}

_POI_SCHEMA = pl.Schema({
    "name":          pl.String,
    "kind":          pl.String,
    "geometry_type": pl.String,
    "lat":           pl.Float64,
    "lon":           pl.Float64,
    "radius_m":      pl.Float64,
    "lon_min":       pl.Float64,
    "lon_max":       pl.Float64,
    "lat_min":       pl.Float64,
    "lat_max":       pl.Float64,
})


def _rectangle_bounds(ring: list[list[float]]) -> tuple[float, float, float, float] | None:
    """Validate a GeoJSON Polygon ring is a closed, axis-aligned rectangle
    and return (lon_min, lon_max, lat_min, lat_max), or None if it isn't.
    """
    if len(ring) != 5 or ring[0] != ring[-1]:
        return None
    corners = [tuple(p) for p in ring[:-1]]
    if len(set(corners)) != 4:
        return None
    lons = sorted({round(p[0], 9) for p in corners})
    lats = sorted({round(p[1], 9) for p in corners})
    if len(lons) != 2 or len(lats) != 2:
        return None
    # 4 distinct corners drawn from only 2 lon values and 2 lat values must,
    # by pigeonhole, be exactly the 4 (lon, lat) combinations — i.e. a
    # rectangle. No further check needed.
    return lons[0], lons[1], lats[0], lats[1]


def parse_poi_feature(feature: dict) -> dict | None:
    """Parse a single GeoJSON feature into a POI row.

    Returns None if the feature has no name, or its geometry isn't a Point
    or a valid axis-aligned rectangle Polygon — these can't be represented
    at all, as opposed to a missing/invalid `kind` or `radius_m`, which are
    kept and caught by asset checks instead (they're a real, nameable POI
    with incomplete metadata, not garbage).
    """
    props = feature.get("properties") or {}
    name = props.get("name")
    if not name:
        return None

    geometry = feature.get("geometry") or {}
    geom_type = geometry.get("type")

    if geom_type == "Point":
        lon, lat = geometry["coordinates"]
        return {
            "name": name,
            "kind": props.get("kind"),
            "geometry_type": "circle",
            "lat": lat,
            "lon": lon,
            "radius_m": props.get("radius_m"),
            "lon_min": None,
            "lon_max": None,
            "lat_min": None,
            "lat_max": None,
        }

    if geom_type == "Polygon":
        rings = geometry.get("coordinates") or []
        if len(rings) != 1:
            return None
        bounds = _rectangle_bounds(rings[0])
        if bounds is None:
            return None
        lon_min, lon_max, lat_min, lat_max = bounds
        return {
            "name": name,
            "kind": props.get("kind"),
            "geometry_type": "rectangle",
            "lat": None,
            "lon": None,
            "radius_m": None,
            "lon_min": lon_min,
            "lon_max": lon_max,
            "lat_min": lat_min,
            "lat_max": lat_max,
        }

    return None


def parse_poi_geojson(raw: bytes) -> tuple[list[dict], list[str]]:
    """Parse a GeoJSON FeatureCollection into POI rows.

    Returns (records, dropped) where dropped holds the name (or a
    placeholder) of each feature that couldn't be parsed.
    """
    data = json.loads(raw)
    records = []
    dropped = []
    for feature in data.get("features", []):
        record = parse_poi_feature(feature)
        if record is None:
            dropped.append((feature.get("properties") or {}).get("name") or "<unnamed>")
            continue
        records.append(record)
    return records, dropped


def _to_dataframe(records: list[dict]) -> pl.DataFrame:
    if not records:
        return pl.DataFrame(schema=_POI_SCHEMA)
    return pl.DataFrame(records, schema=_POI_SCHEMA)


def find_same_kind_overlaps(rectangles: pl.DataFrame) -> list[tuple[str, str]]:
    """Pure logic: given rectangle rows (name, kind, lon_min, lon_max,
    lat_min, lat_max), return (name, name) pairs that share a kind and
    whose bounding boxes overlap. Rectangles of different kinds are allowed
    to overlap (nesting a poi/area inside a region is the whole point).
    """
    items = list(
        rectangles.select("name", "kind", "lon_min", "lon_max", "lat_min", "lat_max")
        .iter_rows(named=True)
    )
    pairs = []
    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            a, b = items[i], items[j]
            if a["kind"] != b["kind"]:
                continue
            if (
                a["lon_min"] < b["lon_max"] and b["lon_min"] < a["lon_max"]
                and a["lat_min"] < b["lat_max"] and b["lat_min"] < a["lat_max"]
            ):
                pairs.append(tuple(sorted((a["name"], b["name"]))))
    return pairs


@dg.asset(
    name="points_of_interest",
    group_name="poi",
    key_prefix=["poi", "bronze"],
    io_manager_key="poi_deltalake_io_manager",
    description="Named points (circles) and axis-aligned rectangles from a hand-curated GeoJSON file in S3, for matching against location records.",
)
def points_of_interest(context: dg.AssetExecutionContext, s3: S3ClientResource) -> pl.DataFrame:
    buffer = io.BytesIO()
    s3.get_client().download_fileobj(_POI_BUCKET, _POI_KEY, buffer)
    records, dropped = parse_poi_geojson(buffer.getvalue())

    context.log.info(f"Parsed {len(records)} POI(s), dropped {len(dropped)}")
    if dropped:
        context.log.warning(f"Dropped unparseable features: {dropped}")

    context.add_output_metadata({"pois": len(records), "dropped": len(dropped)})

    return _to_dataframe(records)


@dg.asset_check(
    asset=dg.AssetKey(["poi", "bronze", "points_of_interest"]),
    blocking=True,
    name="poi_names_unique",
    description="Every POI name is unique — name is the only stable identity we have from the GeoJSON source.",
)
def poi_names_unique(points_of_interest: pl.DataFrame) -> AssetCheckResult:
    if points_of_interest.is_empty():
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    counts = points_of_interest["name"].value_counts()
    dupes = counts.filter(pl.col("count") > 1)["name"].to_list()
    return AssetCheckResult(passed=len(dupes) == 0, metadata={"duplicate_names": str(dupes)})


@dg.asset_check(
    asset=dg.AssetKey(["poi", "bronze", "points_of_interest"]),
    blocking=True,
    name="poi_kind_valid",
    description="Every POI has a kind, and it's one of the allowed tiers (point-of-interest, area, region, territory).",
)
def poi_kind_valid(points_of_interest: pl.DataFrame) -> AssetCheckResult:
    if points_of_interest.is_empty():
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    invalid = (
        points_of_interest
        .filter(pl.col("kind").is_null() | ~pl.col("kind").is_in(_ALLOWED_KINDS))
        ["name"].to_list()
    )
    return AssetCheckResult(passed=len(invalid) == 0, metadata={"invalid_kind_names": str(invalid)})


@dg.asset_check(
    asset=dg.AssetKey(["poi", "bronze", "points_of_interest"]),
    blocking=True,
    name="poi_circle_radius_positive",
    description="Every circle (Point) POI has a non-null, positive radius_m — required to test whether a location falls inside it.",
)
def poi_circle_radius_positive(points_of_interest: pl.DataFrame) -> AssetCheckResult:
    circles = points_of_interest.filter(pl.col("geometry_type") == "circle")
    if circles.is_empty():
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    bad = circles.filter(
        pl.col("radius_m").is_null() | (pl.col("radius_m") <= 0)
    )["name"].to_list()
    return AssetCheckResult(passed=len(bad) == 0, metadata={"invalid_radius_names": str(bad)})


@dg.asset_check(
    asset=dg.AssetKey(["poi", "bronze", "points_of_interest"]),
    blocking=True,
    name="poi_rectangle_bounds_valid",
    description="Every rectangle POI has non-null, correctly ordered lon/lat bounds (min < max on both axes).",
)
def poi_rectangle_bounds_valid(points_of_interest: pl.DataFrame) -> AssetCheckResult:
    rects = points_of_interest.filter(pl.col("geometry_type") == "rectangle")
    if rects.is_empty():
        return AssetCheckResult(passed=True, metadata={"row_count": 0})
    bad = rects.filter(
        pl.col("lon_min").is_null() | pl.col("lon_max").is_null()
        | pl.col("lat_min").is_null() | pl.col("lat_max").is_null()
        | (pl.col("lon_min") >= pl.col("lon_max"))
        | (pl.col("lat_min") >= pl.col("lat_max"))
    )["name"].to_list()
    return AssetCheckResult(passed=len(bad) == 0, metadata={"invalid_bounds_names": str(bad)})


@dg.asset_check(
    asset=dg.AssetKey(["poi", "bronze", "points_of_interest"]),
    blocking=True,
    name="poi_no_same_kind_overlap",
    description="No two rectangles of the same kind overlap (e.g. two regions shouldn't double-cover the same ground) — different kinds may nest freely.",
)
def poi_no_same_kind_overlap(points_of_interest: pl.DataFrame) -> AssetCheckResult:
    rects = points_of_interest.filter(pl.col("geometry_type") == "rectangle")
    overlaps = find_same_kind_overlaps(rects)
    return AssetCheckResult(passed=len(overlaps) == 0, metadata={"overlapping_pairs": str(overlaps)})


defs = Definitions(
    assets=[points_of_interest],
    asset_checks=[
        poi_names_unique,
        poi_kind_valid,
        poi_circle_radius_positive,
        poi_rectangle_bounds_valid,
        poi_no_same_kind_overlap,
    ],
)
