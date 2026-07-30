import json

import polars as pl

from gadgetbridge_pipeline.defs.poi.bronze import (
    find_same_kind_overlaps,
    parse_poi_feature,
    parse_poi_geojson,
    poi_circle_radius_positive,
    poi_kind_valid,
    poi_names_unique,
    poi_no_same_kind_overlap,
    poi_rectangle_bounds_valid,
)

# parse_poi_feature — single GeoJSON feature -> POI row or None

def _point(name="home", kind="poi", radius_m=None, **extra):
    props = {
        "name": name,
        "kind": kind,
        **({"radius_m": radius_m} if radius_m is not None else {}),
        **extra,
    }
    return {
        "type": "Feature",
        "properties": props,
        "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
    }


def _rectangle(name="bangkok", kind="region", ring=None):
    ring = ring or [[100.4, 13.6], [100.4, 14.0], [100.8, 14.0], [100.8, 13.6], [100.4, 13.6]]
    return {
        "type": "Feature",
        "properties": {"name": name, "kind": kind},
        "geometry": {"type": "Polygon", "coordinates": [ring]},
    }


def test_parse_point_basic():
    record = parse_poi_feature(_point(radius_m=150))
    assert record["name"] == "home"
    assert record["kind"] == "poi"
    assert record["geometry_type"] == "circle"
    assert record["lon"] == 2.3522
    assert record["lat"] == 48.8566
    assert record["radius_m"] == 150
    assert record["lon_min"] is None


def test_parse_point_without_radius_keeps_row_with_null_radius():
    record = parse_poi_feature(_point())
    assert record is not None
    assert record["radius_m"] is None


def test_parse_point_without_kind_keeps_row_with_null_kind():
    feature = _point()
    del feature["properties"]["kind"]
    record = parse_poi_feature(feature)
    assert record is not None
    assert record["kind"] is None


def test_parse_feature_without_name_is_dropped():
    feature = _point()
    del feature["properties"]["name"]
    assert parse_poi_feature(feature) is None


def test_parse_rectangle_basic():
    record = parse_poi_feature(_rectangle())
    assert record["geometry_type"] == "rectangle"
    assert record["lon_min"] == 100.4
    assert record["lon_max"] == 100.8
    assert record["lat_min"] == 13.6
    assert record["lat_max"] == 14.0
    assert record["lat"] is None


def test_parse_rectangle_rejects_non_axis_aligned_trapezoid():
    # 4 distinct longitudes instead of 2 — a slanted quadrilateral, not a box
    ring = [
        [-74.228316, 45.918737],
        [-74.227356, 45.909039],
        [-74.208851, 45.909039],
        [-74.208546, 45.918737],
        [-74.228316, 45.918737],
    ]
    assert parse_poi_feature(_rectangle(ring=ring)) is None


def test_parse_rectangle_rejects_unclosed_ring():
    ring = [[100.4, 13.6], [100.4, 14.0], [100.8, 14.0], [100.8, 13.6]]
    assert parse_poi_feature(_rectangle(ring=ring)) is None


def test_parse_rectangle_rejects_duplicate_corner():
    # only 3 distinct points — a degenerate triangle-ish ring, not a box
    ring = [[100.4, 13.6], [100.4, 13.6], [100.8, 14.0], [100.8, 13.6], [100.4, 13.6]]
    assert parse_poi_feature(_rectangle(ring=ring)) is None


def test_parse_polygon_with_holes_is_rejected():
    outer = [[100.4, 13.6], [100.4, 14.0], [100.8, 14.0], [100.8, 13.6], [100.4, 13.6]]
    hole = [[100.5, 13.7], [100.5, 13.8], [100.6, 13.8], [100.6, 13.7], [100.5, 13.7]]
    feature = {
        "type": "Feature",
        "properties": {"name": "with-hole", "kind": "region"},
        "geometry": {"type": "Polygon", "coordinates": [outer, hole]},
    }
    assert parse_poi_feature(feature) is None


def test_parse_feature_rejects_unsupported_geometry_type():
    feature = {
        "type": "Feature",
        "properties": {"name": "x", "kind": "poi"},
        "geometry": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]},
    }
    assert parse_poi_feature(feature) is None


# parse_poi_geojson — full FeatureCollection -> (records, dropped)

def test_parse_geojson_splits_valid_and_dropped():
    unnamed = _point()
    del unnamed["properties"]["name"]
    data = json.dumps({
        "type": "FeatureCollection",
        "features": [_point(name="home"), _rectangle(name="bangkok"), unnamed],
    }).encode()
    records, dropped = parse_poi_geojson(data)
    assert {r["name"] for r in records} == {"home", "bangkok"}
    assert dropped == ["<unnamed>"]


def test_parse_geojson_empty_feature_collection():
    data = json.dumps({"type": "FeatureCollection", "features": []}).encode()
    assert parse_poi_geojson(data) == ([], [])


# find_same_kind_overlaps — pure geometry logic

def _rect_row(name, kind, lon_min, lon_max, lat_min, lat_max):
    return {
        "name": name,
        "kind": kind,
        "lon_min": lon_min,
        "lon_max": lon_max,
        "lat_min": lat_min,
        "lat_max": lat_max,
    }


def test_find_overlaps_same_kind_overlapping():
    df = pl.DataFrame([
        _rect_row("a", "region", 0.0, 10.0, 0.0, 10.0),
        _rect_row("b", "region", 5.0, 15.0, 5.0, 15.0),
    ])
    assert find_same_kind_overlaps(df) == [("a", "b")]


def test_find_overlaps_same_kind_non_overlapping():
    df = pl.DataFrame([
        _rect_row("a", "region", 0.0, 10.0, 0.0, 10.0),
        _rect_row("b", "region", 20.0, 30.0, 20.0, 30.0),
    ])
    assert find_same_kind_overlaps(df) == []


def test_find_overlaps_different_kind_nesting_is_allowed():
    # an area nested inside a region — expected, not flagged
    df = pl.DataFrame([
        _rect_row("bangkok", "region", 0.0, 10.0, 0.0, 10.0),
        _rect_row("benjakitti-park", "area", 4.0, 6.0, 4.0, 6.0),
    ])
    assert find_same_kind_overlaps(df) == []


# poi_names_unique

def test_names_unique_passes():
    df = pl.DataFrame({"name": ["a", "b"]})
    assert poi_names_unique(df).passed


def test_names_unique_fails_on_duplicate():
    df = pl.DataFrame({"name": ["a", "a"]})
    result = poi_names_unique(df)
    assert not result.passed
    assert "a" in result.metadata["duplicate_names"].value


# poi_kind_valid

def test_kind_valid_passes():
    df = pl.DataFrame({"name": ["a", "b"], "kind": ["point-of-interest", "region"]})
    assert poi_kind_valid(df).passed


def test_kind_valid_fails_on_bogus_kind():
    df = pl.DataFrame({"name": ["a"], "kind": ["city"]})
    assert not poi_kind_valid(df).passed


def test_kind_valid_fails_on_null_kind():
    df = pl.DataFrame({"name": ["a"], "kind": pl.Series("kind", [None], dtype=pl.String)})
    assert not poi_kind_valid(df).passed


# poi_circle_radius_positive

def _circle_radius_df(values: list[float | None]) -> pl.DataFrame:
    return pl.DataFrame({
        "name": [f"p{i}" for i in range(len(values))],
        "geometry_type": ["circle"] * len(values),
        "radius_m": pl.Series("radius_m", values, dtype=pl.Float64),
    })


def test_circle_radius_positive_passes():
    assert poi_circle_radius_positive(_circle_radius_df([100.0, 50.0])).passed


def test_circle_radius_positive_fails_on_null():
    assert not poi_circle_radius_positive(_circle_radius_df([100.0, None])).passed


def test_circle_radius_positive_fails_on_non_positive():
    assert not poi_circle_radius_positive(_circle_radius_df([0.0])).passed


def test_circle_radius_positive_ignores_rectangle_rows():
    df = pl.DataFrame({
        "name": ["r1"],
        "geometry_type": ["rectangle"],
        "radius_m": pl.Series("radius_m", [None], dtype=pl.Float64),
    })
    assert poi_circle_radius_positive(df).passed


# poi_rectangle_bounds_valid

def _rect_bounds_df(lon_min, lon_max, lat_min, lat_max) -> pl.DataFrame:
    return pl.DataFrame({
        "name": ["r"],
        "geometry_type": ["rectangle"],
        "lon_min": pl.Series("lon_min", [lon_min], dtype=pl.Float64),
        "lon_max": pl.Series("lon_max", [lon_max], dtype=pl.Float64),
        "lat_min": pl.Series("lat_min", [lat_min], dtype=pl.Float64),
        "lat_max": pl.Series("lat_max", [lat_max], dtype=pl.Float64),
    })


def test_rectangle_bounds_valid_passes():
    assert poi_rectangle_bounds_valid(_rect_bounds_df(0.0, 10.0, 0.0, 10.0)).passed


def test_rectangle_bounds_valid_fails_when_inverted():
    assert not poi_rectangle_bounds_valid(_rect_bounds_df(10.0, 0.0, 0.0, 10.0)).passed


def test_rectangle_bounds_valid_fails_when_null():
    assert not poi_rectangle_bounds_valid(_rect_bounds_df(None, 10.0, 0.0, 10.0)).passed


def test_rectangle_bounds_valid_ignores_circle_rows():
    df = pl.DataFrame({
        "name": ["c"],
        "geometry_type": ["circle"],
        "lon_min": pl.Series("lon_min", [None], dtype=pl.Float64),
        "lon_max": pl.Series("lon_max", [None], dtype=pl.Float64),
        "lat_min": pl.Series("lat_min", [None], dtype=pl.Float64),
        "lat_max": pl.Series("lat_max", [None], dtype=pl.Float64),
    })
    assert poi_rectangle_bounds_valid(df).passed


# poi_no_same_kind_overlap (asset-check wrapper)

def _rect_row(name, kind, lon_min, lon_max, lat_min, lat_max):
    return {
        "name": name,
        "kind": kind,
        "geometry_type": "rectangle",
        "lon_min": lon_min,
        "lon_max": lon_max,
        "lat_min": lat_min,
        "lat_max": lat_max,
    }


def test_no_same_kind_overlap_passes_when_clean():
    df = pl.DataFrame([
        _rect_row("a", "region", 0.0, 10.0, 0.0, 10.0),
        _rect_row("b", "area", 4.0, 6.0, 4.0, 6.0),
    ])
    assert poi_no_same_kind_overlap(df).passed


def test_no_same_kind_overlap_fails_on_same_kind_overlap():
    df = pl.DataFrame([
        _rect_row("a", "region", 0.0, 10.0, 0.0, 10.0),
        _rect_row("b", "region", 5.0, 15.0, 5.0, 15.0),
    ])
    result = poi_no_same_kind_overlap(df)
    assert not result.passed
    assert "('a', 'b')" in result.metadata["overlapping_pairs"].value
