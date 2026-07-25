import polars as pl

from gadgetbridge_pipeline.defs.assets.owntracks_silver import (
    join_location_poi,
    join_location_waypoints,
)

# Eiffel Tower, for a real-world lat/lon pair with a known nearby distance.
_EIFFEL_LAT, _EIFFEL_LON = 48.8584, 2.2945
# ~1km east of the tower.
_NEARBY_LAT, _NEARBY_LON = 48.8584, 2.3080


def _locations(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)


def _waypoints(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)


def test_matches_waypoint_within_radius():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(name=["Eiffel Tower"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[50.0])
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [["Eiffel Tower"]]


def test_excludes_waypoint_outside_radius():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(name=["Far away"], lat=[_NEARBY_LAT], lon=[_NEARBY_LON], radius_m=[50.0])
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [[]]


def test_includes_waypoint_at_edge_of_larger_radius():
    # ~1km away, but the waypoint's radius comfortably covers it.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(name=["Neighborhood"], lat=[_NEARBY_LAT], lon=[_NEARBY_LON], radius_m=[2000.0])
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [["Neighborhood"]]


def test_lists_multiple_overlapping_waypoints_sorted():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(
        name=["Work", "Home"],
        lat=[_EIFFEL_LAT, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, _EIFFEL_LON],
        radius_m=[100.0, 100.0],
    )
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [["Home", "Work"]]


def test_dedupes_same_waypoint_synced_from_multiple_devices():
    # Same waypoint name/location appearing twice (e.g. synced from phone and
    # tablet) should collapse to a single entry, not appear twice in the list.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(
        name=["Home", "Home"],
        lat=[_EIFFEL_LAT, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, _EIFFEL_LON],
        radius_m=[100.0, 100.0],
    )
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [["Home"]]


def test_empty_list_when_no_waypoints():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(name=[], lat=[], lon=[], radius_m=[])
    out = join_location_waypoints(loc, wp.cast({"lat": pl.Float64, "lon": pl.Float64, "radius_m": pl.Float64}))
    assert out["waypoint_names"].to_list() == [[]]


def test_empty_location_records_returns_empty_frame_with_column():
    loc = _locations(id=[], lat=[], lon=[]).cast({"lat": pl.Float64, "lon": pl.Float64})
    wp = _waypoints(name=["Home"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[100.0])
    out = join_location_waypoints(loc, wp)
    assert out.is_empty()
    assert out.schema["waypoint_names"] == pl.List(pl.String)


def test_excludes_waypoint_with_null_radius():
    # rad is an optional OwnTracks field — a null radius must never match.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(
        name=["No radius"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[None]
    ).cast({"radius_m": pl.Float64})
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [[]]


def test_excludes_waypoint_with_null_name():
    # desc is an optional OwnTracks field — an unnamed waypoint can't be
    # represented in a waypoint_names list, so it's dropped entirely rather
    # than surfacing a None in the list.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(
        name=[None], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[100.0]
    ).cast({"name": pl.String})
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [[]]


def test_unnamed_waypoint_does_not_hide_named_match():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    wp = _waypoints(
        name=[None, "Home"],
        lat=[_EIFFEL_LAT, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, _EIFFEL_LON],
        radius_m=[100.0, 100.0],
    ).cast({"name": pl.String})
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [["Home"]]


def test_excludes_location_with_null_lat_lon():
    loc = _locations(id=["a"], lat=[None], lon=[None]).cast({"lat": pl.Float64, "lon": pl.Float64})
    wp = _waypoints(name=["Home"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[100.0])
    out = join_location_waypoints(loc, wp)
    assert out["waypoint_names"].to_list() == [[]]


def test_preserves_row_order_and_unmatched_rows():
    loc = _locations(
        id=["a", "b", "c"],
        lat=[_EIFFEL_LAT, 10.0, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, 10.0, _EIFFEL_LON],
    )
    wp = _waypoints(name=["Home"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON], radius_m=[100.0])
    out = join_location_waypoints(loc, wp)
    assert out["id"].to_list() == ["a", "b", "c"]
    assert out["waypoint_names"].to_list() == [["Home"], [], ["Home"]]


# join_location_poi — circles and axis-aligned rectangles

_POI_SCHEMA = {
    "name": pl.String,
    "kind": pl.String,
    "geometry_type": pl.String,
    "lat": pl.Float64,
    "lon": pl.Float64,
    "radius_m": pl.Float64,
    "lon_min": pl.Float64,
    "lon_max": pl.Float64,
    "lat_min": pl.Float64,
    "lat_max": pl.Float64,
}


def _poi_circle(name: str, lat: float, lon: float, radius_m, kind: str = "point-of-interest") -> dict:
    return {
        "name": name, "kind": kind, "geometry_type": "circle",
        "lat": lat, "lon": lon, "radius_m": radius_m,
        "lon_min": None, "lon_max": None, "lat_min": None, "lat_max": None,
    }


def _poi_rectangle(name: str, lon_min, lon_max, lat_min, lat_max, kind: str = "region") -> dict:
    return {
        "name": name, "kind": kind, "geometry_type": "rectangle",
        "lat": None, "lon": None, "radius_m": None,
        "lon_min": lon_min, "lon_max": lon_max, "lat_min": lat_min, "lat_max": lat_max,
    }


def _poi(*rows: dict) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=_POI_SCHEMA)
    return pl.DataFrame(list(rows), schema=_POI_SCHEMA)


def test_matches_poi_circle_within_radius():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 50.0))
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [["Eiffel Tower"]]


def test_excludes_poi_circle_outside_radius():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_circle("Far away", _NEARBY_LAT, _NEARBY_LON, 50.0))
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [[]]


def test_matches_poi_rectangle_containing_point():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0))
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [["Paris"]]


def test_excludes_poi_rectangle_not_containing_point():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Elsewhere", lon_min=10.0, lon_max=11.0, lat_min=10.0, lat_max=11.0))
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [[]]


def test_rectangle_match_is_inclusive_of_boundary():
    # The point sits exactly on lon_max/lat_max — should still count as inside.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Edge", lon_min=2.0, lon_max=_EIFFEL_LON, lat_min=48.0, lat_max=_EIFFEL_LAT))
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [["Edge"]]


def test_matches_nested_circle_and_rectangle_of_different_kinds():
    # A point-of-interest circle nested inside a region rectangle — both
    # should show up for the same location record.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(
        _poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0, kind="point-of-interest"),
        _poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0, kind="region"),
    )
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [["Eiffel Tower", "Paris"]]


def test_poi_empty_list_when_no_poi():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    out = join_location_poi(loc, _poi())
    assert out["poi_names"].to_list() == [[]]


def test_poi_empty_location_records_returns_empty_frame_with_column():
    loc = _locations(id=[], lat=[], lon=[]).cast({"lat": pl.Float64, "lon": pl.Float64})
    poi = _poi(_poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0))
    out = join_location_poi(loc, poi)
    assert out.is_empty()
    assert out.schema["poi_names"] == pl.List(pl.String)


def test_excludes_poi_match_for_location_with_null_lat_lon():
    loc = _locations(id=["a"], lat=[None], lon=[None]).cast({"lat": pl.Float64, "lon": pl.Float64})
    poi = _poi(
        _poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0),
        _poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0),
    )
    out = join_location_poi(loc, poi)
    assert out["poi_names"].to_list() == [[]]


def test_poi_preserves_row_order_and_unmatched_rows():
    loc = _locations(
        id=["a", "b", "c"],
        lat=[_EIFFEL_LAT, 10.0, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, 10.0, _EIFFEL_LON],
    )
    poi = _poi(_poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0))
    out = join_location_poi(loc, poi)
    assert out["id"].to_list() == ["a", "b", "c"]
    assert out["poi_names"].to_list() == [["Eiffel Tower"], [], ["Eiffel Tower"]]
