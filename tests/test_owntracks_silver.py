import polars as pl

from gadgetbridge_pipeline.defs.assets.owntracks_silver import join_location_waypoints

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
