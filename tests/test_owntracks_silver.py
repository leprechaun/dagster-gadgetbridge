import polars as pl

from gadgetbridge_pipeline.defs.assets.owntracks.silver import join_location_poi

# Eiffel Tower, for a real-world lat/lon pair with a known nearby distance.
_EIFFEL_LAT, _EIFFEL_LON = 48.8584, 2.2945
# ~1km east of the tower.
_NEARBY_LAT, _NEARBY_LON = 48.8584, 2.3080


def _locations(**cols) -> pl.DataFrame:
    return pl.DataFrame(cols)


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
    assert out["point_of_interest"].to_list() == [["Eiffel Tower"]]


def test_excludes_poi_circle_outside_radius():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_circle("Far away", _NEARBY_LAT, _NEARBY_LON, 50.0))
    out = join_location_poi(loc, poi)
    assert out["point_of_interest"].to_list() == [[]]


def test_matches_poi_rectangle_containing_point():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0))
    out = join_location_poi(loc, poi)
    assert out["region"].to_list() == ["Paris"]


def test_excludes_poi_rectangle_not_containing_point():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Elsewhere", lon_min=10.0, lon_max=11.0, lat_min=10.0, lat_max=11.0))
    out = join_location_poi(loc, poi)
    assert out["region"].to_list() == [None]


def test_rectangle_match_is_inclusive_of_boundary():
    # The point sits exactly on lon_max/lat_max — should still count as inside.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(_poi_rectangle("Edge", lon_min=2.0, lon_max=_EIFFEL_LON, lat_min=48.0, lat_max=_EIFFEL_LAT))
    out = join_location_poi(loc, poi)
    assert out["region"].to_list() == ["Edge"]


def test_matches_nested_circle_and_rectangle_of_different_kinds():
    # A point-of-interest circle nested inside a region rectangle — both
    # should show up for the same location record, each in its own column.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(
        _poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0, kind="point-of-interest"),
        _poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0, kind="region"),
    )
    out = join_location_poi(loc, poi)
    assert out["point_of_interest"].to_list() == [["Eiffel Tower"]]
    assert out["region"].to_list() == ["Paris"]


def test_matches_area_and_territory_kinds():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(
        _poi_rectangle("Le Gros Caillou", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0, kind="area"),
        _poi_rectangle("France", lon_min=-5.0, lon_max=10.0, lat_min=40.0, lat_max=51.0, kind="territory"),
    )
    out = join_location_poi(loc, poi)
    assert out["area"].to_list() == ["Le Gros Caillou"]
    assert out["territory"].to_list() == ["France"]
    assert out["region"].to_list() == [None]
    assert out["point_of_interest"].to_list() == [[]]


def test_poi_empty_columns_when_no_poi():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    out = join_location_poi(loc, _poi())
    assert out["point_of_interest"].to_list() == [[]]
    assert out["area"].to_list() == [None]
    assert out["region"].to_list() == [None]
    assert out["territory"].to_list() == [None]


def test_poi_empty_location_records_returns_empty_frame_with_columns():
    loc = _locations(id=[], lat=[], lon=[]).cast({"lat": pl.Float64, "lon": pl.Float64})
    poi = _poi(_poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0))
    out = join_location_poi(loc, poi)
    assert out.is_empty()
    assert out.schema["point_of_interest"] == pl.List(pl.String)
    assert out.schema["area"] == pl.String
    assert out.schema["region"] == pl.String
    assert out.schema["territory"] == pl.String


def test_excludes_poi_match_for_location_with_null_lat_lon():
    loc = _locations(id=["a"], lat=[None], lon=[None]).cast({"lat": pl.Float64, "lon": pl.Float64})
    poi = _poi(
        _poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0),
        _poi_rectangle("Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0),
    )
    out = join_location_poi(loc, poi)
    assert out["point_of_interest"].to_list() == [[]]
    assert out["region"].to_list() == [None]


def test_poi_preserves_row_order_and_unmatched_rows():
    loc = _locations(
        id=["a", "b", "c"],
        lat=[_EIFFEL_LAT, 10.0, _EIFFEL_LAT],
        lon=[_EIFFEL_LON, 10.0, _EIFFEL_LON],
    )
    poi = _poi(_poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0))
    out = join_location_poi(loc, poi)
    assert out["id"].to_list() == ["a", "b", "c"]
    assert out["point_of_interest"].to_list() == [["Eiffel Tower"], [], ["Eiffel Tower"]]
