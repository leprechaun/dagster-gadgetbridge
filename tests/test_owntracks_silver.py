from datetime import datetime, timedelta, timezone

import polars as pl

from gadgetbridge_pipeline.defs.owntracks.silver import (
    build_poi_visit_spans,
    join_location_poi,
    poi_visit_spans_start_before_end,
)

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


def _poi_circle(
    name: str, lat: float, lon: float, radius_m, kind: str = "point-of-interest"
) -> dict:
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
    poi = _poi(
        _poi_rectangle("Edge", lon_min=2.0, lon_max=_EIFFEL_LON, lat_min=48.0, lat_max=_EIFFEL_LAT)
    )
    out = join_location_poi(loc, poi)
    assert out["region"].to_list() == ["Edge"]


def test_matches_nested_circle_and_rectangle_of_different_kinds():
    # A point-of-interest circle nested inside a region rectangle — both
    # should show up for the same location record, each in its own column.
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(
        _poi_circle("Eiffel Tower", _EIFFEL_LAT, _EIFFEL_LON, 100.0, kind="point-of-interest"),
        _poi_rectangle(
            "Paris", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0, kind="region"
        ),
    )
    out = join_location_poi(loc, poi)
    assert out["point_of_interest"].to_list() == [["Eiffel Tower"]]
    assert out["region"].to_list() == ["Paris"]


def test_matches_area_and_territory_kinds():
    loc = _locations(id=["a"], lat=[_EIFFEL_LAT], lon=[_EIFFEL_LON])
    poi = _poi(
        _poi_rectangle(
            "Le Gros Caillou", lon_min=2.0, lon_max=2.5, lat_min=48.5, lat_max=49.0, kind="area"
        ),
        _poi_rectangle(
            "France", lon_min=-5.0, lon_max=10.0, lat_min=40.0, lat_max=51.0, kind="territory"
        ),
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


# build_poi_visit_spans

_GAP = timedelta(minutes=30)
_T0 = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)


def _at(minutes: int) -> datetime:
    return _T0 + timedelta(minutes=minutes)


def _pings(rows: list[tuple[int, list[str]]], user: str = "alice", device: str = "phone"):
    return pl.DataFrame({
        "user": [user] * len(rows),
        "device": [device] * len(rows),
        "timestamp": [_at(m) for m, _ in rows],
        "point_of_interest": [names for _, names in rows],
    })


def test_single_continuous_visit():
    pings = _pings([(0, ["gym"]), (10, ["gym"]), (20, ["gym"])])
    out = build_poi_visit_spans(pings, _GAP)
    assert out["point_of_interest"].to_list() == ["gym"]
    assert out["start"].to_list() == [_at(0)]
    assert out["end"].to_list() == [_at(20)]
    assert out["duration_minutes"].to_list() == [20]


def test_ping_outside_poi_splits_visit_in_two():
    pings = _pings([(0, ["gym"]), (10, []), (20, ["gym"])])
    out = build_poi_visit_spans(pings, _GAP)
    assert out["start"].to_list() == [_at(0), _at(20)]
    assert out["end"].to_list() == [_at(0), _at(20)]


def test_gap_past_threshold_splits_visit_even_without_a_leaving_ping():
    # No ping in between at all (not even a non-matching one) — just a long
    # silence, then "gym" again. Past the threshold, that's two visits.
    pings = _pings([(0, ["gym"]), (0 + 90, ["gym"])])
    out = build_poi_visit_spans(pings, _GAP)
    assert out["start"].to_list() == [_at(0), _at(90)]
    assert out["end"].to_list() == [_at(0), _at(90)]


def test_gap_under_threshold_stays_one_visit():
    pings = _pings([(0, ["gym"]), (20, ["gym"])])
    out = build_poi_visit_spans(pings, _GAP)
    assert out["start"].to_list() == [_at(0)]
    assert out["end"].to_list() == [_at(20)]


def test_overlapping_pois_tracked_as_independent_spans():
    # Nested circles: inside both "yard" and "shed" the whole time, then
    # just "yard" once they step out of the shed.
    pings = _pings([(0, ["yard", "shed"]), (10, ["yard", "shed"]), (20, ["yard"])])
    out = build_poi_visit_spans(pings, _GAP).sort("point_of_interest")
    assert out["point_of_interest"].to_list() == ["shed", "yard"]
    assert out["start"].to_list() == [_at(0), _at(0)]
    assert out["end"].to_list() == [_at(10), _at(20)]


def test_different_devices_are_independent():
    alice = _pings([(0, ["home"]), (10, ["home"])], user="alice", device="phone")
    bob = _pings([(0, ["home"]), (10, ["home"])], user="bob", device="phone")
    out = build_poi_visit_spans(pl.concat([alice, bob]), _GAP).sort("user")
    assert out["user"].to_list() == ["alice", "bob"]
    assert out["start"].to_list() == [_at(0), _at(0)]
    assert out["end"].to_list() == [_at(10), _at(10)]


def test_no_matches_anywhere_returns_empty_frame_with_columns():
    pings = _pings([(0, []), (10, [])])
    out = build_poi_visit_spans(pings, _GAP)
    assert out.is_empty()
    assert out.schema["point_of_interest"] == pl.String
    assert out.schema["duration_minutes"] == pl.Int64


def test_empty_input_returns_empty_frame_with_columns():
    pings = _pings([]).cast({"timestamp": pl.Datetime(time_unit="us", time_zone="UTC")})
    out = build_poi_visit_spans(pings, _GAP)
    assert out.is_empty()
    assert out.schema["start"] == pl.Datetime(time_unit="us", time_zone="UTC")


# poi_visit_spans_start_before_end

def test_start_before_end_passes_on_empty():
    assert poi_visit_spans_start_before_end(build_poi_visit_spans(_pings([]), _GAP)).passed


def test_start_before_end_passes_when_valid():
    spans = build_poi_visit_spans(_pings([(0, ["gym"]), (10, ["gym"])]), _GAP)
    assert poi_visit_spans_start_before_end(spans).passed


def test_start_before_end_fails_when_violated():
    spans = pl.DataFrame({
        "user": ["alice"],
        "device": ["phone"],
        "point_of_interest": ["gym"],
        "start": [_at(10)],
        "end": [_at(0)],
        "duration_minutes": [-10],
    })
    result = poi_visit_spans_start_before_end(spans)
    assert not result.passed
    assert result.metadata["invalid_row_count"].value == 1
