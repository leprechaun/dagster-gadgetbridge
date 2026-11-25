import polars as pl

from gadgetbridge_pipeline.defs.owntracks.fix_poi import (
    build_derived_bridge_fix_poi,
    build_derived_fix_poi_tiers,
    find_fixes_with_bad_weight,
)
from gadgetbridge_pipeline.defs.poi.dimensions import build_dim_poi, find_reserved_poi_names

_SCHEMA = {
    "name": pl.String, "kind": pl.String, "geometry_type": pl.String,
    "lat": pl.Float64, "lon": pl.Float64, "radius_m": pl.Float64,
    "lon_min": pl.Float64, "lon_max": pl.Float64, "lat_min": pl.Float64, "lat_max": pl.Float64,
}


def _circle(name, lat=0.0, lon=0.0, radius=100.0, kind="point-of-interest"):
    return (name, kind, "circle", lat, lon, radius, None, None, None, None)


def _rect(name, lon_min, lon_max, lat_min, lat_max, kind="area"):
    return (name, kind, "rectangle", None, None, None, lon_min, lon_max, lat_min, lat_max)


def _poi(*rows):
    return pl.DataFrame(list(rows), schema=_SCHEMA, orient="row")


def test_dim_poi_has_every_poi_plus_the_reserved_none_and_unknown_members():
    poi = _poi(_circle("example-cafe"), _rect("example-area", -1, 1, -1, 1))

    dim = build_dim_poi(poi).sort("poi_key")

    assert dim.select("poi_key", "kind", "geometry_type").rows() == [
        ("(none)", None, None), ("(unknown)", None, None),
        ("example-area", "area", "rectangle"), ("example-cafe", "point-of-interest", "circle"),
    ]


def test_poi_names_colliding_with_reserved_members_are_reported():
    poi = _poi(_circle("example-cafe"), _circle("(none)"), _circle("(unknown)"))

    assert find_reserved_poi_names(poi) == ["(none)", "(unknown)"]


def _fixes(*rows):
    """rows: (fix_key, lat, lon)"""
    return pl.DataFrame(
        list(rows), schema={"fix_key": pl.Int64, "lat": pl.Float64, "lon": pl.Float64},
        orient="row",
    )


def test_a_fix_gets_its_area_region_and_territory_and_none_for_each_tier_it_is_outside():
    poi = _poi(
        _rect("example-area", -1, 1, -1, 1, kind="area"),
        _rect("example-region", -5, 5, -5, 5, kind="region"),
    )
    fixes = _fixes((1, 0.0, 0.0), (2, 3.0, 3.0), (3, 50.0, 50.0))

    out = build_derived_fix_poi_tiers(fixes, poi).sort("fix_key")

    assert out.columns == ["fix_key", "area_key", "region_key", "territory_key"]
    assert out.rows() == [
        (1, "example-area", "example-region", "(none)"),
        (2, "(none)", "example-region", "(none)"),
        (3, "(none)", "(none)", "(none)"),
    ]


def test_overlapping_circles_split_a_fixs_weight_and_a_fix_in_none_maps_to_none_at_weight_one():
    poi = _poi(
        _circle("example-cafe-a", lat=0.0, lon=0.0, radius=500.0),
        _circle("example-cafe-b", lat=0.0, lon=0.0, radius=500.0),   # same spot: overlaps a
        _circle("example-gym", lat=10.0, lon=10.0, radius=100.0),
    )
    fixes = _fixes((1, 0.0, 0.0), (2, 10.0, 10.0), (3, 50.0, 50.0))

    out = build_derived_bridge_fix_poi(fixes, poi).sort("fix_key", "poi_key")

    assert out.columns == ["fix_key", "poi_key", "weight"]
    assert out.rows() == [
        (1, "example-cafe-a", 0.5), (1, "example-cafe-b", 0.5),
        (2, "example-gym", 1.0),
        (3, "(none)", 1.0),
    ]


def test_a_fix_with_no_bridge_rows_or_weights_not_summing_to_one_fails_but_thirds_pass():
    fixes = _fixes((1, 0.0, 0.0), (2, 0.0, 0.0), (3, 0.0, 0.0), (4, 0.0, 0.0))
    bridge = pl.DataFrame({
        "fix_key": [1, 1, 1, 2, 3, 3],
        "poi_key": ["a", "b", "c", "a", "a", "b"],
        "weight": [1 / 3, 1 / 3, 1 / 3, 0.5, 0.5, 0.5],
    })
    # fix 1: thirds, passes. fix 2: sums to 0.5. fix 3: sums to 1.0 via 0.5+0.5, passes.
    # fix 4: no bridge rows at all.

    assert find_fixes_with_bad_weight(fixes, bridge) == [2, 4]
