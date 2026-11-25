from __future__ import annotations

import dagster as dg
import polars as pl

NO_POI = "(none)"          # a Fix inside no point-of-interest (a statement, not an absence)
UNKNOWN_POI = "(unknown)"  # a failed lookup; never used for "outside"
RESERVED_POIS = (NO_POI, UNKNOWN_POI)


def find_reserved_poi_names(poi: pl.DataFrame) -> list[str]:
    """Curated POI names that collide with the reserved dimension members."""
    return sorted({n for n in poi["name"].to_list() if n in RESERVED_POIS})


def build_dim_poi(poi: pl.DataFrame) -> pl.DataFrame:
    """Every POI keyed by name, plus the reserved `(none)` and `(unknown)` members."""
    reserved = pl.DataFrame(
        {"poi_key": list(RESERVED_POIS), "kind": [None, None], "geometry_type": [None, None]},
        schema={"poi_key": pl.String, "kind": pl.String, "geometry_type": pl.String},
    )
    curated = poi.select(pl.col("name").alias("poi_key"), "kind", "geometry_type")
    return pl.concat([reserved, curated])


@dg.asset(
    name="dim_poi",
    group_name="poi",
    key_prefix=["poi", "gold"],
    io_manager_key="poi_deltalake_io_manager",
    kinds={"deltalake"},
    description="Every curated POI keyed by name, plus the reserved (none) and (unknown) members.",
    ins={"points_of_interest": dg.AssetIn(key=["poi", "bronze", "points_of_interest"])},
    automation_condition=dg.AutomationCondition.eager(),
)
def dim_poi(points_of_interest: pl.DataFrame) -> pl.DataFrame:
    return build_dim_poi(points_of_interest)


defs = dg.Definitions(assets=[dim_poi])
