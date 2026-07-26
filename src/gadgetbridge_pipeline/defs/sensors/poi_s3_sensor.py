"""
poi_s3_sensor
-------------
Triggers points_of_interest materialization when the hand-curated
poi.geojson file changes on S3, via make_object_watch_sensor (see
s3_watch.py).
"""

from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.sensors.s3_watch import make_object_watch_sensor

_POI_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_POI_KEY = "poi/raw/poi.geojson"

poi_s3_sensor = make_object_watch_sensor(
    name="poi_s3_sensor",
    description="Triggers points_of_interest materialization when poi.geojson changes on S3.",
    keys={"poi": (_POI_BUCKET, _POI_KEY)},
    asset_selection=AssetSelection.assets(AssetKey(["poi", "bronze", "points_of_interest"])),
)

defs = Definitions(sensors=[poi_s3_sensor])
