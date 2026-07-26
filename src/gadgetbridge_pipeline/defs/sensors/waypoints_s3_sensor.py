"""
waypoints_s3_sensor
--------------------
Watches s3://deltalake/owntracks/raw/waypoints/ for new, changed, or removed
OwnTracks waypoint JSON files (one file per waypoint), via
make_prefix_watch_sensor (see s3_watch.py). Unlike owntracks_s3_sensor
there's no grouping — waypoints is a single unpartitioned asset, so any
change to the file set triggers one run.
"""

from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.sensors.s3_watch import make_prefix_watch_sensor

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PREFIX = "owntracks/raw/waypoints/"

waypoints_s3_sensor = make_prefix_watch_sensor(
    name="waypoints_s3_sensor",
    description="Triggers waypoints materialization when OwnTracks waypoint JSON files in S3 change.",
    bucket=_BUCKET,
    prefix=_PREFIX,
    suffix=".json",
    asset_selection=AssetSelection.assets(AssetKey(["owntracks", "bronze", "waypoints"])),
)

defs = Definitions(sensors=[waypoints_s3_sensor])
