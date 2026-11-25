"""
conformed_s3_sensor
-------------------
Triggers rematerialization of the bronze persons/devices assets when either
CSV's ETag changes on S3, via make_object_watch_sensor (see s3_watch.py).
The dimensions follow via their eager automation conditions.
"""

from __future__ import annotations

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.conformed.bronze import _BUCKET, DEVICES_KEY, PERSONS_KEY
from gadgetbridge_pipeline.defs.s3_watch import make_object_watch_sensor

conformed_s3_sensor = make_object_watch_sensor(
    name="conformed_s3_sensor",
    description="Triggers persons/devices materialization when either CSV changes on S3.",
    keys={"persons": (_BUCKET, PERSONS_KEY), "devices": (_BUCKET, DEVICES_KEY)},
    asset_selection=AssetSelection.assets(
        AssetKey(["conformed", "bronze", "persons"]),
        AssetKey(["conformed", "bronze", "devices"]),
    ),
)

defs = Definitions(sensors=[conformed_s3_sensor])
