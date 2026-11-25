"""
medicine_checkin_s3_sensor
--------------------------
Triggers rematerialization of the bronze medicine_checkins asset when the
append-only medicine_checkins.csv changes on S3, via make_object_watch_sensor
(see s3_watch.py). fact_medicine_checkin follows via its eager condition.
"""

from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.s3_watch import make_object_watch_sensor

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")

medicine_checkin_s3_sensor = make_object_watch_sensor(
    name="medicine_checkin_s3_sensor",
    description="Triggers medicine_checkins materialization when medicine_checkins.csv changes.",
    keys={"checkins": (_BUCKET, "medicine/raw/medicine_checkins.csv")},
    asset_selection=AssetSelection.assets(AssetKey(["medicine", "bronze", "medicine_checkins"])),
)

defs = Definitions(sensors=[medicine_checkin_s3_sensor])
