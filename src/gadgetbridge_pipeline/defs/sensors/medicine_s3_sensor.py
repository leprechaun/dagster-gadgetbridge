"""
medicine_s3_sensor
------------------
Triggers rematerialization of the bronze prescriptions/medicine_skips
assets when either CSV's ETag changes on S3, via make_object_watch_sensor
(see s3_watch.py). medicine_log and daily_medicine_adherence follow
automatically via their eager automation conditions.
"""

from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.sensors.s3_watch import make_object_watch_sensor

_MEDICINE_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")

medicine_s3_sensor = make_object_watch_sensor(
    name="medicine_s3_sensor",
    description="Triggers medicine_log materialization when prescriptions.csv or medicine_skips.csv changes on S3.",
    keys={
        "prescriptions": (_MEDICINE_BUCKET, "medicine/raw/prescriptions.csv"),
        "skips": (_MEDICINE_BUCKET, "medicine/raw/medicine_skips.csv"),
    },
    asset_selection=AssetSelection.assets(
        AssetKey(["medicine", "bronze", "prescriptions"]),
        AssetKey(["medicine", "bronze", "medicine_skips"]),
    ),
)

defs = Definitions(sensors=[medicine_s3_sensor])
