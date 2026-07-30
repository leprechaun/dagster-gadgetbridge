"""
s3_sensor
----------
Triggers a full pipeline run when the S3 SQLite export changes, via
make_object_watch_sensor (see s3_watch.py) — HEADs the object and compares
its ETag against the sensor cursor.
"""

from __future__ import annotations

from dagster import AssetSelection, Definitions

from gadgetbridge_pipeline.defs.resources import GADGETBRIDGE_DB_BUCKET, GADGETBRIDGE_DB_KEY
from gadgetbridge_pipeline.defs.s3_watch import make_object_watch_sensor

s3_sensor = make_object_watch_sensor(
    name="s3_sqlite_sensor",
    description="Triggers a full pipeline run when the S3 SQLite file changes (ETag-based).",
    keys={"sqlite": (GADGETBRIDGE_DB_BUCKET, GADGETBRIDGE_DB_KEY)},
    asset_selection=AssetSelection.groups("gadgetbridge"),
)

defs = Definitions(sensors=[s3_sensor])
