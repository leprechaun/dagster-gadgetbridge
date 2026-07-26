"""
owntracks_s3_sensor
--------------------
Watches s3://deltalake/owntracks/raw/rec/ for new or changed .rec files, via
make_prefix_watch_sensor (see s3_watch.py). Files are grouped by month
(derived from the filename); when any file within a month changes or
appears, that month's partition is triggered — one run per affected month,
processing all user/device files for that month.
"""

from __future__ import annotations

import os

from dagster import AssetKey, AssetSelection, Definitions

from gadgetbridge_pipeline.defs.sensors.s3_watch import make_prefix_watch_sensor

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PREFIX = "owntracks/raw/rec/"


def _month_from_key(key: str) -> str:
    """'owntracks/raw/rec/alice/phone/2026-07.rec' -> '2026-07'"""
    return key.split("/")[-1].removesuffix(".rec")


def _partition_key(year_month: str) -> str:
    """'2026-07' -> '2026-07-01' (Dagster monthly partition key format)"""
    return f"{year_month}-01"


owntracks_s3_sensor = make_prefix_watch_sensor(
    name="owntracks_s3_sensor",
    description="Triggers monthly partition runs when .rec files in S3 change.",
    bucket=_BUCKET,
    prefix=_PREFIX,
    suffix=".rec",
    asset_selection=AssetSelection.assets(AssetKey(["owntracks", "bronze", "location_records"])),
    group_key_fn=_month_from_key,
    partition_key_fn=_partition_key,
)

defs = Definitions(sensors=[owntracks_s3_sensor])
