"""
waypoints_s3_sensor
--------------------
Watches s3://deltalake/owntracks/raw/waypoints/ for new, changed, or removed
OwnTracks waypoint JSON files (one file per waypoint).

Cursor: JSON dict of {s3_key: etag} for all known waypoint files. Unlike
owntracks_s3_sensor there's no monthly partitioning to group by — waypoints
is a single unpartitioned asset, so any change to the file set triggers one
run.
"""

from __future__ import annotations

import hashlib
import json
import os

from botocore.exceptions import ClientError
from dagster import (
    AssetKey,
    AssetSelection,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from gadgetbridge_pipeline.defs.resources import S3ClientResource

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PREFIX = "owntracks/raw/waypoints/"


def plan_run_request(current: dict[str, str], previous: dict[str, str]) -> dict | None:
    """Pure decision logic: return a run request dict if the waypoint file
    set or any ETag changed since last tick, else None.
    """
    if current == previous:
        return None
    run_key = hashlib.md5(json.dumps(sorted(current.items())).encode()).hexdigest()
    return {"run_key": run_key}


@sensor(
    name="waypoints_s3_sensor",
    description="Triggers waypoints materialization when OwnTracks waypoint JSON files in S3 change.",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.assets(
        AssetKey(["owntracks", "bronze", "waypoints"])
    ),
)
def waypoints_s3_sensor(context: SensorEvaluationContext, s3: S3ClientResource):
    client = s3.get_client()

    try:
        paginator = client.get_paginator("list_objects_v2")
        current: dict[str, str] = {}
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=_PREFIX):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                if key.endswith(".json"):
                    current[key] = obj["ETag"]
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        yield SkipReason(f"S3 list_objects_v2 failed ({code}) — will retry next tick")
        return

    previous: dict[str, str] = {}
    if context.cursor:
        try:
            previous = json.loads(context.cursor)
        except (json.JSONDecodeError, ValueError):
            pass

    request = plan_run_request(current, previous)

    if request is None:
        yield SkipReason(f"No changes detected across {len(current)} waypoint file(s).")
        return

    context.log.info(f"Waypoint file set changed: {len(current)} file(s) tracked")
    context.update_cursor(json.dumps(current))

    yield RunRequest(run_key=request["run_key"], tags={"triggered_by": "waypoints_s3_sensor"})


defs = Definitions(sensors=[waypoints_s3_sensor])
