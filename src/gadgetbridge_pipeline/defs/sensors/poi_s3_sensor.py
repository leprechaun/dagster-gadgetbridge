"""
poi_s3_sensor
-------------
Polls S3 for changes to the hand-curated poi.geojson file (named points,
circles, and axis-aligned rectangles). Triggers rematerialization of the
bronze points_of_interest asset when its ETag changes.

Cursor: JSON dict {"etag": <etag>} for the single watched object.
"""

from __future__ import annotations

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

_POI_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_POI_KEY = "poi/raw/poi.geojson"


def parse_cursor(cursor: str | None) -> dict:
    if not cursor:
        return {}
    try:
        return json.loads(cursor)
    except (json.JSONDecodeError, ValueError):
        return {}


def evaluate_change(current_etag: str, cursor: dict) -> dict:
    """Pure decision logic: compare the current S3 ETag against the cursor's
    last-seen ETag and decide whether the sensor should skip or run.
    """
    if cursor.get("etag") == current_etag:
        return {
            "action": "skip",
            "reason": f"ETag unchanged ({current_etag}) — poi.geojson has not been updated.",
        }
    return {
        "action": "run",
        "run_key": current_etag,
        "new_cursor": {"etag": current_etag},
        "tags": {"triggered_by": "poi_s3_sensor"},
    }


@sensor(
    name="poi_s3_sensor",
    description="Triggers points_of_interest materialization when poi.geojson changes on S3.",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.assets(
        AssetKey(["poi", "bronze", "points_of_interest"])
    ),
)
def poi_s3_sensor(context: SensorEvaluationContext, s3: S3ClientResource):
    client = s3.get_client()

    try:
        head = client.head_object(Bucket=_POI_BUCKET, Key=_POI_KEY)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        yield SkipReason(f"S3 HEAD failed for {_POI_KEY} ({code}) — will retry next tick")
        return

    current_etag: str = head["ETag"]
    cursor = parse_cursor(context.cursor)
    decision = evaluate_change(current_etag, cursor)

    if decision["action"] == "skip":
        yield SkipReason(decision["reason"])
        return

    context.log.info(f"poi.geojson ETag changed: {cursor.get('etag')!r} → {current_etag!r}")
    context.update_cursor(json.dumps(decision["new_cursor"]))

    yield RunRequest(run_key=decision["run_key"], tags=decision["tags"])


defs = Definitions(sensors=[poi_s3_sensor])
