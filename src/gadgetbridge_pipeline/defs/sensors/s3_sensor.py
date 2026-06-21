"""
s3_sensor
----------
Polls S3 for changes to the SQLite export.  Uses the object's ETag as a
cursor — a run is only requested when the ETag differs from the last run.

This means:
  • No unnecessary downloads between phone uploads.
  • If S3 is unavailable, the sensor skips gracefully.
  • ETag is persisted in Dagster's cursor store (SQLite-backed by default).
"""

from __future__ import annotations

import json

from botocore.exceptions import ClientError
from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    Definitions,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)
from gadgetbridge_pipeline.defs.resources import S3ClientResource


@sensor(
    name="s3_sqlite_sensor",
    description="Triggers a full pipeline run when the S3 SQLite file changes (ETag-based).",
    minimum_interval_seconds=300,   # poll every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.all(),
)
def s3_sensor(context: SensorEvaluationContext, s3: S3ClientResource):
    client = s3.get_client()

    try:
        head = client.head_object(Bucket=s3.bucket, Key=s3.key)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        yield SkipReason(f"S3 HEAD failed ({code}) — will retry next tick")
        return

    current_etag: str = head["ETag"]
    last_modified: str = head["LastModified"].isoformat()

    cursor: dict = {}
    if context.cursor:
        try:
            cursor = json.loads(context.cursor)
        except (json.JSONDecodeError, ValueError):
            cursor = {"etag": context.cursor}

    previous_etag = cursor.get("etag")

    if current_etag == previous_etag:
        yield SkipReason(
            f"ETag unchanged ({current_etag}) — SQLite file has not been updated since last run."
        )
        return

    context.log.info(
        f"ETag changed: {previous_etag!r} → {current_etag!r}  "
        f"(LastModified={last_modified})"
    )

    context.update_cursor(json.dumps({"etag": current_etag, "last_modified": last_modified}))

    yield RunRequest(
        run_key=current_etag,
        tags={
            "s3_etag": current_etag,
            "s3_last_modified": last_modified,
            "triggered_by": "s3_sensor",
        },
    )


defs = Definitions(sensors=[s3_sensor])
