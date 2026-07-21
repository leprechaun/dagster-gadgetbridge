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


def parse_cursor(cursor: str | None) -> dict:
    if not cursor:
        return {}
    try:
        return json.loads(cursor)
    except (json.JSONDecodeError, ValueError):
        return {"etag": cursor}


def evaluate_change(current_etag: str, last_modified: str, cursor: dict) -> dict:
    """Pure decision logic: compare the current S3 ETag against the cursor's
    last-seen ETag and decide whether the sensor should skip or run.
    """
    previous_etag = cursor.get("etag")

    if current_etag == previous_etag:
        return {
            "action": "skip",
            "reason": (
                f"ETag unchanged ({current_etag}) — SQLite file has not been "
                "updated since last run."
            ),
        }

    return {
        "action": "run",
        "run_key": current_etag,
        "new_cursor": {"etag": current_etag, "last_modified": last_modified},
        "tags": {
            "s3_etag": current_etag,
            "s3_last_modified": last_modified,
            "triggered_by": "s3_sensor",
        },
    }


@sensor(
    name="s3_sqlite_sensor",
    description="Triggers a full pipeline run when the S3 SQLite file changes (ETag-based).",
    minimum_interval_seconds=300,   # poll every 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.groups("gadgetbridge"),
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

    cursor = parse_cursor(context.cursor)
    decision = evaluate_change(current_etag, last_modified, cursor)

    if decision["action"] == "skip":
        yield SkipReason(decision["reason"])
        return

    context.log.info(
        f"ETag changed: {cursor.get('etag')!r} → {current_etag!r}  "
        f"(LastModified={last_modified})"
    )

    context.update_cursor(json.dumps(decision["new_cursor"]))

    yield RunRequest(run_key=decision["run_key"], tags=decision["tags"])


defs = Definitions(sensors=[s3_sensor])
