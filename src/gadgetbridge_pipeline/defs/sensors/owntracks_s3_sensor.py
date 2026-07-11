"""
owntracks_s3_sensor
--------------------
Watches s3://deltalake/owntracks/raw/rec/ for new or changed .rec files.
Uses a JSON dict cursor of {s3_key: etag} to detect any change.
A run is only requested when at least one file appears or its ETag changes.
"""

from __future__ import annotations

import hashlib
import json
import os

import boto3
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

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PREFIX = "owntracks/raw/rec/"


def _s3_client():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"))


@sensor(
    name="owntracks_s3_sensor",
    description="Triggers owntracks/bronze/location_records when .rec files in S3 change.",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.assets(
        AssetKey(["owntracks", "bronze", "location_records"])
    ),
)
def owntracks_s3_sensor(context: SensorEvaluationContext):
    client = _s3_client()

    try:
        paginator = client.get_paginator("list_objects_v2")
        current: dict[str, str] = {}
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=_PREFIX):
            for obj in page.get("Contents", []):
                key: str = obj["Key"]
                if key.endswith(".rec"):
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

    new_keys = set(current) - set(previous)
    changed_keys = {k for k in current if k in previous and current[k] != previous[k]}

    if not new_keys and not changed_keys:
        yield SkipReason(
            f"No changes detected across {len(current)} OwnTracks .rec file(s)."
        )
        return

    context.log.info(
        f"OwnTracks S3 change detected — new={len(new_keys)}, changed={len(changed_keys)}"
    )
    context.update_cursor(json.dumps(current))

    run_key = hashlib.md5(
        json.dumps(sorted(current.items())).encode()
    ).hexdigest()

    yield RunRequest(
        run_key=run_key,
        tags={
            "triggered_by": "owntracks_s3_sensor",
            "new_files": str(len(new_keys)),
            "changed_files": str(len(changed_keys)),
        },
    )


defs = Definitions(sensors=[owntracks_s3_sensor])
