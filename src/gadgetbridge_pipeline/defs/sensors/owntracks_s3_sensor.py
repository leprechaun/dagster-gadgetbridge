"""
owntracks_s3_sensor
--------------------
Watches s3://deltalake/owntracks/raw/rec/ for new or changed .rec files.

Cursor: JSON dict of {s3_key: etag} for all known .rec files.

Files are grouped by month (derived from the filename). When any file within
a month changes or appears, that month's partition is triggered. This means
one run per affected month, processing all user/device files for that month.

run_key = "{partition_key}::{etag_hash}" so a changed ETag for any file in
a month produces a new run for that partition.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import defaultdict

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


def _month_from_key(key: str) -> str:
    """'owntracks/raw/rec/alice/phone/2026-07.rec' -> '2026-07'"""
    return key.split("/")[-1].removesuffix(".rec")


def _partition_key(year_month: str) -> str:
    """'2026-07' -> '2026-07-01' (Dagster monthly partition key format)"""
    return f"{year_month}-01"


@sensor(
    name="owntracks_s3_sensor",
    description="Triggers monthly partition runs when .rec files in S3 change.",
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

    # Group file ETags by month
    current_by_month: dict[str, dict[str, str]] = defaultdict(dict)
    for key, etag in current.items():
        current_by_month[_month_from_key(key)][key] = etag

    previous_by_month: dict[str, dict[str, str]] = defaultdict(dict)
    for key, etag in previous.items():
        previous_by_month[_month_from_key(key)][key] = etag

    affected_months = [
        month for month, files in current_by_month.items()
        if files != previous_by_month.get(month)
    ]

    if not affected_months:
        yield SkipReason(
            f"No changes detected across {len(current)} OwnTracks .rec file(s)."
        )
        return

    context.log.info(f"Affected months: {sorted(affected_months)}")
    context.update_cursor(json.dumps(current))

    for month in sorted(affected_months):
        pk = _partition_key(month)
        etag_hash = hashlib.md5(
            json.dumps(sorted(current_by_month[month].items())).encode()
        ).hexdigest()
        yield RunRequest(
            partition_key=pk,
            run_key=f"{pk}::{etag_hash}",
            tags={"triggered_by": "owntracks_s3_sensor"},
        )


defs = Definitions(sensors=[owntracks_s3_sensor])
