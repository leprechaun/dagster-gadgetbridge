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


def _group_by_month(files: dict[str, str]) -> dict[str, dict[str, str]]:
    grouped: dict[str, dict[str, str]] = defaultdict(dict)
    for key, etag in files.items():
        grouped[_month_from_key(key)][key] = etag
    return grouped


def plan_run_requests(
    current: dict[str, str], previous: dict[str, str]
) -> list[dict]:
    """Pure decision logic: given current and previously-seen {s3_key: etag}
    maps, return one entry per month whose files changed, sorted by month.
    """
    current_by_month = _group_by_month(current)
    previous_by_month = _group_by_month(previous)

    affected_months = sorted(
        month
        for month, files in current_by_month.items()
        if files != previous_by_month.get(month)
    )

    requests = []
    for month in affected_months:
        pk = _partition_key(month)
        etag_hash = hashlib.md5(
            json.dumps(sorted(current_by_month[month].items())).encode()
        ).hexdigest()
        requests.append(
            {"partition_key": pk, "month": month, "run_key": f"{pk}::{etag_hash}"}
        )
    return requests


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

    run_requests = plan_run_requests(current, previous)

    if not run_requests:
        yield SkipReason(
            f"No changes detected across {len(current)} OwnTracks .rec file(s)."
        )
        return

    context.log.info(f"Affected months: {[r['month'] for r in run_requests]}")
    context.update_cursor(json.dumps(current))

    for req in run_requests:
        yield RunRequest(
            partition_key=req["partition_key"],
            run_key=req["run_key"],
            tags={"triggered_by": "owntracks_s3_sensor"},
        )


defs = Definitions(sensors=[owntracks_s3_sensor])
