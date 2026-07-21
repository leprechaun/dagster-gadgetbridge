"""
medicine_s3_sensor
------------------
Polls S3 for changes to the two medicine CSV files (prescriptions and skips).
Triggers rematerialization of the raw prescriptions/medicine_skips assets when
either file's ETag changes. medicine_log and daily_medicine_adherence follow
automatically via their eager automation conditions.
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

_MEDICINE_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_WATCHED_KEYS = {
    "prescriptions": "gadgetbridge/raw/prescriptions.csv",
    "skips": "gadgetbridge/raw/medicine_skips.csv",
}


def parse_cursor(cursor: str | None) -> dict:
    if not cursor:
        return {}
    try:
        return json.loads(cursor)
    except (json.JSONDecodeError, ValueError):
        return {}


def evaluate_change(current_etags: dict[str, str], cursor: dict) -> dict:
    """Pure decision logic: compare current per-file ETags against the
    cursor's last-seen ETags and decide whether the sensor should skip or run.
    """
    if cursor.get("etags") == current_etags:
        return {
            "action": "skip",
            "reason": "ETags unchanged — medicine CSVs have not been updated.",
        }

    combined_etag = "-".join(current_etags[k] for k in sorted(current_etags))
    return {
        "action": "run",
        "run_key": combined_etag,
        "new_cursor": {"etags": current_etags},
        "tags": {"triggered_by": "medicine_s3_sensor"},
    }


@sensor(
    name="medicine_s3_sensor",
    description="Triggers medicine_log materialization when prescriptions.csv or medicine_skips.csv changes on S3.",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.assets(
        AssetKey(["gadgetbridge", "raw", "prescriptions"]),
        AssetKey(["gadgetbridge", "raw", "medicine_skips"]),
    ),
)
def medicine_s3_sensor(context: SensorEvaluationContext, s3: S3ClientResource):
    client = s3.get_client()

    current_etags: dict[str, str] = {}
    for name, key in _WATCHED_KEYS.items():
        try:
            head = client.head_object(Bucket=_MEDICINE_BUCKET, Key=key)
            current_etags[name] = head["ETag"]
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            yield SkipReason(f"S3 HEAD failed for {key} ({code}) — will retry next tick")
            return

    cursor = parse_cursor(context.cursor)
    decision = evaluate_change(current_etags, cursor)

    if decision["action"] == "skip":
        yield SkipReason(decision["reason"])
        return

    context.update_cursor(json.dumps(decision["new_cursor"]))

    yield RunRequest(run_key=decision["run_key"], tags=decision["tags"])


defs = Definitions(sensors=[medicine_s3_sensor])
