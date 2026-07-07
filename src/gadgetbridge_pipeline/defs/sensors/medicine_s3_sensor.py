"""
medicine_s3_sensor
------------------
Polls S3 for changes to the two medicine CSV files (prescriptions and skips).
Triggers rematerialization of medicine_log when either file's ETag changes.
daily_medicine_adherence follows automatically via its eager automation condition.
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


@sensor(
    name="medicine_s3_sensor",
    description="Triggers medicine_log materialization when prescriptions.csv or medicine_skips.csv changes on S3.",
    minimum_interval_seconds=300,
    default_status=DefaultSensorStatus.RUNNING,
    asset_selection=AssetSelection.assets(AssetKey(["gadgetbridge", "bronze", "medicine_log"])),
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

    cursor: dict = {}
    if context.cursor:
        try:
            cursor = json.loads(context.cursor)
        except (json.JSONDecodeError, ValueError):
            cursor = {}

    if cursor.get("etags") == current_etags:
        yield SkipReason("ETags unchanged — medicine CSVs have not been updated.")
        return

    context.update_cursor(json.dumps({"etags": current_etags}))

    combined_etag = "-".join(current_etags[k] for k in sorted(current_etags))
    yield RunRequest(
        run_key=combined_etag,
        tags={"triggered_by": "medicine_s3_sensor"},
    )


defs = Definitions(sensors=[medicine_s3_sensor])
