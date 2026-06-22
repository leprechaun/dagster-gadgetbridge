import os
import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext, Output
from gadgetbridge_pipeline.defs.resources import S3ClientResource
from typing import Dict

_SQLITE_LOCAL_PATH = "/tmp/gb.db"


@dg.asset(
    group_name="ingestion",
    description="SQLite database downloaded from S3. Re-downloaded only when the S3 ETag changes.",
    io_manager_key="sqlite_s3_io_manager",
)
def gadgetbridge_db_file(context: AssetExecutionContext, s3: S3ClientResource) -> Output[str]:
    client = s3.get_client()
    head = client.head_object(Bucket=s3.bucket, Key=s3.key)
    etag = head["ETag"]
    last_modified = head["LastModified"].isoformat()
    context.log.info(f"S3 object  ETag={etag}  LastModified={last_modified}")
    os.makedirs(os.path.dirname(_SQLITE_LOCAL_PATH) or ".", exist_ok=True)
    context.log.info(f"Downloading s3://{s3.bucket}/{s3.key} → {_SQLITE_LOCAL_PATH}")
    client.download_file(s3.bucket, s3.key, _SQLITE_LOCAL_PATH)
    return Output(
        value=_SQLITE_LOCAL_PATH,
        metadata={
            "s3_bucket": s3.bucket,
            "s3_key": s3.key,
            "s3_etag": etag,
            "s3_last_modified": last_modified,
            "size_bytes": os.path.getsize(_SQLITE_LOCAL_PATH),
        },
    )


def _read_table(table: str, db_path: str, settings: Dict[str, str]) -> pl.DataFrame:
    return pl.read_database_uri(
        f"SELECT * FROM {table}",
        f"sqlite://{db_path}"
    ).with_columns(
        pl.from_epoch(
            pl.col("TIMESTAMP"), time_unit=settings.get("epoch_unit", "s")
        ).alias("TIMESTAMP")
    )


_TABLES = {
    "huami_extended_activity_sample": {
        "epoch_unit": "s"
    },
    "generic_temperature_sample": {
        "epoch_unit": "ms"
    },
    "huami_sleep_respiratory_rate_sample": {
        "epoch_unit": "ms"
    },
    "generic_hrv_value_sample": {
        "epoch_unit": "ms"
    },
    "huami_stress_sample": {
        "epoch_unit": "ms"
    },
    "huami_spo2_sample": {
        "epoch_unit": "ms"
    },
    "huami_pai_sample": {
        "epoch_unit": "ms"
    },
    "battery_level": {
        "epoch_unit": "s"
    },
    "huami_sleep_session_sample": {
        "epoch_unit": "ms"
    },
}



def _make_bronze_asset(table_name: str, settings: Dict[str, str]):
    @dg.asset(
        name=table_name.lower(),
        group_name="gadgetbridge",
        io_manager_key="gadgetbridge_io_manager",
        key_prefix="gadgetbridge",
        automation_condition=AutomationCondition.eager(),
    )
    def _asset(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
        return _read_table(table_name.upper(), gadgetbridge_db_file, settings)
    _asset.__name__ = table_name
    return _asset


defs = Definitions(assets=[gadgetbridge_db_file] + [_make_bronze_asset(table, settings) for (table, settings) in _TABLES.items()])
