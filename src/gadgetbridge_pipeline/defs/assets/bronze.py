import os
import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext, Output
from gadgetbridge_pipeline.defs.resources import S3ClientResource

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


def _read_table(table: str, db_path: str) -> pl.DataFrame:
    return pl.read_database_uri(f"SELECT * FROM {table}", f"sqlite://{db_path}")


@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_extended_activity_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_EXTENDED_ACTIVITY_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def generic_temperature_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("GENERIC_TEMPERATURE_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_sleep_respiratory_rate_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_SLEEP_RESPIRATORY_RATE_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def generic_hrv_value_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("GENERIC_HRV_VALUE_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_stress_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_STRESS_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_spo2_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_SPO2_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_pai_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_PAI_SAMPLE", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def battery_level(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("BATTERY_LEVEL", gadgetbridge_db_file)

@dg.asset(group_name="gadgetbridge", io_manager_key="gadgetbridge_io_manager", key_prefix="gadgetbridge", automation_condition=AutomationCondition.eager())
def huami_sleep_session_sample(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
    return _read_table("HUAMI_SLEEP_SESSION_SAMPLE", gadgetbridge_db_file)


defs = Definitions(assets=[
    gadgetbridge_db_file,
    huami_extended_activity_sample,
    generic_temperature_sample,
    huami_sleep_respiratory_rate_sample,
    generic_hrv_value_sample,
    huami_stress_sample,
    huami_spo2_sample,
    huami_pai_sample,
    battery_level,
    huami_sleep_session_sample,
])
