import dagster as dg
from gadgetbridge_pipeline.defs.resources import S3ClientResource
import os


_SQLITE_LOCAL_PATH = "/tmp/gb.db"


@dg.asset(
    group_name="gadgetbridge",
    key_prefix=["gadgetbridge", "raw"],
    description="SQLite database downloaded from S3. Re-downloaded only when the S3 ETag changes.",
    io_manager_key="sqlite_s3_io_manager",
)
def gadgetbridge_db_file(context: dg.AssetExecutionContext, s3: S3ClientResource) -> dg.Output[str]:
    client = s3.get_client()
    head = client.head_object(Bucket=s3.bucket, Key=s3.key)
    etag = head["ETag"]
    last_modified = head["LastModified"].isoformat()
    context.log.info(f"S3 object  ETag={etag}  LastModified={last_modified}")
    os.makedirs(os.path.dirname(_SQLITE_LOCAL_PATH) or ".", exist_ok=True)
    context.log.info(f"Downloading s3://{s3.bucket}/{s3.key} → {_SQLITE_LOCAL_PATH}")
    client.download_file(s3.bucket, s3.key, _SQLITE_LOCAL_PATH)
    return dg.Output(
        value=_SQLITE_LOCAL_PATH,
        metadata={
            "s3_bucket": s3.bucket,
            "s3_key": s3.key,
            "s3_etag": etag,
            "s3_last_modified": last_modified,
            "size_bytes": os.path.getsize(_SQLITE_LOCAL_PATH),
        },
    )
