import boto3

import dagster as dg
from dagster import Definitions, EnvVar, InputContext, OutputContext
from dagster_deltalake import S3Config
from dagster_deltalake_polars import DeltaLakePolarsIOManager


class SqliteS3IOManager(dg.ConfigurableIOManager):
    """Round-trips a local SQLite file through S3 so downstream pods can load it."""
    bucket: str
    prefix: str
    endpoint_url: str

    def _s3(self):
        return boto3.client("s3", endpoint_url=self.endpoint_url)

    def _key(self, asset_key):
        return f"{self.prefix}/{'/'.join(asset_key.path)}.db"

    def handle_output(self, context: OutputContext, obj: str):
        key = self._key(context.asset_key)
        self._s3().upload_file(obj, self.bucket, key)
        context.log.info(f"Uploaded {obj} → s3://{self.bucket}/{key}")

    def load_input(self, context: InputContext) -> str:
        key = self._key(context.asset_key)
        local_path = f"/tmp/{context.asset_key.path[-1]}.db"
        self._s3().download_file(self.bucket, key, local_path)
        context.log.info(f"Downloaded s3://{self.bucket}/{key} → {local_path}")
        return local_path


class S3ClientResource(dg.ConfigurableResource):
    endpoint_url: str
    bucket: str
    key: str

    def get_client(self):
        return boto3.client("s3", endpoint_url=self.endpoint_url)


_s3_config = S3Config(allow_unsafe_rename=True, endpoint=EnvVar("AWS_ENDPOINT_URL_S3"))

defs = Definitions(resources={
    "s3": S3ClientResource(
        endpoint_url=EnvVar("AWS_ENDPOINT_URL_S3"),
        bucket="android-backups",
        key="GadgetBridge/Gadgetbridge.db",
    ),
    "sqlite_s3_io_manager": SqliteS3IOManager(
        bucket="deltalake",
        prefix="gadgetbridge/raw",
        endpoint_url=EnvVar("AWS_ENDPOINT_URL_S3"),
    ),
    "deltalake_io_manager": DeltaLakePolarsIOManager(
        root_uri="s3://deltalake/gadgetbridge/",
        storage_options=_s3_config,
        # no schema — key_prefix on each asset drives the subfolder
    ),
})
