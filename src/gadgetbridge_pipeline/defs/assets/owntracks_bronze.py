import json
import os
from datetime import datetime

import boto3
import polars as pl
import dagster as dg
from dagster import Definitions, AssetExecutionContext, MonthlyPartitionsDefinition

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
_PREFIX = "owntracks/raw/rec/"

owntracks_partitions = MonthlyPartitionsDefinition(start_date="2020-01-01", end_offset=1)

_RAW_SCHEMA = pl.Schema({
    "id":         pl.String,
    "user":       pl.String,
    "device":     pl.String,
    "arrived_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "tst":        pl.Int64,
    "created_at": pl.Int64,
    "lat":        pl.Float64,
    "lon":        pl.Float64,
    "alt":        pl.Int64,
    "acc":        pl.Int64,
    "vac":        pl.Int64,
    "batt":       pl.Int64,
    "bs":         pl.Int64,
    "conn":       pl.String,
    "ssid":       pl.String,
    "bssid":      pl.String,
    "tid":        pl.String,
    "source":     pl.String,
    "m":          pl.Int64,
})

_SCHEMA = pl.Schema({
    "id":         pl.String,
    "user":       pl.String,
    "device":     pl.String,
    "year_month": pl.String,
    "arrived_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "timestamp":  pl.Datetime(time_unit="us", time_zone="UTC"),
    "created_at": pl.Datetime(time_unit="us", time_zone="UTC"),
    "lat":        pl.Float64,
    "lon":        pl.Float64,
    "alt":        pl.Int64,
    "acc":        pl.Int64,
    "vac":        pl.Int64,
    "batt":       pl.Int64,
    "bs":         pl.Int64,
    "conn":       pl.String,
    "ssid":       pl.String,
    "bssid":      pl.String,
    "tid":        pl.String,
    "source":     pl.String,
    "m":          pl.Int64,
})


def _s3_client():
    return boto3.client("s3", endpoint_url=os.environ.get("AWS_ENDPOINT_URL_S3"))


def parse_rec_lines(lines: list[str], user: str, device: str) -> tuple[list[dict], list[str]]:
    """Parse lines from a .rec file into raw dicts. Only _type=location entries are kept.

    Returns (records, dropped) where dropped is a list of raw arrived_at strings that
    could not be parsed (e.g. null-byte-corrupted lines).
    """
    records = []
    dropped = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        parts = line.split("\t", 2)
        if len(parts) < 3:
            continue
        arrived_at_str, _, json_str = parts
        try:
            payload = json.loads(json_str)
        except json.JSONDecodeError:
            continue
        if payload.get("_type") != "location":
            continue
        try:
            arrived_at = datetime.fromisoformat(arrived_at_str.strip(" \t\r\n\x00"))
        except ValueError:
            dropped.append(repr(arrived_at_str))
            continue
        records.append({
            "id":         payload.get("_id"),
            "user":       user,
            "device":     device,
            "arrived_at": arrived_at,
            "tst":        payload.get("tst"),
            "created_at": payload.get("created_at"),
            "lat":        payload.get("lat"),
            "lon":        payload.get("lon"),
            "alt":        payload.get("alt"),
            "acc":        payload.get("acc"),
            "vac":        payload.get("vac"),
            "batt":       payload.get("batt"),
            "bs":         payload.get("bs"),
            "conn":       payload.get("conn"),
            "ssid":       payload.get("SSID"),
            "bssid":      payload.get("BSSID"),
            "tid":        payload.get("tid"),
            "source":     payload.get("source"),
            "m":          payload.get("m"),
        })
    return records, dropped


def _transform(records: list[dict], partition_key: str) -> pl.DataFrame:
    return (
        pl.DataFrame(records, schema=_RAW_SCHEMA)
        .rename({"tst": "timestamp"})
        .with_columns(
            pl.lit(partition_key).alias("year_month"),
            pl.from_epoch(pl.col("timestamp").cast(pl.Int64), time_unit="s")
            .dt.replace_time_zone("UTC")
            .alias("timestamp"),
            pl.from_epoch(pl.col("created_at").cast(pl.Int64), time_unit="s")
            .dt.replace_time_zone("UTC")
            .alias("created_at"),
            pl.col("arrived_at").dt.convert_time_zone("UTC").alias("arrived_at"),
        )
        .select(list(_SCHEMA.keys()))
    )


@dg.asset(
    partitions_def=owntracks_partitions,
    group_name="owntracks",
    io_manager_key="owntracks_deltalake_io_manager",
    key_prefix=["owntracks", "bronze"],
    metadata={"partition_expr": "year_month"},
    op_tags={"dagster/concurrency_key": "owntracks_deltalake"},
    description="Parsed OwnTracks location records, written to Delta Lake via partition_expr on year_month.",
)
def location_records(context: AssetExecutionContext) -> pl.DataFrame:
    # partition_key is "2026-07-01"; filenames use "2026-07"
    partition_key = context.partition_key
    year_month = partition_key[:7]

    client = _s3_client()
    paginator = client.get_paginator("list_objects_v2")

    all_records: list[dict] = []
    all_dropped: list[str] = []
    files_read: list[str] = []

    for page in paginator.paginate(Bucket=_BUCKET, Prefix=_PREFIX):
        for obj in page.get("Contents", []):
            key: str = obj["Key"]
            if not key.endswith(f"{year_month}.rec"):
                continue
            parts = key.split("/")
            if len(parts) < 6:
                context.log.warning(f"Skipping unexpected key shape: {key}")
                continue
            user, device = parts[3], parts[4]
            body = client.get_object(Bucket=_BUCKET, Key=key)["Body"].read().decode("utf-8")
            records, dropped = parse_rec_lines(body.splitlines(), user, device)
            all_records.extend(records)
            all_dropped.extend(dropped)
            files_read.append(key)
            if dropped:
                for raw in dropped:
                    context.log.warning(f"{key}: dropped record with unparseable arrived_at: {raw}")
            context.log.info(f"{key}: {len(records)} location records, {len(dropped)} dropped")

    context.log.info(f"Total: {len(all_records)} records, {len(all_dropped)} dropped, from {len(files_read)} file(s)")

    context.add_output_metadata({
        "records": len(all_records),
        "dropped": len(all_dropped),
        "files": len(files_read),
    })

    if not all_records:
        return pl.DataFrame(schema=_SCHEMA)

    return _transform(all_records, partition_key)


defs = Definitions(assets=[location_records])
