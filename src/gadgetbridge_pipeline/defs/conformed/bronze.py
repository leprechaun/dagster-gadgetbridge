from __future__ import annotations

import io
import os

import dagster as dg
import polars as pl
from dagster import AssetCheckResult, Definitions

from gadgetbridge_pipeline.defs.conformed.validation import (
    find_duplicate_ids,
    find_invalid_kinds,
    find_multi_owner_devices,
    find_overlapping_periods,
)
from gadgetbridge_pipeline.defs.resources import S3ClientResource, column

_BUCKET = os.environ.get("DELTALAKE_BUCKET", "deltalake")
PERSONS_KEY = "conformed/raw/persons.csv"
DEVICES_KEY = "conformed/raw/devices.csv"

_PERSONS_PREFIX = ["conformed", "bronze", "persons"]
_DEVICES_PREFIX = ["conformed", "bronze", "devices"]


def _read_s3_csv(s3: S3ClientResource, key: str, **kwargs) -> pl.DataFrame:
    buffer = io.BytesIO()
    s3.get_client().download_fileobj(_BUCKET, key, buffer)
    buffer.seek(0)
    return pl.read_csv(buffer, **kwargs)


@dg.asset(
    name="persons",
    group_name="conformed",
    key_prefix=["conformed", "bronze"],
    io_manager_key="conformed_deltalake_io_manager",
    kinds={"s3", "deltalake"},
    description="Persons CSV mirrored from S3, versioned in Delta Lake.",
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("person_id", "Slug identifying the Person; source of person_key"),
        column("display_name", "Free-text name"),
    ])},
)
def persons(s3: S3ClientResource) -> pl.DataFrame:
    return _read_s3_csv(s3, PERSONS_KEY)


@dg.asset(
    name="devices",
    group_name="conformed",
    key_prefix=["conformed", "bronze"],
    io_manager_key="conformed_deltalake_io_manager",
    kinds={"s3", "deltalake"},
    description="Wearables and phones, one row per period of use, mirrored from S3.",
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("person_id", "Owner; a person_id in persons"),
        column("kind", "wearable or phone"),
        column("natural_key", "Gadgetbridge DEVICE_ID (wearable) or OwnTracks user/device (phone)"),
        column("valid_from", "First day of use, inclusive"),
        column("valid_to", "Last day of use, inclusive; null = still in use"),
    ])},
)
def devices(s3: S3ClientResource) -> pl.DataFrame:
    return _read_s3_csv(
        s3,
        DEVICES_KEY,
        schema_overrides={
            "natural_key": pl.String,
            "valid_from": pl.Date,
            "valid_to": pl.Date,
        },
    )


def _result(offenders: list[str], label: str) -> AssetCheckResult:
    return AssetCheckResult(passed=not offenders, metadata={label: str(offenders)})


@dg.asset_check(asset=dg.AssetKey(_PERSONS_PREFIX), blocking=True, name="person_ids_unique")
def person_ids_unique(persons: pl.DataFrame) -> AssetCheckResult:
    return _result(find_duplicate_ids(persons["person_id"]), "duplicate_person_ids")


@dg.asset_check(asset=dg.AssetKey(_DEVICES_PREFIX), blocking=True, name="device_kind_valid")
def device_kind_valid(devices: pl.DataFrame) -> AssetCheckResult:
    return _result(find_invalid_kinds(devices), "invalid_kind_natural_keys")


@dg.asset_check(asset=dg.AssetKey(_DEVICES_PREFIX), blocking=True, name="device_has_one_owner")
def device_has_one_owner(devices: pl.DataFrame) -> AssetCheckResult:
    return _result(find_multi_owner_devices(devices), "multi_owner_natural_keys")


@dg.asset_check(
    asset=dg.AssetKey(_DEVICES_PREFIX), blocking=True, name="device_periods_do_not_overlap"
)
def device_periods_do_not_overlap(devices: pl.DataFrame) -> AssetCheckResult:
    return _result(find_overlapping_periods(devices), "overlapping_natural_keys")


defs = Definitions(
    assets=[persons, devices],
    asset_checks=[
        person_ids_unique,
        device_kind_valid,
        device_has_one_owner,
        device_periods_do_not_overlap,
    ],
)
