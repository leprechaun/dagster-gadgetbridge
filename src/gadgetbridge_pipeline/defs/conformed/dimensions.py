from __future__ import annotations

import dagster as dg
import polars as pl
from dagster import AutomationCondition, Definitions


def build_dim_device(devices: pl.DataFrame) -> pl.DataFrame:
    """Wearables only. The key includes valid_from, so a Gadgetbridge device
    id reused after a database reset is a different Device (ADR-0001).
    """
    return (
        devices.filter(pl.col("kind") == "wearable")
        .select(
            (pl.col("natural_key") + "@" + pl.col("valid_from").cast(pl.String)).alias(
                "device_key"
            ),
            pl.col("person_id").alias("person_key"),
            pl.col("natural_key").alias("gadgetbridge_device_id"),
            "valid_from",
            "valid_to",
        )
    )


def build_dim_phone(devices: pl.DataFrame) -> pl.DataFrame:
    """Phones only, keyed by the OwnTracks `user/device` (ADR-0001)."""
    return devices.filter(pl.col("kind") == "phone").select(
        pl.col("natural_key").alias("phone_key"),
        pl.col("person_id").alias("person_key"),
        "valid_from",
        "valid_to",
    )


def build_dim_person(persons: pl.DataFrame) -> pl.DataFrame:
    return persons.select(pl.col("person_id").alias("person_key"), "display_name")


def _dim_asset(name: str, upstream: str, builder):
    @dg.asset(
        name=name,
        group_name="conformed",
        key_prefix=["conformed", "gold"],
        io_manager_key="conformed_deltalake_io_manager",
        kinds={"deltalake"},
        ins={upstream: dg.AssetIn(key=["conformed", "bronze", upstream])},
        automation_condition=AutomationCondition.eager(),
        description=f"{name} dimension with deterministic text keys (ADR-0001).",
    )
    def _asset(**frames: pl.DataFrame) -> pl.DataFrame:
        return builder(frames[upstream])

    return _asset


dim_person = _dim_asset("dim_person", "persons", build_dim_person)
dim_device = _dim_asset("dim_device", "devices", build_dim_device)
dim_phone = _dim_asset("dim_phone", "devices", build_dim_phone)

defs = Definitions(assets=[dim_person, dim_device, dim_phone])
