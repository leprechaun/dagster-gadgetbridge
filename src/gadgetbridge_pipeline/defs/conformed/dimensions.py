from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import dagster as dg
import polars as pl
from dagster import AutomationCondition, Definitions

from gadgetbridge_pipeline.defs.conformed.calendar import (
    build_dim_date,
    build_dim_time_of_day,
    build_person_timezone,
    find_invalid_timezones,
    find_persons_without_one_covering_timezone,
)
from gadgetbridge_pipeline.defs.conformed.constants import DEFAULT_TIMEZONE, DIM_DATE_START


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

@dg.asset(
    name="dim_date",
    group_name="conformed",
    key_prefix=["conformed", "gold"],
    io_manager_key="conformed_deltalake_io_manager",
    kinds={"deltalake"},
    description="One row per Local Date, typed date key; role-playing in facts.",
    automation_condition=AutomationCondition.on_cron("0 0 * * *"),
)
def dim_date() -> pl.DataFrame:
    today = datetime.now(ZoneInfo(DEFAULT_TIMEZONE)).date()
    return build_dim_date(DIM_DATE_START, today + timedelta(days=365))


@dg.asset(
    name="dim_time_of_day",
    group_name="conformed",
    key_prefix=["conformed", "gold"],
    io_manager_key="conformed_deltalake_io_manager",
    kinds={"deltalake"},
    description="1,440 minute-of-day rows plus the `Whole day` member (key -1).",
    automation_condition=AutomationCondition.on_missing(),
)
def dim_time_of_day() -> pl.DataFrame:
    return build_dim_time_of_day()


@dg.asset(
    name="person_timezone",
    group_name="conformed",
    key_prefix=["conformed", "gold"],
    io_manager_key="conformed_deltalake_io_manager",
    kinds={"deltalake"},
    ins={"persons": dg.AssetIn(key=["conformed", "bronze", "persons"])},
    automation_condition=AutomationCondition.eager(),
    description=(
        "IANA timezone per Person over time. A lookup used once in fact builds, "
        "not a dimension. One open-ended default row per Person for now."
    ),
)
def person_timezone(persons: pl.DataFrame) -> pl.DataFrame:
    return build_person_timezone(persons)


_PERSON_TZ_KEY = dg.AssetKey(["conformed", "gold", "person_timezone"])


@dg.asset_check(
    asset=_PERSON_TZ_KEY,
    blocking=True,
    name="person_has_one_covering_timezone",
    additional_ins={"persons": dg.AssetIn(key=["conformed", "bronze", "persons"])},
)
def person_has_one_covering_timezone(
    person_timezone: pl.DataFrame, persons: pl.DataFrame
) -> dg.AssetCheckResult:
    bad = find_persons_without_one_covering_timezone(
        persons["person_id"].to_list(), person_timezone
    )
    return dg.AssetCheckResult(passed=not bad, metadata={"uncovered_persons": str(bad)})


@dg.asset_check(asset=_PERSON_TZ_KEY, blocking=True, name="timezone_names_valid")
def timezone_names_valid(person_timezone: pl.DataFrame) -> dg.AssetCheckResult:
    bad = find_invalid_timezones(person_timezone)
    return dg.AssetCheckResult(passed=not bad, metadata={"invalid_timezones": str(bad)})


defs = Definitions(
    assets=[dim_person, dim_device, dim_phone, dim_date, dim_time_of_day, person_timezone],
    asset_checks=[person_has_one_covering_timezone, timezone_names_valid],
)
