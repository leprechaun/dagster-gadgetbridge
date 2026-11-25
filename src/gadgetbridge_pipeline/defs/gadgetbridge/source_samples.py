"""One fact per remaining wearable source table, generated from the SOURCES dict."""

from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.devices import resolve_device
from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_person_device_mismatches,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys

# Adding a source is a new entry here, not new code. `measures` maps each fact
# column (with its unit in the name) to the bronze column it comes from.
SOURCES: dict[str, dict] = {
    "fact_hrv_sample": {
        "table": "generic_hrv_value_sample",
        "description": "One heart-rate-variability reading.",
        "measures": {"hrv_ms": "VALUE"},
    },
    "fact_temperature_sample": {
        "table": "generic_temperature_sample",
        "description": "One wrist-sensor temperature reading (not core body temperature).",
        "measures": {"temperature_celsius": "TEMPERATURE"},
    },
    "fact_pai_sample": {
        "table": "huami_pai_sample",
        "description": "One PAI snapshot; there are several per day.",
        "measures": {
            "pai_low": "PAI_LOW", "pai_moderate": "PAI_MODERATE", "pai_high": "PAI_HIGH",
            "time_low": "TIME_LOW", "time_moderate": "TIME_MODERATE", "time_high": "TIME_HIGH",
            "pai_today": "PAI_TODAY", "pai_total": "PAI_TOTAL",
        },
    },
    "fact_respiratory_rate_sample": {
        "table": "huami_sleep_respiratory_rate_sample",
        "description": "One night-time respiratory-rate reading.",
        "measures": {"rate_breaths_per_minute": "RATE"},
    },
    "fact_spo2_sample": {
        "table": "huami_spo2_sample",
        "description": "One blood-oxygen saturation reading.",
        "measures": {"spo2_percent": "SPO2"},
    },
    "fact_stress_sample": {
        "table": "huami_stress_sample",
        "description": "One Zepp stress score (0-100, derived from HRV).",
        "measures": {"stress_score": "STRESS"},
    },
}


def build_fact_source_sample(
    bronze: pl.DataFrame,
    measures: dict[str, str],
    dim_device: pl.DataFrame,
    person_timezone: pl.DataFrame,
) -> pl.DataFrame:
    """One fact row per source row. `measures` maps each fact measure column to
    its bronze column; the seconds of the timestamp are kept.
    """
    resolved = resolve_device(bronze, "DEVICE_ID", "TIMESTAMP", dim_device).select(
        "device_key", "person_key",
        pl.col("TIMESTAMP").alias("timestamp_utc"),
        *(pl.col(source).alias(name) for name, source in measures.items()),
    )
    return with_local_keys(resolved, "timestamp_utc", person_timezone).select(
        "date_key", "time_key", "device_key", "person_key", "timestamp_utc", *measures
    )


_GOLD = ["gadgetbridge", "gold"]
_CONFORMED = ["conformed", "gold"]
_CHECKS = ["grain_unique", "keys_resolve", "person_matches_device"]


def _make_fact_asset(fact_name: str, spec: dict) -> dg.AssetsDefinition:
    key = dg.AssetKey([*_GOLD, fact_name])
    measures = spec["measures"]

    @dg.asset(
        name=fact_name,
        group_name="gadgetbridge",
        key_prefix=_GOLD,
        io_manager_key="deltalake_io_manager",
        kinds={"deltalake"},
        description=f"{spec['description']} One row per source row of {spec['table']}.",
        ins={
            "source": dg.AssetIn(key=["gadgetbridge", "bronze", spec["table"]]),
            "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
            "person_timezone": dg.AssetIn(key=[*_CONFORMED, "person_timezone"]),
            "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
            "dim_time_of_day": dg.AssetIn(key=[*_CONFORMED, "dim_time_of_day"]),
            "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
        },
        check_specs=[dg.AssetCheckSpec(name=n, asset=key, blocking=True) for n in _CHECKS],
        automation_condition=dg.AutomationCondition.eager(),
    )
    def _fact(
        source: pl.DataFrame,
        dim_device: pl.DataFrame,
        person_timezone: pl.DataFrame,
        dim_date: pl.DataFrame,
        dim_time_of_day: pl.DataFrame,
        dim_person: pl.DataFrame,
    ):
        fact = build_fact_source_sample(source, measures, dim_device, person_timezone)
        yield dg.Output(fact)

        dupes = find_duplicate_grain(fact, ["device_key", "timestamp_utc"])
        yield dg.AssetCheckResult(
            check_name="grain_unique", passed=not dupes,
            metadata={"duplicate_grain": str(dupes[:20])},
        )
        unresolved = {
            k: find_unresolved_keys(fact, k, dim)[:20]
            for k, dim in {
                "date_key": dim_date, "time_key": dim_time_of_day,
                "device_key": dim_device, "person_key": dim_person,
            }.items()
        }
        yield dg.AssetCheckResult(
            check_name="keys_resolve", passed=not any(unresolved.values()),
            metadata={"unresolved": str(unresolved)},
        )
        bad = find_person_device_mismatches(fact, dim_device)
        yield dg.AssetCheckResult(
            check_name="person_matches_device", passed=not bad,
            metadata={"mismatches": str(bad[:20])},
        )

    return _fact


source_sample_facts = [_make_fact_asset(name, spec) for name, spec in SOURCES.items()]

defs = dg.Definitions(assets=source_sample_facts)
