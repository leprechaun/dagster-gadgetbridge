import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext
from typing import Dict

_TABLES = {
    "huami_extended_activity_sample": {
    },
    "generic_temperature_sample": {
    },
    "huami_sleep_respiratory_rate_sample": {
    },
    "generic_hrv_value_sample": {
    },
    "huami_stress_sample": {
    },
    "huami_spo2_sample": {
    },
    "huami_pai_sample": {
    },
    "battery_level": {
    },
    "huami_sleep_session_sample": {
    },
}


def apply_silver_transform(df: pl.DataFrame, epoch_unit: str) -> pl.DataFrame:
    return df


def _make_silver_asset(table_name: str, settings: Dict[str, str]):
    @dg.asset(
        name=table_name,
        group_name="gadgetbridge_silver",
        io_manager_key="gadgetbridge_silver_io_manager",
        key_prefix="gadgetbridge-silver",
        ins={"df": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", table_name]))},
        automation_condition=AutomationCondition.eager(),
    )
    def _asset(context: AssetExecutionContext, df: pl.DataFrame) -> pl.DataFrame:
        return df

    _asset.__name__ = f"silver_{table_name}"
    return _asset


defs = Definitions(assets=[_make_silver_asset(table, settings) for (table, settings) in _TABLES.items()])
