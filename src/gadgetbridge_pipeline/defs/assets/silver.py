import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext
from typing import Dict

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
        context.log.info(f"Processing silver: table: %s -- epoch unit: %s" % (table_name, settings.get('epoch_unit')))
        result = apply_silver_transform(df, settings.get('epoch_unit', 'ms'))
        context.add_output_metadata({"rows": result.shape[0], "columns": result.shape[1]})
        return result
    _asset.__name__ = f"silver_{table_name}"
    return _asset


defs = Definitions(assets=[_make_silver_asset(table, settings) for (table, settings) in _TABLES.items()])
