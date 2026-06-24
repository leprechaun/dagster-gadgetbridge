import polars as pl
import dagster as dg
from dagster import AutomationCondition, Definitions, AssetExecutionContext
from typing import Dict

def apply_bronze_transform(df: pl.DataFrame, epoch_unit) -> pl.DataFrame:
    return df.with_columns(
        pl.from_epoch(
            pl.col("TIMESTAMP"),
            time_unit=epoch_unit
        ).dt.replace_time_zone("Asia/Bangkok").alias("TIMESTAMP")
    )

def _read_table(table: str, db_path: str) -> pl.DataFrame:
    return pl.read_database_uri(
        f"SELECT * FROM {table}",
        f"sqlite://{db_path}"
    )

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



def _make_bronze_asset(table_name: str, settings: Dict[str, str]):
    @dg.asset(
        name=table_name.lower(),
        group_name="gadgetbridge",
        io_manager_key="deltalake_io_manager",
        key_prefix=["gadgetbridge", "bronze"],
        automation_condition=AutomationCondition.eager(),
    )
    def _asset(context: AssetExecutionContext, gadgetbridge_db_file) -> pl.DataFrame:
        return apply_bronze_transform(
            _read_table(
                table_name.upper(),
                gadgetbridge_db_file),
            settings.get('epoch_unit', 'ms')
        )

    _asset.__name__ = table_name
    return _asset

_tables = [_make_bronze_asset(table, settings) for (table, settings) in _TABLES.items()]
defs = Definitions(assets=_tables)
