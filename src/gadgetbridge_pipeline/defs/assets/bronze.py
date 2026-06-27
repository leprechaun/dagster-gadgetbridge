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
        "epoch_unit": "s",
        "description": "a wide table of per minute metrics including step count, sleep, vigor of movement, etc"
    },
    "generic_temperature_sample": {
        "epoch_unit": "ms",
        "description": "temperature of the sensor"
    },
    "huami_sleep_respiratory_rate_sample": {
        "epoch_unit": "ms",
        "description": "night time respiratory rate"
    },
    "generic_hrv_value_sample": {
        "epoch_unit": "ms",
        "description": "Heart rate variability"
    },
    "huami_stress_sample": {
        "epoch_unit": "ms",
        "description": "A bad nmeasurement of stress, based on HRV. Inaccurate."
    },
    "huami_spo2_sample": {
        "epoch_unit": "ms",
        "description": "SPO2 samples: more often at night"
    },
    "huami_pai_sample": {
        "epoch_unit": "ms",
        "description": "Amazfit's calculation of the PAI health metric"
    },
    "battery_level": {
        "epoch_unit": "s",
        "description": "The battery level over time: 0% to 100%"
    },
    "huami_sleep_session_sample": {
        "epoch_unit": "ms",
        "description": "sleep session with binary data. there can be overlapping sessions during the same day."
    },
}



def _make_bronze_asset(table_name: str, settings: Dict[str, str]):
    @dg.asset(
        name=table_name.lower(),
        group_name="gadgetbridge",
        io_manager_key="deltalake_io_manager",
        key_prefix=["gadgetbridge", "bronze"],
        automation_condition=AutomationCondition.eager(),
        description=settings.get('description')
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
