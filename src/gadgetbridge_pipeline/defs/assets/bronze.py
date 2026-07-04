from typing import Any, Dict

import polars as pl
import dagster as dg

from dagster import AutomationCondition, Definitions, AssetExecutionContext, AssetCheckResult

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
        "description": "The battery level over time: 0% to 100%",
        "schema": pl.Schema({'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'), 'DEVICE_ID': pl.Int64, 'LEVEL': pl.Int64, 'BATTERY_INDEX': pl.Int64})
    },
    "huami_sleep_session_sample": {
        "epoch_unit": "ms",
        "required": {"TIMESTAMP", "DEVICE_ID", "USER_ID", "DATA"},
        "description": "sleep session with binary data. there can be overlapping sessions during the same day."
    },
}

def _make_asset(table_name: str, settings: Dict[str, Any]):

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

def _make_asset_check(table_name: str, settings: Dict[str, Any]):

    @dg.asset_check(
        asset=dg.AssetKey(["gadgetbridge", "bronze", table_name]),
        blocking=True,
        name="%s_schema_matches_expectations" % table_name
    )
    def _asset_check(battery_level: pl.DataFrame) -> AssetCheckResult:
        expected_schema = _TABLES[table_name]['schema']

        actual_schema = battery_level.schema

        if actual_schema != expected_schema:
            differences = []

            for column, expected_type in expected_schema.items():
                actual_type = actual_schema.get(column)
                if actual_type != expected_type:
                    differences.append(f"Column '{column}': expected {expected_type}, got {actual_type}")

            for column in actual_schema:
                if column not in expected_schema:
                    differences.append(f"Unexpected column: '{column}'")

            return AssetCheckResult(
                passed=False,
                description="Schema mismatch",
                metadata={"differences": "\n".join(differences)},
            )
        else:
            return AssetCheckResult(
                passed=True
            )

    return _asset_check


def _make_bronze(table_name: str, settings: Dict[str, str]):
    _asset = _make_asset(table_name, settings)

    if 'schema' in settings:
        _check = _make_asset_check(table_name, settings)
    else:
        _check = None


    return (_asset, _check)

_tables = []
_checks = []


for (table, settings) in _TABLES.items():
    t, c = _make_bronze(table, settings)
    _tables.append(t)

    if c is not None:
        _checks.append(c)


defs = Definitions(
    assets=_tables,
    asset_checks=_checks
)
