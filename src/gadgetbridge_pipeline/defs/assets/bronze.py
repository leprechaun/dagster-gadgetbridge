from datetime import datetime, timezone
from typing import Any, Callable, Dict

import polars as pl
import dagster as dg

from dagster import AutomationCondition, Definitions, AssetExecutionContext, AssetCheckResult

START_OF_DATA_COLLECTION = datetime(2026, 1, 1, tzinfo=timezone.utc)

def apply_bronze_transform(df: pl.DataFrame, epoch_unit) -> pl.DataFrame:
    return df.with_columns(
        pl.from_epoch(
            pl.col("TIMESTAMP"),
            time_unit=epoch_unit
        ).dt.replace_time_zone("UTC").alias("TIMESTAMP")
    )

def _read_table(table: str, db_path: str) -> pl.DataFrame:
    return pl.read_database_uri(
        f"SELECT * FROM {table}",
        f"sqlite://{db_path}"
    )

_TABLES = {
    "huami_extended_activity_sample": {
        "epoch_unit": "s",
        "description": "a wide table of per minute metrics including step count, sleep, vigor of movement, etc",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'RAW_INTENSITY': pl.Int64,
            'STEPS': pl.Int64,
            'RAW_KIND': pl.Int64,
            'HEART_RATE': pl.Int64,
            'UNKNOWN1': pl.Int64,
            'SLEEP': pl.Int64,
            'DEEP_SLEEP': pl.Int64,
            'REM_SLEEP': pl.Int64
        })
    },
    "generic_temperature_sample": {
        "epoch_unit": "ms",
        "description": "temperature of the sensor",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'TEMPERATURE': pl.Float64,
            'TEMPERATURE_TYPE': pl.Int64,
            'TEMPERATURE_LOCATION': pl.Int64
        })
    },
    "huami_sleep_respiratory_rate_sample": {
        "epoch_unit": "ms",
        "description": "night time respiratory rate",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'UTC_OFFSET': pl.Int64,
            'RATE': pl.Int64
        })
    },
    "generic_hrv_value_sample": {
        "epoch_unit": "ms",
        "description": "Heart rate variability",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'VALUE': pl.Int64
        })
    },
    "huami_stress_sample": {
        "epoch_unit": "ms",
        "description": "A bad nmeasurement of stress, based on HRV. Inaccurate.",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'TYPE_NUM': pl.Int64,
            'STRESS': pl.Int64
        })
    },
    "huami_spo2_sample": {
        "epoch_unit": "ms",
        "description": "SPO2 samples: more often at night",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'TYPE_NUM': pl.Int64,
            'SPO2': pl.Int64
        })
    },
    "huami_pai_sample": {
        "epoch_unit": "ms",
        "description": "Amazfit's calculation of the PAI health metric",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'UTC_OFFSET': pl.Int64,
            'PAI_LOW': pl.Float64,
            'PAI_MODERATE': pl.Float64,
            'PAI_HIGH': pl.Float64,
            'TIME_LOW': pl.Int64,
            'TIME_MODERATE': pl.Int64,
            'TIME_HIGH': pl.Int64,
            'PAI_TODAY': pl.Float64,
            'PAI_TOTAL': pl.Float64
        })
    },
    "battery_level": {
        "epoch_unit": "s",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'LEVEL': pl.Int64,
            'BATTERY_INDEX': pl.Int64
        })
    },
    "huami_sleep_session_sample": {
        "epoch_unit": "ms",
        "description": "sleep session with binary data. there can be overlapping sessions during the same day.",
        "schema": pl.Schema({
            'TIMESTAMP': pl.Datetime(time_unit='us', time_zone='UTC'),
            'DEVICE_ID': pl.Int64,
            'USER_ID': pl.Int64,
            'DATA': pl.Binary
        })
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
        transformed = apply_bronze_transform(
            _read_table(
                table_name.upper(),
                gadgetbridge_db_file),
            settings.get('epoch_unit', 'ms')
        )

        return transformed

    _asset.__name__ = table_name

    return _asset

def _make_asset_check(table_name: str, settings: Dict[str, Any]):

    @dg.asset_check(
        asset=dg.AssetKey(["gadgetbridge", "bronze", table_name]),
        blocking=True,
        name="%s_schema_matches_expectations" % table_name
    )
    def _asset_check(df: pl.DataFrame) -> AssetCheckResult:
        expected_schema = _TABLES[table_name]['schema']

        actual_schema = df.schema

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


def _make_timestamp_check(table_name: str):

    @dg.asset_check(
        asset=dg.AssetKey(["gadgetbridge", "bronze", table_name]),
        blocking=True,
        name="%s_timestamp_checks" % table_name
    )
    def _asset_check(df: pl.DataFrame) -> AssetCheckResult:
        if "TIMESTAMP" not in df.columns or df.is_empty():
            return AssetCheckResult(
                passed=False,
                description="No TIMESTAMP data to check (missing column or empty table)",
                metadata={"row_count": df.height},
            )

        null_count = int(df["TIMESTAMP"].null_count())
        minimum = df["TIMESTAMP"].min()

        checks = {
            "no_nulls": null_count == 0,
            "is_after_min_timestamp": minimum is not None and minimum >= START_OF_DATA_COLLECTION,
        }

        return AssetCheckResult(
            passed=all(checks.values()),
            metadata=checks | {"null_count": null_count, "minimum": str(minimum)},
        )

    return _asset_check


def _make_bronze(table_name: str, settings: Dict[str, str]):
    _asset = _make_asset(table_name, settings)

    checks = []

    if 'schema' in settings:
        checks.append(_make_asset_check(table_name, settings))

    checks.append(_make_timestamp_check(table_name))

    return (_asset, checks)

_tables = []
_checks = []


for (table, settings) in _TABLES.items():
    t, checks = _make_bronze(table, settings)
    _tables.append(t)
    _checks.extend(checks)


def _make_range_check(
    asset_key: list,
    name: str,
    column: str,
    checks: Dict[str, Callable[[Any, Any], bool]],
    cast=int,
):
    """Factory for a blocking min/max range check on a single column.

    `checks` maps a check name to a predicate over (minimum, maximum). Guards
    against empty tables up front, since `cast(df[column].min())` raises
    TypeError on None rather than failing the check gracefully.
    """

    @dg.asset_check(asset=dg.AssetKey(asset_key), blocking=True, name=name)
    def _check(df: pl.DataFrame) -> AssetCheckResult:
        if df.is_empty():
            return AssetCheckResult(
                passed=False,
                description=f"No {column!r} data to check (empty table)",
                metadata={"row_count": 0},
            )

        minimum = cast(df[column].min())
        maximum = cast(df[column].max())

        results = {check_name: fn(minimum, maximum) for check_name, fn in checks.items()}

        return AssetCheckResult(
            passed=all(results.values()),
            metadata=results | {"minimum": minimum, "maximum": maximum},
        )

    return _check


activity_heartrate_checks = _make_range_check(
    ["gadgetbridge", "bronze", "huami_extended_activity_sample"],
    "huami_extended_activity_sample_heartrate_checks",
    "HEART_RATE",
    {
        "is_positive": lambda mn, mx: mn > 0,
        "is_255 or below": lambda mn, mx: mx <= 255,
    },
)
_checks.append(activity_heartrate_checks)


battery_level_checks = _make_range_check(
    ["gadgetbridge", "bronze", "battery_level"],
    "battery_level_range_checks",
    "LEVEL",
    {
        "is_non_negative": lambda mn, mx: mn >= 0,
        "is_at_most_100": lambda mn, mx: mx <= 100,
    },
)
_checks.append(battery_level_checks)


spo2_checks = _make_range_check(
    ["gadgetbridge", "bronze", "huami_spo2_sample"],
    "huami_spo2_sample_spo2_checks",
    "SPO2",
    {
        "is_at_least_70": lambda mn, mx: mn >= 70,
        "is_at_most_100": lambda mn, mx: mx <= 100,
    },
)
_checks.append(spo2_checks)


temperature_checks = _make_range_check(
    ["gadgetbridge", "bronze", "generic_temperature_sample"],
    "generic_temperature_sample_temperature_checks",
    "TEMPERATURE",
    {
        "is_at_least_15": lambda mn, mx: mn >= 15.0,
        "is_at_most_42": lambda mn, mx: mx <= 42.0,
    },
    cast=float,
)
_checks.append(temperature_checks)


stress_checks = _make_range_check(
    ["gadgetbridge", "bronze", "huami_stress_sample"],
    "huami_stress_sample_stress_checks",
    "STRESS",
    {
        "is_at_least_1": lambda mn, mx: mn >= 1,
        "is_at_most_100": lambda mn, mx: mx <= 100,
    },
)
_checks.append(stress_checks)


hrv_checks = _make_range_check(
    ["gadgetbridge", "bronze", "generic_hrv_value_sample"],
    "generic_hrv_value_sample_hrv_checks",
    "VALUE",
    {
        "is_positive": lambda mn, mx: mn > 0,
        "is_at_most_300": lambda mn, mx: mx <= 300,
    },
)
_checks.append(hrv_checks)


respiratory_rate_checks = _make_range_check(
    ["gadgetbridge", "bronze", "huami_sleep_respiratory_rate_sample"],
    "huami_sleep_respiratory_rate_sample_rate_checks",
    "RATE",
    {
        "is_at_least_4": lambda mn, mx: mn >= 4,
        "is_at_most_60": lambda mn, mx: mx <= 60,
    },
)
_checks.append(respiratory_rate_checks)


defs = Definitions(
    assets=_tables,
    asset_checks=_checks
)
