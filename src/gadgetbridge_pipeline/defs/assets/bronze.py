from datetime import datetime, timezone
from typing import Any, Dict

import polars as pl
import dagster as dg

from dagster import AutomationCondition, Definitions, AssetExecutionContext, AssetCheckResult

MIN_TIMESTAMP = datetime(2026, 1, 1, tzinfo=timezone.utc)

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

        try:
            context.log.info("max TS=%s" % transformed.select(pl.col("TIMESTAMP").max()))
        except: # noqa: E722
            context.log.info("max TS=blew-up")

        return transformed

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


def _make_timestamp_check(table_name: str):

    @dg.asset_check(
        asset=dg.AssetKey(["gadgetbridge", "bronze", table_name]),
        blocking=True,
        name="%s_timestamp_checks" % table_name
    )
    def _asset_check(df: pl.DataFrame) -> AssetCheckResult:
        null_count = int(df["TIMESTAMP"].null_count())
        minimum = df["TIMESTAMP"].min()

        checks = {
            "no_nulls": null_count == 0,
            "is_after_min_timestamp": minimum is not None and minimum >= MIN_TIMESTAMP,
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


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"]),
    blocking=True,
    name="huami_extended_activity_sample_heartrate_checks"
)
def activity_heartrate_checks(huami_extended_activity_sample: pl.DataFrame) -> AssetCheckResult:
    act = huami_extended_activity_sample

    minimum = int(act['HEART_RATE'].min())
    maximum = int(act['HEART_RATE'].max())

    checks = {
        "is_positive": minimum > 0,
        "is_255 or below": maximum <= 255
    }

    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(activity_heartrate_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "battery_level"]),
    blocking=True,
    name="battery_level_range_checks",
)
def battery_level_checks(battery_level: pl.DataFrame) -> AssetCheckResult:
    minimum = int(battery_level["LEVEL"].min())
    maximum = int(battery_level["LEVEL"].max())
    checks = {
        "is_non_negative": minimum >= 0,
        "is_at_most_100": maximum <= 100,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(battery_level_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "huami_spo2_sample"]),
    blocking=True,
    name="huami_spo2_sample_spo2_checks",
)
def spo2_checks(huami_spo2_sample: pl.DataFrame) -> AssetCheckResult:
    minimum = int(huami_spo2_sample["SPO2"].min())
    maximum = int(huami_spo2_sample["SPO2"].max())
    checks = {
        "is_at_least_70": minimum >= 70,
        "is_at_most_100": maximum <= 100,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(spo2_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "generic_temperature_sample"]),
    blocking=True,
    name="generic_temperature_sample_temperature_checks",
)
def temperature_checks(generic_temperature_sample: pl.DataFrame) -> AssetCheckResult:
    minimum = float(generic_temperature_sample["TEMPERATURE"].min())
    maximum = float(generic_temperature_sample["TEMPERATURE"].max())
    checks = {
        "is_at_least_15": minimum >= 15.0,
        "is_at_most_42": maximum <= 42.0,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(temperature_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"]),
    blocking=True,
    name="huami_stress_sample_stress_checks",
)
def stress_checks(huami_stress_sample: pl.DataFrame) -> AssetCheckResult:
    minimum = int(huami_stress_sample["STRESS"].min())
    maximum = int(huami_stress_sample["STRESS"].max())
    checks = {
        "is_at_least_1": minimum >= 1,
        "is_at_most_100": maximum <= 100,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(stress_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "generic_hrv_value_sample"]),
    blocking=True,
    name="generic_hrv_value_sample_hrv_checks",
)
def hrv_checks(generic_hrv_value_sample: pl.DataFrame) -> AssetCheckResult:
    minimum = int(generic_hrv_value_sample["VALUE"].min())
    maximum = int(generic_hrv_value_sample["VALUE"].max())
    checks = {
        "is_positive": minimum > 0,
        "is_at_most_300": maximum <= 300,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(hrv_checks)


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "bronze", "huami_sleep_respiratory_rate_sample"]),
    blocking=True,
    name="huami_sleep_respiratory_rate_sample_rate_checks",
)
def respiratory_rate_checks(huami_sleep_respiratory_rate_sample: pl.DataFrame) -> AssetCheckResult:
    minimum = int(huami_sleep_respiratory_rate_sample["RATE"].min())
    maximum = int(huami_sleep_respiratory_rate_sample["RATE"].max())
    checks = {
        "is_at_least_4": minimum >= 4,
        "is_at_most_60": maximum <= 60,
    }
    return AssetCheckResult(
        passed=all(checks.values()),
        metadata=checks | {"minimum": minimum, "maximum": maximum},
    )

_checks.append(respiratory_rate_checks)


defs = Definitions(
    assets=_tables,
    asset_checks=_checks
)
