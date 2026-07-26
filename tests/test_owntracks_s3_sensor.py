from dagster import AssetKey, AssetSelection

from gadgetbridge_pipeline.defs.sensors.owntracks_s3_sensor import (
    _month_from_key,
    _partition_key,
    owntracks_s3_sensor,
)


def test_month_from_key_extracts_year_month():
    assert _month_from_key("owntracks/raw/rec/alice/phone/2026-07.rec") == "2026-07"


def test_month_from_key_handles_bare_filename():
    assert _month_from_key("2026-01.rec") == "2026-01"


def test_partition_key_appends_day_one():
    assert _partition_key("2026-07") == "2026-07-01"


def test_owntracks_s3_sensor_wiring():
    assert owntracks_s3_sensor.name == "owntracks_s3_sensor"
    assert owntracks_s3_sensor.minimum_interval_seconds == 300
    assert owntracks_s3_sensor.asset_selection == AssetSelection.assets(
        AssetKey(["owntracks", "bronze", "location_records"])
    )
