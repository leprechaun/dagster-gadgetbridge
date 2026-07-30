from dagster import AssetSelection

from gadgetbridge_pipeline.defs.gadgetbridge.sensor import s3_sensor


def test_s3_sensor_wiring():
    assert s3_sensor.name == "s3_sqlite_sensor"
    assert s3_sensor.minimum_interval_seconds == 300
    assert s3_sensor.asset_selection == AssetSelection.groups("gadgetbridge")
