from dagster import AssetKey, AssetSelection

from gadgetbridge_pipeline.defs.sensors.medicine_s3_sensor import medicine_s3_sensor


def test_medicine_s3_sensor_wiring():
    assert medicine_s3_sensor.name == "medicine_s3_sensor"
    assert medicine_s3_sensor.minimum_interval_seconds == 300
    assert medicine_s3_sensor.asset_selection == AssetSelection.assets(
        AssetKey(["medicine", "bronze", "prescriptions"]),
        AssetKey(["medicine", "bronze", "medicine_skips"]),
    )
