from dagster import AssetKey, AssetSelection

from gadgetbridge_pipeline.defs.sensors.waypoints_s3_sensor import waypoints_s3_sensor


def test_waypoints_s3_sensor_wiring():
    assert waypoints_s3_sensor.name == "waypoints_s3_sensor"
    assert waypoints_s3_sensor.minimum_interval_seconds == 300
    assert waypoints_s3_sensor.asset_selection == AssetSelection.assets(
        AssetKey(["owntracks", "bronze", "waypoints"])
    )
