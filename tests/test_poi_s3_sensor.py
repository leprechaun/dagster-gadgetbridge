from dagster import AssetKey, AssetSelection

from gadgetbridge_pipeline.defs.sensors.poi_s3_sensor import poi_s3_sensor


def test_poi_s3_sensor_wiring():
    assert poi_s3_sensor.name == "poi_s3_sensor"
    assert poi_s3_sensor.minimum_interval_seconds == 300
    assert poi_s3_sensor.asset_selection == AssetSelection.assets(
        AssetKey(["poi", "bronze", "points_of_interest"])
    )
