import polars as pl
from datetime import datetime
from gadgetbridge_pipeline.defs.assets.gold import daily_health_snapshot


def test_daily_health_snapshot_joins_and_averages():
    ts = lambda d: datetime.fromisoformat(d)

    hrv = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 08:00"), ts("2024-01-01 20:00")], "VALUE": [40.0, 60.0]})
    spo2 = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 09:00")], "SPO2": [97.0]})
    stress = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 10:00")], "STRESS": [30.0]})
    temperature = pl.DataFrame({"TIMESTAMP": [ts("2024-01-01 11:00")], "TEMPERATURE": [36.6]})

    result = daily_health_snapshot(hrv=hrv, spo2=spo2, stress=stress, temperature=temperature)

    assert result.shape[0] == 1
    assert result["avg_hrv"][0] == 50.0
    assert result["avg_spo2"][0] == 97.0
