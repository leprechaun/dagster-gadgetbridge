import json
from datetime import datetime, timezone
from gadgetbridge_pipeline.defs.assets.owntracks_bronze import parse_rec_lines


def _line(tst: int, lat: float, lon: float, arrived_at: str = "2026-07-01T00:00:00Z", **extra) -> str:
    payload = {"_type": "location", "tst": tst, "lat": lat, "lon": lon, **extra}
    return f"{arrived_at}\t*\t{json.dumps(payload)}"


def test_parse_basic_location():
    lines = [_line(1782872268, 13.726, 100.573, alt=116, acc=17)]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1
    r = records[0]
    assert r["user"] == "alice"
    assert r["device"] == "phone"
    assert r["tst"] == 1782872268
    assert r["lat"] == 13.726
    assert r["lon"] == 100.573
    assert r["alt"] == 116
    assert r["acc"] == 17


def test_parse_arrived_at():
    lines = [_line(1782872268, 1.0, 2.0, arrived_at="2026-07-01T02:17:48Z")]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert records[0]["arrived_at"] == datetime(2026, 7, 1, 2, 17, 48, tzinfo=timezone.utc)


def test_parse_id_and_created_at():
    lines = [_line(1782872268, 1.0, 2.0, _id="ea90db97", created_at=1782872100)]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    r = records[0]
    assert r["id"] == "ea90db97"
    assert r["created_at"] == 1782872100


def test_parse_skips_non_location_types():
    lines = [
        f"2026-07-01T00:00:00Z\t*\t{json.dumps({'_type': 'waypoint', 'tst': 1})}",
        _line(1782872268, 1.0, 2.0),
    ]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1


def test_parse_skips_malformed_json():
    lines = [
        "2026-07-01T00:00:00Z\t*\tnot-valid-json",
        _line(1782872268, 1.0, 2.0),
    ]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1


def test_parse_skips_blank_lines():
    lines = ["", "  ", _line(1782872268, 1.0, 2.0)]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1


def test_parse_skips_lines_without_tab_separator():
    lines = ["no-tabs-here", _line(1782872268, 1.0, 2.0)]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1


def test_parse_optional_fields_are_none_when_absent():
    lines = [_line(1782872268, 13.726, 100.573)]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    r = records[0]
    assert r["vac"] is None
    assert r["batt"] is None
    assert r["ssid"] is None
    assert r["bssid"] is None
    assert r["id"] is None
    assert r["created_at"] is None


def test_parse_wifi_fields():
    lines = [_line(1782872268, 13.726, 100.573, SSID="MyNet", BSSID="00:11:22:33:44:55")]
    records, _ = parse_rec_lines(lines, user="alice", device="phone")
    assert records[0]["ssid"] == "MyNet"
    assert records[0]["bssid"] == "00:11:22:33:44:55"


def test_parse_multiple_records():
    lines = [
        _line(1000, 1.0, 2.0),
        _line(2000, 3.0, 4.0),
        _line(3000, 5.0, 6.0),
    ]
    records, _ = parse_rec_lines(lines, user="bob", device="tablet")
    assert len(records) == 3
    assert [r["tst"] for r in records] == [1000, 2000, 3000]


def test_parse_drops_corrupted_arrived_at():
    # arrived_at is all null bytes — unrecoverable after stripping
    bad_line = f"\x00\x00\x00\t*\t{json.dumps({'_type': 'location', 'tst': 1, 'lat': 1.0, 'lon': 2.0})}"
    lines = [bad_line, _line(2000, 3.0, 4.0)]
    records, dropped = parse_rec_lines(lines, user="alice", device="phone")
    assert len(records) == 1
    assert len(dropped) == 1
