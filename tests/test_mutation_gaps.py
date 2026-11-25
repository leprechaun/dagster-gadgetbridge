"""Tests added to kill surviving mutants found by `mutmut` (see [tool.mutmut]).

Each section pins behaviour that earlier tests only exercised indirectly.
"""

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from gadgetbridge_pipeline.defs.gadgetbridge import bronze as gb_bronze
from gadgetbridge_pipeline.defs.gadgetbridge.sleep_session import SleepSession
from gadgetbridge_pipeline.defs.medicine import bronze as medicine_bronze
from gadgetbridge_pipeline.defs.medicine.bronze import _EMPTY_SCHEMA, build_medicine_log
from gadgetbridge_pipeline.defs.owntracks.bronze import parse_rec_lines
from gadgetbridge_pipeline.defs.owntracks.silver import _haversine_distance_m
from gadgetbridge_pipeline.defs.poi.bronze import (
    find_same_kind_overlaps,
    parse_poi_feature,
    parse_poi_geojson,
)
from gadgetbridge_pipeline.defs.s3_watch import evaluate_object_change

# --- haversine: an actual distance, not just inside/outside a radius ---------


def _distance(lat1, lon1, lat2, lon2) -> float:
    df = pl.DataFrame({"a": [lat1], "b": [lon1], "c": [lat2], "d": [lon2]})
    return df.select(
        _haversine_distance_m(pl.col("a"), pl.col("b"), pl.col("c"), pl.col("d"))
    ).item()


def test_haversine_one_degree_of_latitude_is_about_111_2_km():
    assert _distance(0, 0, 1, 0) == pytest.approx(111_195, rel=1e-3)


def test_haversine_one_degree_of_longitude_shrinks_with_latitude():
    assert _distance(0, 0, 0, 1) == pytest.approx(111_195, rel=1e-3)
    assert _distance(60, 0, 60, 1) == pytest.approx(111_195 / 2, rel=2e-3)


def test_haversine_is_symmetric_and_zero_for_same_point():
    assert _distance(13.7, 100.5, 13.7, 100.5) == 0
    assert _distance(13.7, 100.5, 14.2, 101.1) == pytest.approx(
        _distance(14.2, 101.1, 13.7, 100.5)
    )


def test_haversine_half_the_globe_is_pi_r():
    assert _distance(0, 0, 0, 180) == pytest.approx(3.14159265 * 6_371_000, rel=1e-6)


# --- medicine _today: Bangkok, not the system clock --------------------------


def test_today_uses_bangkok_not_the_system_clock(monkeypatch):
    # 17:30 UTC on the 5th is already the 6th in Bangkok (UTC+7)
    instant = datetime(2026, 7, 5, 17, 30, tzinfo=timezone.utc)

    class _FrozenDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            assert tz is not None, "must be asked for a specific timezone"
            return instant.astimezone(tz)

    monkeypatch.setattr(medicine_bronze, "datetime", _FrozenDatetime)
    assert medicine_bronze._today() == date(2026, 7, 6)


# --- medicine log: column names and empty-result schema ----------------------


def _rx(start, end, name="Tylenol", mg=500.0):
    return pl.DataFrame({
        "start_date": [start], "end_date": [end], "medicine": [name], "dosage_mg": [mg],
    })


def test_medicine_log_columns():
    log = build_medicine_log(
        _rx(date(2026, 7, 1), date(2026, 7, 2)),
        pl.DataFrame({"date": pl.Series([], dtype=pl.Date)}),
        today=date(2026, 7, 6),
    )
    assert log.columns == ["date", "medicine", "dosage_mg", "taken", "effective_dosage"]
    assert log["medicine"].to_list() == ["Tylenol", "Tylenol"]


def test_empty_medicine_log_keeps_its_schema():
    empty = _rx(date(2026, 7, 1), None).clear()
    log = build_medicine_log(
        empty, pl.DataFrame({"date": pl.Series([], dtype=pl.Date)}), today=date(2026, 7, 6)
    )
    assert log.schema == pl.Schema(_EMPTY_SCHEMA)


# --- find_same_kind_overlaps -------------------------------------------------


def _rects(*rows):
    return pl.DataFrame(
        [dict(zip(("name", "kind", "lon_min", "lon_max", "lat_min", "lat_max"), r, strict=True))
         for r in rows]
    )


def test_overlap_requires_both_axes_to_overlap():
    # overlap in longitude only
    assert find_same_kind_overlaps(
        _rects(("a", "area", 0, 2, 0, 1), ("b", "area", 1, 3, 5, 6))
    ) == []
    # overlap in latitude only
    assert find_same_kind_overlaps(
        _rects(("a", "area", 0, 1, 0, 2), ("b", "area", 5, 6, 1, 3))
    ) == []


def test_overlap_touching_edges_is_not_an_overlap():
    assert find_same_kind_overlaps(
        _rects(("a", "area", 0, 1, 0, 1), ("b", "area", 1, 2, 0, 1))
    ) == []
    assert find_same_kind_overlaps(
        _rects(("a", "area", 0, 1, 0, 1), ("b", "area", 0, 1, 1, 2))
    ) == []


def test_overlap_pair_is_sorted_and_found_in_either_order():
    assert find_same_kind_overlaps(
        _rects(("z", "area", 0, 2, 0, 2), ("a", "area", 1, 3, 1, 3))
    ) == [("a", "z")]


def test_overlap_skips_a_different_kind_pair_without_stopping():
    # the first pair is a different kind; later pairs must still be examined
    rects = _rects(
        ("big", "region", 0, 10, 0, 10),
        ("x", "area", 1, 3, 1, 3),
        ("y", "area", 2, 4, 2, 4),
    )
    assert find_same_kind_overlaps(rects) == [("x", "y")]


def test_overlap_finds_every_pair_among_three():
    rects = _rects(
        ("a", "area", 0, 3, 0, 3), ("b", "area", 1, 4, 1, 4), ("c", "area", 2, 5, 2, 5)
    )
    assert sorted(find_same_kind_overlaps(rects)) == [("a", "b"), ("a", "c"), ("b", "c")]


# --- POI parsing: field names ------------------------------------------------


def test_circle_feature_fields():
    feature = {
        "properties": {"name": "home", "kind": "point-of-interest", "radius_m": 50},
        "geometry": {"type": "Point", "coordinates": [100.5, 13.7]},
    }
    assert parse_poi_feature(feature) == {
        "name": "home", "kind": "point-of-interest", "geometry_type": "circle",
        "lat": 13.7, "lon": 100.5, "radius_m": 50,
        "lon_min": None, "lon_max": None, "lat_min": None, "lat_max": None,
    }


def test_rectangle_feature_fields():
    ring = [[1, 10], [3, 10], [3, 20], [1, 20], [1, 10]]
    feature = {
        "properties": {"name": "box", "kind": "area"},
        "geometry": {"type": "Polygon", "coordinates": [ring]},
    }
    assert parse_poi_feature(feature) == {
        "name": "box", "kind": "area", "geometry_type": "rectangle",
        "lat": None, "lon": None, "radius_m": None,
        "lon_min": 1, "lon_max": 3, "lat_min": 10, "lat_max": 20,
    }


def test_geojson_reports_dropped_names_and_keeps_going():
    ring = [[1, 10], [3, 10], [3, 20], [1, 20], [1, 10]]
    raw = json.dumps({"features": [
        {"properties": {"name": "bad"}, "geometry": {"type": "LineString"}},
        {"properties": {}, "geometry": {"type": "LineString"}},
        {"geometry": {"type": "LineString"}},
        {"properties": {"name": "box", "kind": "area"},
         "geometry": {"type": "Polygon", "coordinates": [ring]}},
    ]}).encode()
    records, dropped = parse_poi_geojson(raw)
    assert [r["name"] for r in records] == ["box"]
    assert dropped == ["bad", "<unnamed>", "<unnamed>"]


def test_geojson_without_features_is_empty():
    assert parse_poi_geojson(b"{}") == ([], [])


# --- OwnTracks .rec parsing: field mapping -----------------------------------


def test_every_payload_field_is_mapped():
    payload = {
        "_type": "location", "_id": "abc", "tst": 1, "created_at": 2, "lat": 3.5, "lon": 4.5,
        "alt": 5, "acc": 6, "vac": 7, "batt": 8, "bs": 1, "conn": "w", "SSID": "net",
        "BSSID": "aa:bb", "tid": "ab", "source": "gps", "m": 2,
    }
    line = f"2026-07-01T00:00:00Z\t*\t{json.dumps(payload)}"
    (rec,), dropped = parse_rec_lines([line], user="u", device="d")
    assert dropped == []
    assert rec == {
        "id": "abc", "user": "u", "device": "d",
        "arrived_at": datetime(2026, 7, 1, tzinfo=timezone.utc),
        "tst": 1, "created_at": 2, "lat": 3.5, "lon": 4.5, "alt": 5, "acc": 6, "vac": 7,
        "batt": 8, "bs": 1, "conn": "w", "ssid": "net", "bssid": "aa:bb", "tid": "ab",
        "source": "gps", "m": 2,
    }


def test_missing_payload_fields_are_none():
    line = '2026-07-01T00:00:00Z\t*\t{"_type": "location"}'
    (rec,), _ = parse_rec_lines([line], user="u", device="d")
    assert all(v is None for k, v in rec.items() if k not in {"user", "device", "arrived_at"})


def test_json_payload_may_contain_a_raw_tab():
    line = '2026-07-01T00:00:00Z\t*\t{"_type": "location",\t"tst": 7}'
    (rec,), _ = parse_rec_lines([line], user="u", device="d")
    assert rec["tst"] == 7


def test_line_with_too_few_fields_is_skipped():
    assert parse_rec_lines(["2026-07-01T00:00:00Z\t*"], "u", "d") == ([], [])


def test_unparseable_timestamp_is_dropped_and_parsing_continues():
    bad = 'not-a-date\x00\t*\t{"_type": "location", "tst": 1}'
    good = '2026-07-01T00:00:00Z\t*\t{"_type": "location", "tst": 2}'
    records, dropped = parse_rec_lines([bad, good], "u", "d")
    assert [r["tst"] for r in records] == [2]
    assert len(dropped) == 1
    assert "not-a-date" in dropped[0]


def test_nul_padding_around_timestamp_is_stripped():
    line = '\x002026-07-01T00:00:00Z\x00\t*\t{"_type": "location", "tst": 1}'
    (rec,), dropped = parse_rec_lines([line], "u", "d")
    assert dropped == []
    assert rec["arrived_at"] == datetime(2026, 7, 1, tzinfo=timezone.utc)


def test_surrounding_whitespace_and_blank_lines_are_ignored():
    line = '  2026-07-01T00:00:00Z\t*\t{"_type": "location", "tst": 1}  \r\n'
    records, _ = parse_rec_lines(["", "   \n", line], "u", "d")
    assert len(records) == 1


# --- evaluate_object_change: the reason the sensor reports -------------------


def test_skip_reason_names_the_watched_object():
    result = evaluate_object_change("poi.geojson", {"k": "e1"}, {"etags": {"k": "e1"}})
    assert result["action"] == "skip"
    assert "poi.geojson" in result["reason"]


# --- SleepSession byte reading -----------------------------------------------


def test_uint32_reads_exactly_four_bytes_unsigned():
    buf = bytearray(0x60)
    buf[0x00:0x04] = b"\x00\x00\x00\x00"
    buf[0x04:0x08] = b"\xff\xff\xff\xff"  # neighbouring field must not leak in
    s = SleepSession(bytes(buf))
    assert s.session == 0
    assert s.midnight == 0xFFFFFFFF


def test_uint16_reads_exactly_two_bytes_unsigned():
    buf = bytearray(0x60)
    buf[0x0A:0x0C] = b"\xff\xff"
    buf[0x0C:0x0E] = b"\x00\x00"
    s = SleepSession(bytes(buf))
    assert s.start == 0xFFFF
    assert s.end == 0


def test_uint16_ignores_the_byte_after_it():
    buf = bytearray(0x60)
    buf[0x0C] = 0x01  # `end`'s low byte sits right after `start`
    assert SleepSession(bytes(buf)).start == 0


def test_single_byte_fields_are_unsigned():
    buf = bytearray(0x60)
    buf[0x16] = 0xFF
    buf[0x54] = 0x80
    s = SleepSession(bytes(buf))
    assert s.score == 255
    assert s.stage_count == 128


# --- gadgetbridge bronze registry: every table gets the right checks ---------

_RANGE_TABLES = {
    "huami_extended_activity_sample", "battery_level", "huami_spo2_sample",
    "generic_temperature_sample", "huami_stress_sample", "generic_hrv_value_sample",
    "huami_sleep_respiratory_rate_sample",
}


def _check_names(table):
    _, checks = gb_bronze._make_bronze(table, gb_bronze._TABLES[table])
    return [spec.name for c in checks for spec in c.check_specs]


def test_exactly_the_expected_tables_declare_a_range_check():
    assert {t for t, s in gb_bronze._TABLES.items() if "range" in s} == _RANGE_TABLES


@pytest.mark.parametrize("table", sorted(gb_bronze._TABLES))
def test_each_table_gets_its_declared_checks(table):
    settings = gb_bronze._TABLES[table]
    names = _check_names(table)
    expected = int("schema" in settings) + 1 + int("range" in settings)
    assert len(names) == expected == len(set(names))
    if "range" in settings:
        assert settings["range"]["name"] in names
        spec = next(iter(gb_bronze.range_check_for(table).check_specs))
        assert spec.name == settings["range"]["name"]


def test_every_table_check_is_registered_once_and_blocking():
    registered = [spec.name for c in gb_bronze._checks for spec in c.check_specs]
    assert len(registered) == len(set(registered))
    assert len(registered) == sum(len(_check_names(t)) for t in gb_bronze._TABLES)
    assert len(gb_bronze._tables) == len(gb_bronze._TABLES)
    assert all(spec.blocking for c in gb_bronze._checks for spec in c.check_specs)
