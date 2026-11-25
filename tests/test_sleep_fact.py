import struct
from datetime import datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64
from gadgetbridge_pipeline.defs.gadgetbridge.sleep_fact import (
    build_dim_sleep_stage,
    build_fact_sleep_session,
    build_fact_sleep_stage,
    find_stage_date_mismatches,
    parse_sleep_sessions,
    parse_sleep_stages,
)

_UTC = timezone.utc


def _blob(
    midnight: datetime, start_min: int, end_min: int, score=80, stage_count=5, stages=()
) -> bytes:
    """A synthetic 594-byte Huami session blob with only the parsed fields set."""
    b = bytearray(594)
    struct.pack_into("<I", b, 0x00, 1)
    struct.pack_into("<I", b, 0x04, int(midnight.timestamp()))
    struct.pack_into("<H", b, 0x0A, start_min)
    struct.pack_into("<H", b, 0x0C, end_min)
    b[0x16] = score
    b[0x54] = len(stages) or stage_count
    for i, (stage_start, stage_end, kind) in enumerate(stages):
        struct.pack_into("<HH", b, 0x56 + 5 * i, stage_start, stage_end)
        b[0x56 + 5 * i + 4] = kind
    return bytes(b)


def _bronze(*rows):
    """rows: (device_id, data)"""
    return pl.DataFrame(
        {
            "TIMESTAMP": [datetime(2000, 1, 2, 8, tzinfo=_UTC)] * len(rows),
            "DEVICE_ID": [r[0] for r in rows],
            "USER_ID": [1] * len(rows),
            "DATA": [r[1] for r in rows],
        },
        schema={"TIMESTAMP": pl.Datetime("us", "UTC"), "DEVICE_ID": pl.Int64,
                "USER_ID": pl.Int64, "DATA": pl.Binary},
    )


_MIDNIGHT = datetime(2000, 1, 2, tzinfo=_UTC)
_START = datetime(2000, 1, 1, 22, tzinfo=_UTC)  # session start for _MIDNIGHT with start=22:00


def test_a_session_blob_becomes_absolute_utc_start_and_end_measured_from_the_day_before():
    # start/end are minutes after the midnight *before* the reference midnight.
    bronze = _bronze((1, _blob(_MIDNIGHT, 22 * 60, 31 * 60, score=77, stage_count=9)))

    out = parse_sleep_sessions(bronze).row(0, named=True)

    assert out["device_id"] == 1
    assert out["start_utc"] == datetime(2000, 1, 1, 22, tzinfo=_UTC)
    assert out["end_utc"] == datetime(2000, 1, 2, 7, tzinfo=_UTC)
    assert (out["score"], out["stage_count"]) == (77, 9)


def test_sessions_with_no_stages_or_no_start_are_dropped_as_garbage():
    bronze = _bronze(
        (1, _blob(_MIDNIGHT, 22 * 60, 31 * 60)),
        (1, _blob(_MIDNIGHT, 22 * 60, 31 * 60, stage_count=0)),
        (1, _blob(_MIDNIGHT, 0, 31 * 60)),
    )

    assert parse_sleep_sessions(bronze).height == 1


# build_fact_sleep_session

_EPOCH = datetime(1970, 1, 1, tzinfo=_UTC)
_DEVICES = pl.DataFrame(
    [("1@2000-01-01", "person-a", "1", datetime(2000, 1, 1).date(), None)],
    schema={"device_key": pl.String, "person_key": pl.String, "gadgetbridge_device_id": pl.String,
            "valid_from": pl.Date, "valid_to": pl.Date},
    orient="row",
)
_TZ = pl.DataFrame(
    [("person-a", "UTC", _EPOCH, None)],
    schema={"person_key": pl.String, "timezone": pl.String,
            "valid_from": pl.Datetime("us", "UTC"), "valid_to": pl.Datetime("us", "UTC")},
    orient="row",
)


def _parsed(*rows):
    """rows: (device_id, start, end) as datetimes"""
    return pl.DataFrame(
        [(d, s, e, 80, 5) for d, s, e in rows],
        schema=parse_sleep_sessions(_bronze()).schema,
        orient="row",
    )


def test_an_overnight_session_gets_keys_reporting_date_and_signed_minutes_from_reporting_midnight():
    start = datetime(2000, 1, 1, 22, 0, tzinfo=_UTC)
    end = datetime(2000, 1, 2, 7, 0, tzinfo=_UTC)

    fact = build_fact_sleep_session(_parsed((1, start, end)), _DEVICES, _TZ)

    assert fact.row(0, named=True) == {
        "session_key": stable_hash64("1@2000-01-01", "2000-01-01T22:00:00+00:00"),
        "start_date_key": datetime(2000, 1, 1).date(),
        "end_date_key": datetime(2000, 1, 2).date(),
        "reporting_date_key": datetime(2000, 1, 2).date(),
        "start_time_key": 22 * 60,
        "end_time_key": 7 * 60,
        "device_key": "1@2000-01-01",
        "person_key": "person-a",
        "is_nap": False,
        "start_utc": start,
        "end_utc": end,
        "start_minutes_from_reporting_midnight": -120,
        "end_minutes_from_reporting_midnight": 420,
        "score": 80,
        "stage_count": 5,
        "length_minutes": 540,
    }


def test_a_nap_is_dated_by_its_end_and_charted_at_positive_minutes_from_that_midnight():
    night = (1, datetime(2000, 1, 1, 22, 0, tzinfo=_UTC), datetime(2000, 1, 2, 7, 0, tzinfo=_UTC))
    nap = (1, datetime(2000, 1, 2, 16, 30, tzinfo=_UTC), datetime(2000, 1, 2, 17, 10, tzinfo=_UTC))

    next_night = (
        1, datetime(2000, 1, 2, 23, 0, tzinfo=_UTC), datetime(2000, 1, 3, 7, 0, tzinfo=_UTC)
    )

    fact = build_fact_sleep_session(_parsed(night, nap, next_night), _DEVICES, _TZ)

    nap_row = fact.sort("start_utc").row(1, named=True)
    assert nap_row["is_nap"] is True
    assert nap_row["reporting_date_key"] == datetime(2000, 1, 2).date()
    assert nap_row["start_minutes_from_reporting_midnight"] == 990


def test_a_session_from_an_unlisted_device_is_kept_with_unknown_keys():
    s = (99, datetime(2000, 1, 1, 22, 0, tzinfo=_UTC), datetime(2000, 1, 2, 7, 0, tzinfo=_UTC))

    fact = build_fact_sleep_session(_parsed(s), _DEVICES, _TZ)

    assert fact["device_key"].to_list() == ["(unknown)"]
    assert fact["person_key"].to_list() == ["(unknown)"]
    assert fact["reporting_date_key"].to_list() == [None]


# build_dim_sleep_stage

def test_sleep_stage_dimension_has_the_four_stages_keyed_by_name():
    assert sorted(build_dim_sleep_stage()["sleep_stage_key"].to_list()) == [
        "AWAKE", "DEEP", "LIGHT", "REM",
    ]


# parse_sleep_stages

def test_each_stage_is_decoded_with_its_session_start_utc_start_duration_and_name():
    # kinds: 4 LIGHT, 5 DEEP, 7 AWAKE, 8 REM
    blob = _blob(
        _MIDNIGHT, 22 * 60, 31 * 60,
        stages=[(22 * 60, 23 * 60 + 30, 4), (23 * 60 + 30, 25 * 60, 5)],
    )

    out = parse_sleep_stages(_bronze((1, blob)))

    assert out.select(
        "device_id", "session_start_utc", "start_utc", "duration_minutes", "sleep_stage_key"
    ).rows() == [
        (1, _START, datetime(2000, 1, 1, 22, tzinfo=_UTC), 90, "LIGHT"),
        (1, _START, datetime(2000, 1, 1, 23, 30, tzinfo=_UTC), 90, "DEEP"),
    ]


def test_sessions_with_no_stages_yield_no_stage_rows():
    out = parse_sleep_stages(_bronze((1, _blob(_MIDNIGHT, 22 * 60, 31 * 60, stage_count=0))))

    assert out.height == 0


# build_fact_sleep_stage

def _night_blob():
    return _blob(
        _MIDNIGHT, 22 * 60, 31 * 60,
        stages=[(22 * 60, 23 * 60 + 30, 4), (23 * 60 + 30, 25 * 60, 5)],
    )


def test_stage_rows_copy_their_sessions_key_reporting_date_and_person():
    bronze = _bronze((1, _night_blob()))
    session_fact = build_fact_sleep_session(parse_sleep_sessions(bronze), _DEVICES, _TZ)

    out = build_fact_sleep_stage(parse_sleep_stages(bronze), session_fact, _DEVICES)

    assert out.columns == [
        "session_key", "sleep_stage_key", "reporting_date_key", "person_key",
        "start_utc", "duration_minutes",
    ]
    assert out["session_key"].to_list() == session_fact["session_key"].to_list() * 2
    assert out["reporting_date_key"].to_list() == [datetime(2000, 1, 2).date()] * 2
    assert out["person_key"].to_list() == ["person-a"] * 2
    assert out["sleep_stage_key"].to_list() == ["LIGHT", "DEEP"]


def test_a_stage_whose_session_is_missing_from_the_session_fact_gets_null_date_and_unknown_person():
    bronze = _bronze((1, _night_blob()))
    empty_session_fact = build_fact_sleep_session(parse_sleep_sessions(_bronze()), _DEVICES, _TZ)

    out = build_fact_sleep_stage(parse_sleep_stages(bronze), empty_session_fact, _DEVICES)

    assert out["reporting_date_key"].to_list() == [None, None]
    assert out["person_key"].to_list() == ["(unknown)", "(unknown)"]


# find_stage_date_mismatches

def test_stage_rows_whose_reporting_date_differs_from_their_sessions_are_reported():
    d1, d2 = datetime(2000, 1, 1).date(), datetime(2000, 1, 2).date()
    sessions = pl.DataFrame({"session_key": [1, 2], "reporting_date_key": [d2, d2]})
    stages = pl.DataFrame({
        "session_key": [1, 1, 2, 3],
        "reporting_date_key": [d2, d1, d2, d2],
    })

    # session 1 has a stage on the wrong date; session 3 has no session row at all.
    assert find_stage_date_mismatches(stages, sessions) == [1, 3]
