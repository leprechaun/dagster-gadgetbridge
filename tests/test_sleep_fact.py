import struct
from datetime import datetime, timezone

import polars as pl

from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64
from gadgetbridge_pipeline.defs.gadgetbridge.sleep_fact import (
    build_fact_sleep_session,
    parse_sleep_sessions,
)

_UTC = timezone.utc


def _blob(midnight: datetime, start_min: int, end_min: int, score=80, stage_count=5) -> bytes:
    """A synthetic 594-byte Huami session blob with only the parsed fields set."""
    b = bytearray(594)
    struct.pack_into("<I", b, 0x00, 1)
    struct.pack_into("<I", b, 0x04, int(midnight.timestamp()))
    struct.pack_into("<H", b, 0x0A, start_min)
    struct.pack_into("<H", b, 0x0C, end_min)
    b[0x16] = score
    b[0x54] = stage_count
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
