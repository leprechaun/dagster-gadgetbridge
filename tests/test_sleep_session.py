import datetime

from gadgetbridge_pipeline.defs.gadgetbridge.sleep_session import (
    SleepPeriod,
    SleepSession,
    SleepStage,
)


def _u32(v):
    return v.to_bytes(4, byteorder="little", signed=False)


def _u16(v):
    return v.to_bytes(2, byteorder="little", signed=False)


def _u8(v):
    return v.to_bytes(1, byteorder="little", signed=False)


def _sleep_session_bytes(session=0, midnight=0, start=0, end=0, score=0, periods=()):
    # Mirrors the Gadgetbridge binary layout documented in sleep_session.py's
    # offset constants (0x00 session, 0x04 midnight, 0x0a start, 0x0c end,
    # 0x16 score, 0x54 stage_count, 0x56+ per-stage [start, end, kind]).
    buf = bytearray(max(0x56 + 5 * len(periods), 0x56))
    buf[0x00:0x04] = _u32(session)
    buf[0x04:0x08] = _u32(midnight)
    buf[0x0a:0x0c] = _u16(start)
    buf[0x0c:0x0e] = _u16(end)
    buf[0x16:0x17] = _u8(score)
    buf[0x54:0x55] = _u8(len(periods))
    for idx, (p_start, p_end, kind) in enumerate(periods):
        offset = 0x56 + 5 * idx
        buf[offset:offset + 2] = _u16(p_start)
        buf[offset + 2:offset + 4] = _u16(p_end)
        buf[offset + 4:offset + 5] = _u8(kind)
    return bytes(buf)


def test_scalar_fields_are_parsed_from_their_byte_offsets():
    data = _sleep_session_bytes(session=42, midnight=1_700_000_000, start=15, end=495, score=87)
    session = SleepSession(data)

    assert session.session == 42
    assert session.midnight == 1_700_000_000
    assert session.start == 15
    assert session.end == 495
    assert session.score == 87
    assert session.stage_count == 0


def test_uint16_fields_handle_values_above_255():
    # start/end are minutes-since-midnight and can exceed a single byte
    data = _sleep_session_bytes(start=600, end=900)
    session = SleepSession(data)

    assert session.start == 600
    assert session.end == 900


def test_no_periods_yields_empty_iterator_and_zero_duration():
    session = SleepSession(_sleep_session_bytes(periods=()))

    assert list(session.periods) == []
    assert session.duration == datetime.timedelta()


def test_periods_are_decoded_with_correct_stage_and_timing():
    midnight = 1_700_000_000  # arbitrary unix timestamp used as the session's "midnight" anchor
    data = _sleep_session_bytes(
        midnight=midnight,
        periods=[
            (0, 90, SleepStage.LIGHT),
            (90, 150, SleepStage.DEEP),
            (150, 210, SleepStage.REM),
        ],
    )
    session = SleepSession(data)
    periods = list(session.periods)

    assert len(periods) == 3
    assert [p.kind for p in periods] == [SleepStage.LIGHT, SleepStage.DEEP, SleepStage.REM]
    assert [p.duration for p in periods] == [
        datetime.timedelta(minutes=90),
        datetime.timedelta(minutes=60),
        datetime.timedelta(minutes=60),
    ]

    expected_first_start = datetime.datetime.fromtimestamp(
        midnight - 24 * 3600, tz=datetime.timezone.utc
    )
    assert periods[0].start == expected_first_start
    assert periods[1].start == expected_first_start + datetime.timedelta(minutes=90)


def test_period_start_is_relative_to_previous_midnight_not_session_midnight():
    # sleep_session.py's own comment: stageStart = midnight - 24h + stage.start * 60
    midnight = 1_700_000_000
    data = _sleep_session_bytes(midnight=midnight, periods=[(0, 10, SleepStage.AWAKE)])
    session = SleepSession(data)

    [period] = list(session.periods)
    assert period.start == datetime.datetime.fromtimestamp(
        midnight - 24 * 3600, tz=datetime.timezone.utc
    )


def test_duration_sums_across_all_periods():
    data = _sleep_session_bytes(
        periods=[
            (0, 60, SleepStage.LIGHT),
            (60, 75, SleepStage.AWAKE),
            (75, 200, SleepStage.DEEP),
        ]
    )
    session = SleepSession(data)

    assert session.duration == datetime.timedelta(minutes=(60 + 15 + 125))


def test_periods_returns_sleep_period_dataclass_instances():
    data = _sleep_session_bytes(periods=[(0, 30, SleepStage.REM)])
    session = SleepSession(data)

    [period] = list(session.periods)
    assert isinstance(period, SleepPeriod)


def test_sleep_stage_str_is_the_enum_name():
    assert str(SleepStage.REM) == "REM"
    assert str(SleepStage.LIGHT) == "LIGHT"
    assert str(SleepStage.DEEP) == "DEEP"
    assert str(SleepStage.AWAKE) == "AWAKE"
