import datetime
from dataclasses import dataclass
from enum import IntEnum


class SleepStage(IntEnum):
    REM = 8
    LIGHT = 4
    DEEP = 5
    AWAKE = 7

    def __str__(self) -> str:
        return self.name

@dataclass
class SleepPeriod:
    start: datetime.datetime
    duration: int
    kind: SleepStage

class SleepSession:
    def __init__(self, bytes):
        self.bytes = bytes

    def _to_uint32(self, buf, offset):
        return int.from_bytes(buf[offset:offset + 4], byteorder='little', signed=False)

    def _to_uint16(self, buf, offset):
        return int.from_bytes(buf[offset:offset + 2], byteorder='little', signed=False)

    def _to_unsigned(self, buf, offset):
        return buf[offset] & 0xFF

    @property
    def session(self):
        return self._to_uint32(self.bytes, 0x00)

    @property
    def midnight(self):
        return self._to_uint32(self.bytes, 0x04)

    @property
    def start(self):
        return self._to_uint16(self.bytes, 0x0a)

    @property
    def end(self):
        return self._to_uint16(self.bytes, 0x0c)

    @property
    def stage_count(self):
        return self._to_unsigned(self.bytes, 0x54)

    @property
    def score(self):
        return self._to_unsigned(self.bytes, 0x16)

    @property
    def periods(self):
        # stageStart = sleepSession.timestampMidnight - 24 * 3600 + stage.start * 60
        for idx in range(0, self.stage_count):
            start = self._to_uint16(self.bytes, 0x56 + (5 * idx) + 0)
            end = self._to_uint16(self.bytes, 0x56 + (5 * idx) + 2)
            kind = self._to_unsigned(self.bytes, 0x56 + (5 * idx) + 4)

            actual_start = datetime.datetime.fromtimestamp(
                self.midnight - (24 * 3600) + (start * 60),
                tz=datetime.timezone.utc
            )

            duration = end - start

            yield SleepPeriod(
                actual_start,
                datetime.timedelta(minutes=duration),
                SleepStage(kind)
            )

    @property
    def duration(self):
        durations = [s.duration for s in self.periods]
        return sum(durations, datetime.timedelta())
