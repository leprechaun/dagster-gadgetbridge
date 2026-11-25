from __future__ import annotations

from datetime import date
from itertools import pairwise
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import polars as pl

from gadgetbridge_pipeline.defs.conformed.constants import DEFAULT_TIMEZONE, TIMEZONE_EPOCH

WHOLE_DAY_KEY = -1


def build_dim_date(start: date, end: date) -> pl.DataFrame:
    """One row per Local Date, inclusive of both ends."""
    return (
        pl.DataFrame({"date_key": pl.date_range(start, end, "1d", eager=True)})
        .with_columns(
            pl.col("date_key").dt.year().alias("year"),
            pl.col("date_key").dt.month().alias("month"),
            pl.col("date_key").dt.week().alias("iso_week"),
            pl.col("date_key").dt.weekday().alias("weekday"),
        )
        .with_columns((pl.col("weekday") >= 6).alias("is_weekend"))
    )


def build_dim_time_of_day() -> pl.DataFrame:
    """1,440 minute rows plus the `Whole day` member. The label is a string
    because Delta cannot store a time type.
    """
    minutes = pl.DataFrame({"time_key": pl.int_range(0, 1440, eager=True)}).with_columns(
        (pl.col("time_key") // 60).alias("hour"),
        (pl.col("time_key") % 60).alias("minute"),
    ).with_columns(
        pl.format("{}:{}", pl.col("hour").cast(pl.String).str.zfill(2),
                  pl.col("minute").cast(pl.String).str.zfill(2)).alias("label")
    )
    whole_day = pl.DataFrame(
        {"time_key": [WHOLE_DAY_KEY], "hour": [None], "minute": [None], "label": ["Whole day"]},
        schema=minutes.schema,
    )
    return pl.concat([minutes, whole_day]).select("time_key", "label", "hour", "minute")


def build_person_timezone(persons: pl.DataFrame) -> pl.DataFrame:
    """One open-ended row per Person, in the default reporting timezone."""
    return persons.select(
        pl.col("person_id").alias("person_key"),
        pl.lit(DEFAULT_TIMEZONE).alias("timezone"),
        pl.lit(TIMEZONE_EPOCH, dtype=pl.Datetime("us", "UTC")).alias("valid_from"),
        pl.lit(None, dtype=pl.Datetime("us", "UTC")).alias("valid_to"),
    )


def find_persons_without_one_covering_timezone(
    person_keys: list[str], person_timezone: pl.DataFrame
) -> list[str]:
    """Persons for whom some instant has no timezone row or more than one:
    their rows must start at the epoch, abut exactly, and end open.
    """
    bad = []
    for person in person_keys:
        rows = list(
            person_timezone.filter(pl.col("person_key") == person)
            .sort("valid_from")
            .select("valid_from", "valid_to")
            .iter_rows()
        )
        ok = (
            bool(rows)
            and rows[0][0] <= TIMEZONE_EPOCH
            and rows[-1][1] is None
            and all(prev[1] == nxt[0] for prev, nxt in pairwise(rows))
        )
        if not ok:
            bad.append(person)
    return sorted(bad)


def find_invalid_timezones(person_timezone: pl.DataFrame) -> list[str]:
    bad = set()
    for name in person_timezone["timezone"].unique().to_list():
        try:
            ZoneInfo(name)
        except (ZoneInfoNotFoundError, ValueError, TypeError):
            bad.add(name)
    return sorted(bad)
