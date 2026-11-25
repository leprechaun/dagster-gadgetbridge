"""Inferred Sleep: contiguous asleep minutes from the activity fact (fallback only)."""

from __future__ import annotations

import polars as pl

from gadgetbridge_pipeline.defs.gadgetbridge.activity import ASLEEP_KIND
from gadgetbridge_pipeline.defs.gadgetbridge.night_rule import apply_night_rule, night_window_dates


def find_asleep_stretches(fact_activity_minute: pl.DataFrame) -> pl.DataFrame:
    """Contiguous runs of asleep minutes per Person: `start_utc` is the first
    minute, `end_utc` the minute after the last.
    """
    asleep = (
        fact_activity_minute.filter(pl.col("activity_kind_key") == ASLEEP_KIND)
        .select("person_key", "timestamp_utc").unique().sort("person_key", "timestamp_utc")
    )
    return (
        asleep.with_columns(
            (pl.col("timestamp_utc").diff().over("person_key") != pl.duration(minutes=1))
            .fill_null(True).cum_sum().over("person_key").alias("_run")
        )
        .group_by("person_key", "_run")
        .agg(
            pl.col("timestamp_utc").min().alias("start_utc"),
            (pl.col("timestamp_utc").max() + pl.duration(minutes=1)).alias("end_utc"),
            pl.len().cast(pl.Int64).alias("asleep_minutes"),
        )
        .select("person_key", "start_utc", "end_utc", "asleep_minutes")
        .sort("person_key", "start_utc")
    )


def build_inferred_nights(
    stretches: pl.DataFrame, fact_sleep_session: pl.DataFrame, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """Inferred Nights, in `derived_night` shape, for windows with no Sleep Session.

    Stretches are merged into blocks by the same Night rule as sessions and the
    longest block per window is the Night. Inferred naps are out of scope.
    """
    session_windows = night_window_dates(
        fact_sleep_session, "start_utc", "end_utc", person_timezone
    ).select("person_key", pl.col("_window").alias("reporting_date_key")).unique()
    # Drop stretches that fall in a window with a real session before merging, so
    # they can neither make a Night nor bridge real sessions.
    free = night_window_dates(stretches, "start_utc", "end_utc", person_timezone).join(
        session_windows,
        left_on=["person_key", "_window"], right_on=["person_key", "reporting_date_key"],
        how="anti",
    ).drop("_window")
    ruled = apply_night_rule(free, person_timezone)
    nights = ruled.filter(~pl.col("is_nap")).join(
        session_windows, on=["person_key", "reporting_date_key"], how="anti"
    )
    return (
        nights.group_by("person_key", "reporting_date_key")
        .agg(
            pl.col("start_utc").min().alias("bedtime_utc"),
            pl.col("end_utc").max().alias("wake_utc"),
            pl.col("asleep_minutes").sum().alias("total_sleep_minutes"),
        )
        .with_columns(
            pl.lit("inferred").alias("source"),
            (pl.col("wake_utc") - pl.col("bedtime_utc")).dt.total_minutes()
            .alias("time_in_bed_minutes"),
            pl.lit(None, dtype=pl.Int64).alias("score"),
            pl.lit(0, dtype=pl.Int64).alias("session_count"),
        )
        .select(
            "person_key", "reporting_date_key", "source", "bedtime_utc", "wake_utc",
            "time_in_bed_minutes", "total_sleep_minutes", "score", "session_count",
        )
        .sort("person_key", "reporting_date_key")
    )
