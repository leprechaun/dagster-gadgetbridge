"""The Night rule (ADR-0002), as a pure function over Sleep Sessions."""

from __future__ import annotations

import polars as pl

from gadgetbridge_pipeline.defs.conformed.constants import NIGHT_BOUNDARY, SLEEP_BLOCK_GAP
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys

NIGHT_BOUNDARY_MINUTE = NIGHT_BOUNDARY.hour * 60 + NIGHT_BOUNDARY.minute


def _with_block_id(sessions: pl.DataFrame) -> pl.DataFrame:
    """Number each Person's Sleep Blocks: a session starts a new block unless it
    begins less than SLEEP_BLOCK_GAP after everything before it has ended.
    """
    ordered = sessions.sort("person_key", "start_utc")
    prior_end = pl.col("end_utc").cum_max().shift(1).over("person_key")
    new_block = prior_end.is_null() | ((pl.col("start_utc") - prior_end) >= SLEEP_BLOCK_GAP)
    return ordered.with_columns(new_block.cum_sum().over("person_key").alias("_block"))


def apply_night_rule(sessions: pl.DataFrame, person_timezone: pl.DataFrame) -> pl.DataFrame:
    """Add `reporting_date_key` and `is_nap` to each session.

    `sessions` needs `person_key`, `start_utc` and `end_utc`.
    """
    blocked = _with_block_id(sessions)
    blocks = blocked.group_by("person_key", "_block").agg(
        pl.col("start_utc").min().alias("_b_start"), pl.col("end_utc").max().alias("_b_end")
    ).with_columns(
        (pl.col("_b_start") + (pl.col("_b_end") - pl.col("_b_start")) / 2).alias("_mid")
    )
    windowed = with_local_keys(blocks, "_mid", person_timezone).select(
        "person_key", "_block", "_b_start", "_b_end",
        pl.when(pl.col("time_key") >= NIGHT_BOUNDARY_MINUTE)
        .then(pl.col("date_key") + pl.duration(days=1))
        .otherwise(pl.col("date_key"))
        .alias("_window"),
    )
    end_date = with_local_keys(windowed, "_b_end", person_timezone).select(
        "person_key", "_block", pl.col("date_key").alias("_end_date")
    )
    keyed = (
        windowed.join(end_date, on=["person_key", "_block"])
        .sort("_b_start")  # deterministic tie-break: the earliest equal-length block is the Night
        .with_columns(
            (
                pl.col("_b_end") - pl.col("_b_start")
            ).rank("ordinal", descending=True).over("person_key", "_window").alias("_rank")
        )
        .with_columns((pl.col("_rank") > 1).alias("is_nap"))
        .select(
            "person_key", "_block", "is_nap",
            pl.when(pl.col("is_nap")).then(pl.col("_end_date")).otherwise(pl.col("_window"))
            .alias("reporting_date_key"),
        )
    )
    return blocked.join(keyed, on=["person_key", "_block"], how="left").drop("_block")
