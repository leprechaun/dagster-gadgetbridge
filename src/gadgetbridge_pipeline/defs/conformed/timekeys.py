from __future__ import annotations

import polars as pl


def with_local_keys(
    df: pl.DataFrame, ts_col: str, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """Add `date_key` (the Person's Local Date) and `time_key` (minute of day,
    0..1439) computed from a UTC timestamp column and the Person's timezone.

    `df` needs `person_key` and `ts_col`; `person_timezone` is the
    `person_timezone` lookup. Computed once in the fact build, so nothing
    downstream converts again. A row with no covering timezone gets null keys,
    which the fact build resolves to `(unknown)`.
    """
    indexed = df.with_row_index("_row")
    zoned = (
        indexed.sort(ts_col)
        .join_asof(
            person_timezone.sort("valid_from"),
            left_on=ts_col,
            right_on="valid_from",
            by="person_key",
            strategy="backward",
            check_sortedness=False,
        )
        .with_columns(
            pl.when(pl.col("valid_to").is_null() | (pl.col(ts_col) < pl.col("valid_to")))
            .then(pl.col("timezone"))
            .alias("timezone")
        )
    )
    parts = []
    for zone in zoned["timezone"].unique().to_list():
        chunk = zoned.filter(pl.col("timezone").is_null() if zone is None
                             else pl.col("timezone") == zone)
        if zone is None:
            local = pl.lit(None, dtype=pl.Datetime)
        else:
            local = pl.col(ts_col).dt.convert_time_zone(zone)
        parts.append(
            chunk.with_columns(
                local.dt.date().alias("date_key"),
                (local.dt.hour().cast(pl.Int64) * 60 + local.dt.minute()).alias("time_key"),
            )
        )
    return (
        pl.concat(parts)
        .sort("_row")
        .select(*df.columns, "date_key", "time_key")
    )
