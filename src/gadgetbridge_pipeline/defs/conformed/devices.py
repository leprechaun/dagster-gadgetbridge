from __future__ import annotations

import polars as pl

UNKNOWN_KEY = "(unknown)"


def resolve_device(
    df: pl.DataFrame, id_col: str, ts_col: str, dim_device: pl.DataFrame
) -> pl.DataFrame:
    """Add `device_key` and `person_key` by matching a Gadgetbridge device id
    whose period of use (UTC date, inclusive) contains the row's timestamp.

    Rows with no matching Device are kept, with both keys `(unknown)`, and row
    order is preserved.
    """
    devices = dim_device.select(
        pl.col("gadgetbridge_device_id").cast(pl.Int64).alias(id_col),
        "device_key", "person_key", "valid_from", "valid_to",
    )
    indexed = df.with_row_index("_row").with_columns(pl.col(ts_col).dt.date().alias("_utc_date"))
    in_period = (
        indexed.join(devices, on=id_col, how="inner")
        .filter(
            (pl.col("_utc_date") >= pl.col("valid_from"))
            & (pl.col("valid_to").is_null() | (pl.col("_utc_date") <= pl.col("valid_to")))
        )
    )
    no_device = indexed.join(in_period.select("_row"), on="_row", how="anti").with_columns(
        pl.lit(UNKNOWN_KEY).alias("device_key"), pl.lit(UNKNOWN_KEY).alias("person_key")
    )
    return (
        pl.concat([in_period.select(no_device.columns), no_device])
        .sort("_row")
        .drop("_row", "_utc_date")
    )
