from __future__ import annotations

import polars as pl

UNKNOWN_KEY = "(unknown)"


def _attach_owner(
    df: pl.DataFrame, ts_col: str, df_id: pl.Expr, dim: pl.DataFrame, key_cols: list[str]
) -> pl.DataFrame:
    """Left-join `dim` (with an `_id` column, `valid_from` and `valid_to`) onto
    `df` by id, keeping only the period containing the row's UTC date. Rows with
    no match are kept with every key in `key_cols` set to `(unknown)`, in order.
    """
    indexed = df.with_row_index("_row").with_columns(
        df_id.alias("_id"), pl.col(ts_col).dt.date().alias("_utc_date")
    )
    in_period = (
        indexed.join(dim, on="_id", how="inner")
        .filter(
            (pl.col("_utc_date") >= pl.col("valid_from"))
            & (pl.col("valid_to").is_null() | (pl.col("_utc_date") <= pl.col("valid_to")))
        )
    )
    no_match = indexed.join(in_period.select("_row"), on="_row", how="anti").with_columns(
        *(pl.lit(UNKNOWN_KEY).alias(k) for k in key_cols)
    )
    return (
        pl.concat([in_period.select(no_match.columns), no_match])
        .sort("_row")
        .drop("_row", "_utc_date", "_id", "valid_from", "valid_to", strict=False)
    )


def resolve_device(
    df: pl.DataFrame, id_col: str, ts_col: str, dim_device: pl.DataFrame
) -> pl.DataFrame:
    """Add `device_key` and `person_key` by matching a Gadgetbridge device id
    whose period of use (UTC date, inclusive) contains the row's timestamp.

    Rows with no matching Device are kept, with both keys `(unknown)`, and row
    order is preserved.
    """
    dim = dim_device.select(
        pl.col("gadgetbridge_device_id").cast(pl.Int64).alias("_id"),
        "device_key", "person_key", "valid_from", "valid_to",
    )
    return _attach_owner(df, ts_col, pl.col(id_col), dim, ["device_key", "person_key"])


def resolve_phone(
    df: pl.DataFrame, phone_col: str, ts_col: str, dim_phone: pl.DataFrame
) -> pl.DataFrame:
    """Add `phone_key` and `person_key` by matching an OwnTracks `user/device`
    string against `dim_phone` over its period of use, as `resolve_device` does.
    """
    dim = dim_phone.select(
        pl.col("phone_key").alias("_id"), "phone_key", "person_key", "valid_from", "valid_to"
    )
    return _attach_owner(df, ts_col, pl.col(phone_col), dim, ["phone_key", "person_key"])
