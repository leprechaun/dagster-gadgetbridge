from __future__ import annotations

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.fact_checks import find_duplicate_grain


def require_person_id(frame: pl.DataFrame, source: str) -> pl.DataFrame:
    """Fail loudly if a medicine CSV has no `person_id` column. The column is
    filled in by hand and never defaulted in code.
    """
    if "person_id" not in frame.columns:
        raise ValueError(f"{source} has no person_id column; add it by hand, it is never defaulted")
    return frame


ALL_MEDICINES = "(all)"
UNKNOWN_MEDICINE = "(unknown)"
RESERVED_MEDICINES = (ALL_MEDICINES, UNKNOWN_MEDICINE)


def find_unknown_person_ids(frame: pl.DataFrame, persons: pl.DataFrame) -> list[str | None]:
    """Distinct `person_id` values that are null or not in `persons`."""
    known = set(persons["person_id"].to_list())
    bad = [p for p in frame["person_id"].unique().to_list() if p is None or p not in known]
    return sorted(bad, key=lambda p: (p is not None, str(p)))


def find_reserved_medicine_names(prescriptions: pl.DataFrame) -> list[str]:
    """Medicine names that collide with the reserved dimension members."""
    return sorted(
        {m for m in prescriptions["medicine"].to_list() if m in RESERVED_MEDICINES}
    )


def build_dim_medicine(prescriptions: pl.DataFrame) -> pl.DataFrame:
    """Each Medicine once, plus the reserved `(all)` and `(unknown)` members."""
    names = sorted(set(prescriptions["medicine"].to_list()) - set(RESERVED_MEDICINES))
    return pl.DataFrame({"medicine_key": [*RESERVED_MEDICINES, *names]})


def build_dim_prescription(prescriptions: pl.DataFrame) -> pl.DataFrame:
    """A versioned dimension: the prescribed dose is a period attribute, and the
    key is person, medicine and the day the period starts.
    """
    return prescriptions.select(
        (
            pl.col("person_id") + "/" + pl.col("medicine") + "@"
            + pl.col("start_date").cast(pl.String)
        ).alias("prescription_key"),
        pl.col("person_id").alias("person_key"),
        pl.col("medicine").alias("medicine_key"),
        "dosage_mg",
        pl.col("start_date").alias("valid_from"),
        pl.col("end_date").alias("valid_to"),
    )


_BRONZE = ["medicine", "bronze"]
_GOLD = ["medicine", "gold"]


@dg.asset(
    name="dim_medicine",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description="Each Medicine once, plus the reserved (all) and (unknown) members.",
    ins={"prescriptions": dg.AssetIn(key=[*_BRONZE, "prescriptions"])},
    automation_condition=dg.AutomationCondition.eager(),
)
def dim_medicine(prescriptions: pl.DataFrame) -> pl.DataFrame:
    return build_dim_medicine(prescriptions)


@dg.asset(
    name="dim_prescription",
    group_name="medicine",
    key_prefix=_GOLD,
    io_manager_key="medicine_deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "Versioned prescriptions: the prescribed dose is a period attribute; the key is "
        "person, medicine and valid_from."
    ),
    ins={"prescriptions": dg.AssetIn(key=[*_BRONZE, "prescriptions"])},
    automation_condition=dg.AutomationCondition.eager(),
)
def dim_prescription(prescriptions: pl.DataFrame) -> pl.DataFrame:
    return build_dim_prescription(prescriptions)


@dg.asset_check(
    asset=dg.AssetKey([*_GOLD, "dim_prescription"]),
    blocking=True,
    name="prescription_key_unique",
)
def prescription_key_unique(dim_prescription: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(dim_prescription, ["prescription_key"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_keys": str(dupes[:20])})


defs = dg.Definitions(
    assets=[dim_medicine, dim_prescription], asset_checks=[prescription_key_unique]
)
