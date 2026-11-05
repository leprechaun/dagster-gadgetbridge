import dagster as dg
import polars as pl
from dagster_deltalake import LocalConfig
from deltalake import DeltaTable, write_deltalake

from gadgetbridge_pipeline.defs.resources import (
    DescribedDeltaLakePolarsIOManager,
    sync_table_description,
)

# sync_table_description(dt, description) — sets the Delta-level description,
# committing only when it changes. Runs against a local table in tmp_path.

def _table(tmp_path) -> DeltaTable:
    write_deltalake(str(tmp_path), pl.DataFrame({"a": [1]}).to_arrow())
    return DeltaTable(str(tmp_path))


def test_sync_table_description_sets_description(tmp_path):
    dt = _table(tmp_path)
    assert sync_table_description(dt, "hello") is True
    assert DeltaTable(str(tmp_path)).metadata().description == "hello"


def test_sync_table_description_skips_commit_when_unchanged(tmp_path):
    dt = _table(tmp_path)
    sync_table_description(dt, "hello")
    version = DeltaTable(str(tmp_path)).version()
    assert sync_table_description(DeltaTable(str(tmp_path)), "hello") is False
    assert DeltaTable(str(tmp_path)).version() == version


def test_sync_table_description_ignores_missing_description(tmp_path):
    dt = _table(tmp_path)
    assert sync_table_description(dt, None) is False
    assert DeltaTable(str(tmp_path)).version() == 0


# DescribedDeltaLakePolarsIOManager — copies each asset's description onto its
# Delta table; assets without one are left alone.

def test_io_manager_syncs_asset_descriptions(tmp_path):
    @dg.asset(key_prefix=["demo"], description="described table")
    def described() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    @dg.asset(key_prefix=["demo"])
    def undescribed() -> pl.DataFrame:
        return pl.DataFrame({"a": [1]})

    result = dg.materialize(
        [described, undescribed],
        resources={"io_manager": DescribedDeltaLakePolarsIOManager(
            root_uri=str(tmp_path), storage_options=LocalConfig()
        )},
    )

    assert result.success
    described_dt = DeltaTable(str(tmp_path / "demo" / "described"))
    assert described_dt.metadata().description == "described table"
    assert DeltaTable(str(tmp_path / "demo" / "undescribed")).metadata().description is None
