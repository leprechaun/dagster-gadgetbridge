import dagster as dg
import polars as pl
import pytest
from dagster_delta import LocalConfig
from deltalake import DeltaTable, write_deltalake

from gadgetbridge_pipeline.defs.resources import (
    DescribedDeltaLakePolarsIOManager,
    column_metadata_updates,
    sync_column_metadata,
    sync_table_description,
)

# sync_table_description(dt, description) — sets the Delta-level description,
# committing only when it changes. Runs against a local table in tmp_path.

def _table(tmp_path) -> DeltaTable:
    write_deltalake(str(tmp_path), pl.DataFrame({"a": [1]}))
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


# column_metadata_updates(schema, current) — pure diff of declared column
# descriptions/tags against the Delta fields' existing metadata.

_SCHEMA = dg.TableSchema(columns=[
    dg.TableColumn("temp", "float", description="Skin temperature", tags={"unit": "celsius"}),
])


def test_column_metadata_updates_returns_declared_metadata():
    assert column_metadata_updates(_SCHEMA, {"temp": {}}) == {
        "temp": {"unit": "celsius", "comment": "Skin temperature"},
    }


def test_column_metadata_updates_skips_unchanged_columns():
    current = {"temp": {"unit": "celsius", "comment": "Skin temperature", "other": "kept"}}
    assert column_metadata_updates(_SCHEMA, current) == {}


def test_column_metadata_updates_detects_changed_tag():
    current = {"temp": {"unit": "fahrenheit", "comment": "Skin temperature"}}
    assert column_metadata_updates(_SCHEMA, current) == {
        "temp": {"unit": "celsius", "comment": "Skin temperature"},
    }


def test_column_metadata_updates_ignores_missing_schema():
    assert column_metadata_updates(None, {"temp": {}}) == {}


def test_column_metadata_updates_rejects_undeclared_table_column():
    with pytest.raises(ValueError, match="temp"):
        column_metadata_updates(_SCHEMA, {"other": {}})


# sync_column_metadata(dt, schema) — writes the diff onto a local table.

def test_sync_column_metadata_sets_and_then_skips(tmp_path):
    write_deltalake(str(tmp_path), pl.DataFrame({"temp": [36.5]}))
    assert sync_column_metadata(DeltaTable(str(tmp_path)), _SCHEMA) == ["temp"]
    field = DeltaTable(str(tmp_path)).schema().fields[0]
    assert dict(field.metadata) == {"unit": "celsius", "comment": "Skin temperature"}
    version = DeltaTable(str(tmp_path)).version()
    assert sync_column_metadata(DeltaTable(str(tmp_path)), _SCHEMA) == []
    assert DeltaTable(str(tmp_path)).version() == version


def test_column_metadata_survives_partition_overwrite(tmp_path):
    @dg.asset(
        key_prefix=["demo"],
        partitions_def=dg.StaticPartitionsDefinition(["a", "b"]),
        metadata={"partition_expr": "part", "dagster/column_schema": _SCHEMA},
    )
    def partitioned(context) -> pl.DataFrame:
        return pl.DataFrame({"part": [context.partition_key], "temp": [36.5]})

    resources = {"io_manager": DescribedDeltaLakePolarsIOManager(
        root_uri=str(tmp_path), storage_options=LocalConfig()
    )}
    for key in ["a", "b", "a"]:
        assert dg.materialize([partitioned], partition_key=key, resources=resources).success

    dt = DeltaTable(str(tmp_path / "demo" / "partitioned"))
    temp = next(f for f in dt.schema().fields if f.name == "temp")
    assert dict(temp.metadata) == {"unit": "celsius", "comment": "Skin temperature"}
    # metadata written once, then preserved by later partition writes
    assert [h["operation"] for h in dt.history()].count("UPDATE FIELD METADATA") == 1
