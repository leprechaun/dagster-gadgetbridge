from __future__ import annotations

from datetime import UTC, datetime, timedelta

import dagster as dg
import polars as pl

from gadgetbridge_pipeline.defs.conformed.devices import UNKNOWN_KEY, resolve_device
from gadgetbridge_pipeline.defs.conformed.fact_checks import (
    find_duplicate_grain,
    find_person_device_mismatches,
    find_unresolved_keys,
)
from gadgetbridge_pipeline.defs.conformed.keys import stable_hash64
from gadgetbridge_pipeline.defs.conformed.timekeys import with_local_keys
from gadgetbridge_pipeline.defs.gadgetbridge.night_rule import apply_night_rule
from gadgetbridge_pipeline.defs.gadgetbridge.sleep_session import SleepSession, SleepStage

_SCHEMA = pl.Schema({
    "device_id": pl.Int64,
    "start_utc": pl.Datetime("us", "UTC"),
    "end_utc": pl.Datetime("us", "UTC"),
    "score": pl.Int64,
    "stage_count": pl.Int64,
})

_STAGE_SCHEMA = pl.Schema({
    "device_id": pl.Int64,
    "session_start_utc": pl.Datetime("us", "UTC"),
    "start_utc": pl.Datetime("us", "UTC"),
    "duration_minutes": pl.Int64,
    "sleep_stage_key": pl.String,
})


def _valid_sessions(bronze: pl.DataFrame):
    """(device_id, SleepSession) for each blob that carries real data. Sessions
    with no stages or a zero start are garbage and skipped.
    """
    for device_id, data in zip(bronze["DEVICE_ID"], bronze["DATA"], strict=True):
        session = SleepSession(data)
        if session.stage_count == 0 or session.start == 0:
            continue
        yield device_id, session


def _session_start(session: SleepSession) -> datetime:
    return datetime.fromtimestamp(session.midnight, UTC) - timedelta(hours=24) + timedelta(
        minutes=session.start
    )


def parse_sleep_sessions(bronze: pl.DataFrame) -> pl.DataFrame:
    """Decode each Huami session blob into absolute UTC start and end.

    `start`/`end` are minutes after the midnight before the blob's reference
    midnight. Sessions with no stages or a zero start carry no real data and
    are dropped, as in the existing silver asset.
    """
    rows = []
    for device_id, s in _valid_sessions(bronze):
        base = datetime.fromtimestamp(s.midnight, UTC) - timedelta(hours=24)
        rows.append({
            "device_id": device_id,
            "start_utc": base + timedelta(minutes=s.start),
            "end_utc": base + timedelta(minutes=s.end),
            "score": s.score,
            "stage_count": s.stage_count,
        })
    return pl.DataFrame(rows, schema=_SCHEMA)


def parse_sleep_stages(bronze: pl.DataFrame) -> pl.DataFrame:
    """One row per Sleep Stage, from `SleepSession.periods` (the one decoder)."""
    rows = [
        {
            "device_id": device_id,
            "session_start_utc": _session_start(session),
            "start_utc": period.start,
            "duration_minutes": int(period.duration.total_seconds() // 60),
            "sleep_stage_key": period.kind.name,
        }
        for device_id, session in _valid_sessions(bronze)
        for period in session.periods
    ]
    return pl.DataFrame(rows, schema=_STAGE_SCHEMA)


def build_fact_sleep_session(
    sessions: pl.DataFrame, dim_device: pl.DataFrame, person_timezone: pl.DataFrame
) -> pl.DataFrame:
    """One row per device-reported Sleep Session, with the Night rule applied
    (ADR-0002). `sessions` is the output of `parse_sleep_sessions`.
    """
    resolved = resolve_device(sessions, "device_id", "start_utc", dim_device)
    ruled = apply_night_rule(resolved, person_timezone)
    start = with_local_keys(ruled, "start_utc", person_timezone).rename(
        {"date_key": "start_date_key", "time_key": "start_time_key"}
    )
    both = with_local_keys(start, "end_utc", person_timezone).rename(
        {"date_key": "end_date_key", "time_key": "end_time_key"}
    )

    def minutes_from_reporting_midnight(date_col: str, time_col: str) -> pl.Expr:
        days = (pl.col(date_col) - pl.col("reporting_date_key")).dt.total_days()
        return (days * 1440 + pl.col(time_col)).cast(pl.Int64)

    keys = [
        stable_hash64(device_key, start_utc.isoformat())
        for device_key, start_utc in zip(both["device_key"], both["start_utc"], strict=True)
    ]
    return (
        both.with_columns(
            pl.Series("session_key", keys, dtype=pl.Int64),
            minutes_from_reporting_midnight(
                "start_date_key", "start_time_key"
            ).alias("start_minutes_from_reporting_midnight"),
            minutes_from_reporting_midnight(
                "end_date_key", "end_time_key"
            ).alias("end_minutes_from_reporting_midnight"),
            ((pl.col("end_utc") - pl.col("start_utc")).dt.total_minutes()).alias("length_minutes"),
        )
        .select(
            "session_key", "start_date_key", "end_date_key", "reporting_date_key",
            "start_time_key", "end_time_key", "device_key", "person_key", "is_nap",
            "start_utc", "end_utc",
            "start_minutes_from_reporting_midnight", "end_minutes_from_reporting_midnight",
            "score", "stage_count", "length_minutes",
        )
        .sort("start_utc", "device_key")
    )


def build_fact_sleep_stage(
    stages: pl.DataFrame, fact_sleep_session: pl.DataFrame, dim_device: pl.DataFrame
) -> pl.DataFrame:
    """One row per Sleep Stage. The session's `reporting_date_key` and
    `person_key` are copied from the session fact, never recomputed, so the
    Night rule stays in one place.
    """
    resolved = resolve_device(stages, "device_id", "session_start_utc", dim_device)
    keys = [
        stable_hash64(device_key, start.isoformat())
        for device_key, start in zip(
            resolved["device_key"], resolved["session_start_utc"], strict=True
        )
    ]
    sessions = fact_sleep_session.select("session_key", "reporting_date_key", "person_key")
    return (
        resolved.with_columns(pl.Series("session_key", keys, dtype=pl.Int64))
        .drop("person_key")
        .join(sessions, on="session_key", how="left")
        .with_columns(pl.col("person_key").fill_null(UNKNOWN_KEY))
        .select(
            "session_key", "sleep_stage_key", "reporting_date_key", "person_key",
            "start_utc", "duration_minutes",
        )
        .sort("start_utc", "session_key")
    )


def find_stage_date_mismatches(
    fact_sleep_stage: pl.DataFrame, fact_sleep_session: pl.DataFrame
) -> list[int]:
    """session_keys with a stage row whose reporting date is not its session's
    (including stages whose session does not exist).
    """
    sessions = fact_sleep_session.select(
        "session_key", pl.col("reporting_date_key").alias("_session_date")
    )
    bad = (
        fact_sleep_stage.join(sessions, on="session_key", how="left")
        .filter(
            pl.col("_session_date").is_null()
            | (pl.col("reporting_date_key") != pl.col("_session_date"))
            | pl.col("reporting_date_key").is_null()
        )
        .select("session_key").unique().sort("session_key")
    )
    return bad["session_key"].to_list()


def build_dim_sleep_stage() -> pl.DataFrame:
    return pl.DataFrame({"sleep_stage_key": [stage.name for stage in SleepStage]})


_GOLD = ["gadgetbridge", "gold"]
_CONFORMED = ["conformed", "gold"]
_FACT_KEY = dg.AssetKey([*_GOLD, "fact_sleep_session"])


@dg.asset(
    name="fact_sleep_session",
    group_name="gadgetbridge",
    key_prefix=_GOLD,
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One device-reported Sleep Session, with the Night rule (ADR-0002) applied: "
        "reporting date, nap flag and signed minutes from reporting midnight."
    ),
    ins={
        "huami_sleep_session_sample": dg.AssetIn(
            key=["gadgetbridge", "bronze", "huami_sleep_session_sample"]
        ),
        "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
        "person_timezone": dg.AssetIn(key=[*_CONFORMED, "person_timezone"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_sleep_session(
    huami_sleep_session_sample: pl.DataFrame,
    dim_device: pl.DataFrame,
    person_timezone: pl.DataFrame,
) -> pl.DataFrame:
    sessions = parse_sleep_sessions(huami_sleep_session_sample)
    return build_fact_sleep_session(sessions, dim_device, person_timezone)


@dg.asset_check(asset=_FACT_KEY, blocking=True, name="sleep_session_key_unique")
def sleep_session_key_unique(fact_sleep_session: pl.DataFrame) -> dg.AssetCheckResult:
    dupes = find_duplicate_grain(fact_sleep_session, ["session_key"])
    return dg.AssetCheckResult(passed=not dupes, metadata={"duplicate_keys": str(dupes[:20])})


@dg.asset_check(
    asset=_FACT_KEY,
    blocking=True,
    name="sleep_session_keys_resolve",
    additional_ins={
        "dim_date": dg.AssetIn(key=[*_CONFORMED, "dim_date"]),
        "dim_time_of_day": dg.AssetIn(key=[*_CONFORMED, "dim_time_of_day"]),
        "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
    },
)
def sleep_session_keys_resolve(
    fact_sleep_session: pl.DataFrame,
    dim_date: pl.DataFrame,
    dim_time_of_day: pl.DataFrame,
    dim_device: pl.DataFrame,
    dim_person: pl.DataFrame,
) -> dg.AssetCheckResult:
    checks = [
        ("start_date_key", "date_key", dim_date),
        ("end_date_key", "date_key", dim_date),
        ("reporting_date_key", "date_key", dim_date),
        ("start_time_key", "time_key", dim_time_of_day),
        ("end_time_key", "time_key", dim_time_of_day),
        ("device_key", "device_key", dim_device),
        ("person_key", "person_key", dim_person),
    ]
    unresolved = {
        col: find_unresolved_keys(
            fact_sleep_session.rename({col: dim_col}) if col != dim_col else fact_sleep_session,
            dim_col, dim,
        )[:20]
        for col, dim_col, dim in checks
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


@dg.asset_check(
    asset=_FACT_KEY,
    blocking=True,
    name="sleep_session_person_matches_device",
    additional_ins={"dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"])},
)
def sleep_session_person_matches_device(
    fact_sleep_session: pl.DataFrame, dim_device: pl.DataFrame
) -> dg.AssetCheckResult:
    bad = find_person_device_mismatches(fact_sleep_session, dim_device)
    return dg.AssetCheckResult(passed=not bad, metadata={"mismatches": str(bad[:20])})


_STAGE_KEY = dg.AssetKey([*_GOLD, "fact_sleep_stage"])


@dg.asset(
    name="dim_sleep_stage",
    group_name="gadgetbridge",
    key_prefix=_GOLD,
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description="The four Sleep Stages: REM, LIGHT, DEEP, AWAKE.",
    automation_condition=dg.AutomationCondition.on_missing(),
)
def dim_sleep_stage() -> pl.DataFrame:
    return build_dim_sleep_stage()


@dg.asset(
    name="fact_sleep_stage",
    group_name="gadgetbridge",
    key_prefix=_GOLD,
    io_manager_key="deltalake_io_manager",
    kinds={"deltalake"},
    description=(
        "One Sleep Stage within a session. The session's reporting date and Person are "
        "copied from fact_sleep_session at build time."
    ),
    ins={
        "huami_sleep_session_sample": dg.AssetIn(
            key=["gadgetbridge", "bronze", "huami_sleep_session_sample"]
        ),
        "fact_sleep_session": dg.AssetIn(key=_FACT_KEY),
        "dim_device": dg.AssetIn(key=[*_CONFORMED, "dim_device"]),
    },
    automation_condition=dg.AutomationCondition.eager(),
)
def fact_sleep_stage(
    huami_sleep_session_sample: pl.DataFrame,
    fact_sleep_session: pl.DataFrame,
    dim_device: pl.DataFrame,
) -> pl.DataFrame:
    stages = parse_sleep_stages(huami_sleep_session_sample)
    return build_fact_sleep_stage(stages, fact_sleep_session, dim_device)


@dg.asset_check(
    asset=_STAGE_KEY,
    blocking=True,
    name="sleep_stage_date_matches_session",
    additional_ins={"fact_sleep_session": dg.AssetIn(key=_FACT_KEY)},
)
def sleep_stage_date_matches_session(
    fact_sleep_stage: pl.DataFrame, fact_sleep_session: pl.DataFrame
) -> dg.AssetCheckResult:
    bad = find_stage_date_mismatches(fact_sleep_stage, fact_sleep_session)
    return dg.AssetCheckResult(passed=not bad, metadata={"mismatched_sessions": str(bad[:20])})


@dg.asset_check(
    asset=_STAGE_KEY,
    blocking=True,
    name="sleep_stage_keys_resolve",
    additional_ins={
        "fact_sleep_session": dg.AssetIn(key=_FACT_KEY),
        "dim_sleep_stage": dg.AssetIn(key=[*_GOLD, "dim_sleep_stage"]),
        "dim_person": dg.AssetIn(key=[*_CONFORMED, "dim_person"]),
    },
)
def sleep_stage_keys_resolve(
    fact_sleep_stage: pl.DataFrame,
    fact_sleep_session: pl.DataFrame,
    dim_sleep_stage: pl.DataFrame,
    dim_person: pl.DataFrame,
) -> dg.AssetCheckResult:
    unresolved = {
        key: find_unresolved_keys(fact_sleep_stage, key, dim)[:20]
        for key, dim in {
            "session_key": fact_sleep_session,
            "sleep_stage_key": dim_sleep_stage,
            "person_key": dim_person,
        }.items()
    }
    return dg.AssetCheckResult(
        passed=not any(unresolved.values()), metadata={"unresolved": str(unresolved)}
    )


defs = dg.Definitions(
    assets=[fact_sleep_session, dim_sleep_stage, fact_sleep_stage],
    asset_checks=[
        sleep_session_key_unique,
        sleep_session_keys_resolve,
        sleep_session_person_matches_device,
        sleep_stage_date_matches_session,
        sleep_stage_keys_resolve,
    ],
)
