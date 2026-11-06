import datetime

import dagster as dg
import pandera.polars as pa
import polars as pl
from dagster import AssetCheckResult, AutomationCondition, Definitions
from pandera.typing.polars import Series

from gadgetbridge_pipeline.defs.resources import column

_UTC_DATE = column("date", "UTC calendar date")
_WEEKDAY = column("weekday", "ISO weekday, 1 = Monday to 7 = Sunday")
_IS_WEEKEND = column("is_weekend", "Saturday or Sunday")


def _is_weekend(date_col: str) -> pl.Expr:
    """True for Saturday/Sunday. ISO weekday: Monday=1 .. Sunday=7."""
    return pl.col(date_col).dt.weekday() >= 6


_COMMON_DATE = datetime.date(1900, 1, 1)
_CUTOFF = datetime.time(15, 0)
_ONE_DAY = pl.duration(days=1)


def _normalize_time_of_day(expr: pl.Expr) -> pl.Expr:
    """Fold a local datetime onto a common date, keeping only its time-of-day,
    then push times after `_CUTOFF` back a day so an evening bedtime and the
    following morning's wake time land on one continuous numeric axis instead
    of reading as ~24h apart (e.g. 23:30 and 00:15 sixteen minutes later).
    """
    combined = pl.lit(_COMMON_DATE).dt.combine(expr.dt.time())
    return pl.when(combined.dt.time() > _CUTOFF).then(combined - _ONE_DAY).otherwise(combined)


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "activity": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])
        ),
    },
    automation_condition=AutomationCondition.eager(),
    description="Daily step totals (UTC date), with weekday and weekend flag",
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        _UTC_DATE,
        column("STEPS", "Total steps that day", unit="steps"),
        _WEEKDAY,
        _IS_WEEKEND,
    ])},
)
def steps_per_day(activity):
    return (
        activity.select(['TIMESTAMP', 'STEPS'])
        .group_by([
            pl.col("TIMESTAMP").dt.date().alias("date")
        ])
        .agg(
            pl.col("STEPS").sum()
        ).sort(
            by=['date']
        ).with_columns(
            pl.col("date").dt.weekday().alias("weekday"),
        ).with_columns(
            _is_weekend("date").alias("is_weekend")
        )
    )


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "activity_sample": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])
        ),
        "hrv": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "generic_hrv_value_sample"])),
        "spo2": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_spo2_sample"])),
        "stress": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"])),
        "temperature": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "bronze", "generic_temperature_sample"])
        ),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "One row per day (UTC date) of average HRV, SpO2, stress and temperature, "
        "plus heart rate min/max/median/p10/p90"
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        _UTC_DATE,
        column("avg_hrv", "Mean heart-rate variability", unit="ms"),
        column("avg_spo2", "Mean blood oxygen saturation", unit="percent"),
        column("avg_stress", "Mean Zepp stress score (0-100)"),
        column("avg_temperature_c", "Mean wrist sensor temperature", unit="celsius"),
        column("heart_rate_min", "Minimum heart rate", unit="bpm"),
        column(
            "heart_rate_max", "Maximum heart rate; includes the 255 no-reading value",
            unit="bpm",
        ),
        column(
            "heart_rate_median", "Median heart rate; includes the 255 no-reading value",
            unit="bpm",
        ),
        column(
            "heart_rate_p10", "10th-percentile heart rate; includes the 255 no-reading value",
            unit="bpm",
        ),
        column(
            "heart_rate_p90", "90th-percentile heart rate; includes the 255 no-reading value",
            unit="bpm",
        ),
    ])},
)
def daily_health_snapshot(
    activity_sample: pl.DataFrame,
    hrv: pl.DataFrame,
    spo2: pl.DataFrame,
    stress: pl.DataFrame,
    temperature: pl.DataFrame,
) -> pl.DataFrame:
    def by_day(df: pl.DataFrame, col: str, alias: str) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
            .group_by("date")
            .agg(pl.col(col).mean().alias(alias))
        )

    frames = [
        by_day(hrv,         "VALUE",       "avg_hrv"),
        by_day(spo2,        "SPO2",        "avg_spo2"),
        by_day(stress,      "STRESS",      "avg_stress"),
        by_day(temperature, "TEMPERATURE", "avg_temperature_c"),
    ]

    frames.append(
        activity_sample.with_columns(
            pl.col("TIMESTAMP").dt.date().alias("date")
        ).group_by(
            ["date"]
        ).agg(
            pl.col("HEART_RATE").min().alias("heart_rate_min"),
            pl.col("HEART_RATE").max().alias("heart_rate_max"),
            pl.col("HEART_RATE").quantile(0.5).alias("heart_rate_median"),
            pl.col("HEART_RATE").quantile(0.1).alias("heart_rate_p10"),
            pl.col("HEART_RATE").quantile(0.9).alias("heart_rate_p90"),
        )
    )

    result = frames[0]

    for f in frames[1:]:
        result = result.join(f, on="date", how="full", coalesce=True)

    sorted_df = result.sort("date")

    return sorted_df

@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "activity": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "bronze", "huami_extended_activity_sample"])
        ),
        "stress": dg.AssetIn(key=dg.AssetKey(["gadgetbridge", "bronze", "huami_stress_sample"])),
    },
    automation_condition=AutomationCondition.eager(),
    description="Daily step totals joined with average stress score, for correlation analysis",
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("date", "UTC calendar date; only days with both steps and stress data"),
        column("total_steps", "Total steps that day", unit="steps"),
        column("avg_stress", "Mean Zepp stress score (0-100)"),
        column("median_stress", "Median Zepp stress score (0-100)"),
        _WEEKDAY,
        _IS_WEEKEND,
    ])},
)
def steps_vs_stress(activity: pl.DataFrame, stress: pl.DataFrame) -> pl.DataFrame:
    daily_steps = (
        activity.select(["TIMESTAMP", "STEPS"])
        .with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
        .group_by("date")
        .agg(pl.col("STEPS").sum().alias("total_steps"))
    )

    daily_stress = (
        stress.select(["TIMESTAMP", "STRESS"])
        .with_columns(pl.col("TIMESTAMP").dt.date().alias("date"))
        .group_by("date")
        .agg(
            pl.col("STRESS").mean().alias("avg_stress"),
            pl.col("STRESS").median().alias("median_stress"),
        )
    )

    return (
        daily_steps.join(daily_stress, on="date", how="inner")
        .sort("date")
        .with_columns(
            pl.col("date").dt.weekday().alias("weekday"),
            _is_weekend("date").alias("is_weekend"),
        )
    )


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "daily_heart_rate_distribution": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "silver", "daily_heart_rate_distribution"])
        ),
        "medicine_log": dg.AssetIn(key=dg.AssetKey(["medicine", "bronze", "medicine_log"])),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "Heart rate distribution normalized within each "
        "(medication_state x weekday/weekend) group"
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("heart_rate", "Lower edge of a 5-bpm bin", unit="bpm"),
        column(
            "medication_state",
            "Medicines taken that day, sorted and joined with ' + '; 'sober' = none taken",
        ),
        column("is_weekend", "Saturday or Sunday (UTC date)"),
        column(
            "sample_count", "Minutes in the bin across all days in the group", unit="minutes",
        ),
        column(
            "proportion",
            "sample_count as a fraction of its (medication_state, is_weekend) group; "
            "sums to 1 per group",
        ),
    ])},
)
def heart_rate_distribution_by_medication_and_weekday(
    daily_heart_rate_distribution: pl.DataFrame,
    medicine_log: pl.DataFrame,
) -> pl.DataFrame:
    medication_by_date = (
        medicine_log
        .filter(pl.col("taken"))
        .group_by("date")
        .agg(pl.col("medicine").unique().sort().str.join(" + ").alias("medication_state"))
    )

    return (
        daily_heart_rate_distribution
        .join(medication_by_date, on="date", how="left")
        .with_columns(
            pl.col("medication_state").fill_null("sober"),
            _is_weekend("date").alias("is_weekend"),
        )
        .group_by(["heart_rate", "medication_state", "is_weekend"])
        .agg(pl.col("sample_count").sum())
        .with_columns(
            (
                pl.col("sample_count")
                / pl.col("sample_count").sum().over(["medication_state", "is_weekend"])
            ).alias("proportion")
        )
        .sort(["medication_state", "is_weekend", "heart_rate"])
    )


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "sleep_periods": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "silver", "sleep_periods_based_on_activity"])
        ),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "Nightly sleep start/end times normalized onto a common date "
        "for weekday vs weekend overlay charting"
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("reporting_date", "Asia/Bangkok wake-up day, as YYYY-MM-DD"),
        column(
            "start",
            "Asia/Bangkok wall-clock start on a dummy date: 1900-01-01, or 1899-12-31 if "
            "after 15:00, so a night plots as one continuous span",
        ),
        column("end", "Asia/Bangkok wall-clock end, on the same dummy date scheme as start"),
        column("is_weekend", "Saturday or Sunday of the reporting_date"),
    ])},
)
def daily_sleep_schedule(sleep_periods: pl.DataFrame) -> pl.DataFrame:
    TZ = "Asia/Bangkok"

    return (
        sleep_periods
        .with_columns(
            pl.col(["start", "end"]).dt.convert_time_zone(TZ)
        )
        .select(["reporting_date", "start", "end"])
        .sort(by="reporting_date")
        .with_columns(
            _is_weekend("reporting_date").alias("is_weekend")
        )
        .with_columns(
            _normalize_time_of_day(pl.col("start")).alias("start"),
            _normalize_time_of_day(pl.col("end")).alias("end"),
        )
        .with_columns(
            pl.col("reporting_date").dt.strftime("%Y-%m-%d"),
        )
    )


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "sleep_sessions": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "silver", "sleep_sessions"])
        ),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        "Daily sleep score statistics (mean, max, session count), "
        "with a 7-day rolling average"
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("date", "UTC calendar date of the session records"),
        column("mean_score", "Mean device sleep score of the day's sessions, to 1 decimal"),
        column("max_score", "Best device sleep score of the day's sessions"),
        column("session_count", "Sleep sessions that day, naps included"),
        _IS_WEEKEND,
        column(
            "score_7d_ma",
            "Rolling mean of mean_score over the last 7 rows (not calendar days); "
            "null for the first 6",
        ),
    ])},
)
def sleep_score_stats(sleep_sessions: pl.DataFrame) -> pl.DataFrame:
    return (
        sleep_sessions
        .with_columns(pl.col("rec").dt.date().alias("date"))
        .group_by("date")
        .agg(
            pl.col("score").mean().round(1).alias("mean_score"),
            pl.col("score").max().alias("max_score"),
            pl.len().cast(pl.Int64).alias("session_count"),
        )
        .sort(by="date")
        .with_columns(
            _is_weekend("date").alias("is_weekend"),
        )
        .with_columns(
            pl.col("mean_score").rolling_mean(window_size=7).round(1).alias("score_7d_ma"),
        )
    )


_CONSISTENCY_WINDOW_DAYS = 14


@dg.asset(
    io_manager_key="deltalake_io_manager",
    ins={
        "daily_sleep_duration": dg.AssetIn(
            key=dg.AssetKey(["gadgetbridge", "silver", "daily_sleep_duration"])
        ),
        # bare key: sleep_score_stats is defined in this same module, so
        # load_assets_from_current_module's key_prefix rewrites this
        # reference the same way it prefixes sleep_score_stats' own key —
        # giving the already-prefixed key here would double-prefix it.
        "sleep_score_stats": dg.AssetIn(key=dg.AssetKey(["sleep_score_stats"])),
    },
    automation_condition=AutomationCondition.eager(),
    description=(
        f"Rolling {_CONSISTENCY_WINDOW_DAYS}-day standard deviation of bedtime, wake time, "
        "and sleep score — how regular sleep is, kept separate from how good it is"
    ),
    metadata={"dagster/column_schema": dg.TableSchema(columns=[
        column("date", "Sleep reporting_date (Asia/Bangkok) joined with sleep_score_stats date"),
        column(
            "sleep_start_stddev_minutes",
            f"Standard deviation of bedtime over the last {_CONSISTENCY_WINDOW_DAYS} rows",
            unit="minutes",
        ),
        column(
            "wake_time_stddev_minutes",
            f"Standard deviation of wake time over the last {_CONSISTENCY_WINDOW_DAYS} rows; "
            "naps count as wake times",
            unit="minutes",
        ),
        column(
            "score_stddev",
            f"Standard deviation of mean_score over the last {_CONSISTENCY_WINDOW_DAYS} rows",
        ),
    ])},
)
def sleep_consistency(
    daily_sleep_duration: pl.DataFrame,
    sleep_score_stats: pl.DataFrame,
) -> pl.DataFrame:
    timing = (
        daily_sleep_duration
        .select(["reporting_date", "sleep_start", "wake_time"])
        .with_columns(
            pl.col(["sleep_start", "wake_time"]).dt.convert_time_zone("Asia/Bangkok")
        )
        .sort("reporting_date")
        .with_columns(
            _normalize_time_of_day(pl.col("sleep_start")).alias("sleep_start"),
            _normalize_time_of_day(pl.col("wake_time")).alias("wake_time"),
        )
        .with_columns(
            (pl.col("sleep_start").dt.epoch(time_unit="s") / 60.0).alias("sleep_start_minutes"),
            (pl.col("wake_time").dt.epoch(time_unit="s") / 60.0).alias("wake_time_minutes"),
        )
        .with_columns(
            pl.col("sleep_start_minutes")
            .rolling_std(window_size=_CONSISTENCY_WINDOW_DAYS)
            .alias("sleep_start_stddev_minutes"),
            pl.col("wake_time_minutes")
            .rolling_std(window_size=_CONSISTENCY_WINDOW_DAYS)
            .alias("wake_time_stddev_minutes"),
        )
        .select(["reporting_date", "sleep_start_stddev_minutes", "wake_time_stddev_minutes"])
        .rename({"reporting_date": "date"})
    )

    score = (
        sleep_score_stats
        .select(["date", "mean_score"])
        .sort("date")
        .with_columns(
            pl.col("mean_score")
            .rolling_std(window_size=_CONSISTENCY_WINDOW_DAYS)
            .alias("score_stddev"),
        )
        .select(["date", "score_stddev"])
    )

    return timing.join(score, on="date", how="full", coalesce=True).sort("date")


class SleepScoreStatsSchema(pa.DataFrameModel):
    date: Series[pl.Date]
    # ASSUMPTION: score is a 0-100 percentage; unconfirmed against real device data
    # (see sleep_sessions' SleepSessionsSchema, same assumption).
    mean_score: Series[float] = pa.Field(ge=0, le=100)
    max_score: Series[int] = pa.Field(ge=0, le=100)
    session_count: Series[int] = pa.Field(gt=0)
    score_7d_ma: Series[float] = pa.Field(ge=0, le=100, nullable=True)

    @pa.dataframe_check
    def mean_score_le_max_score(cls, data: pa.PolarsData) -> pl.LazyFrame:
        return data.lazyframe.select(pl.col("mean_score") <= pl.col("max_score"))


@dg.asset_check(
    asset=dg.AssetKey(["gadgetbridge", "gold", "sleep_score_stats"]),
    blocking=True,
    name="sleep_score_stats_range_checks",
)
def sleep_score_stats_checks(sleep_score_stats: pl.DataFrame) -> AssetCheckResult:
    try:
        SleepScoreStatsSchema.validate(sleep_score_stats, lazy=True)
    except pa.errors.SchemaErrors as exc:
        return AssetCheckResult(
            passed=False,
            metadata={"failure_cases": exc.failure_cases.to_dicts()},
        )
    return AssetCheckResult(passed=True)


defs = Definitions(
    assets=dg.load_assets_from_current_module(
        group_name="gadgetbridge",
        key_prefix=["gadgetbridge", "gold"],
    ),
    asset_checks=[sleep_score_stats_checks],
)
