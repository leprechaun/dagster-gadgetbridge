import datetime

import dagster as dg
import pandera.polars as pa
import polars as pl
from dagster import AssetCheckResult, AutomationCondition, Definitions
from pandera.typing.polars import Series


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
)
def heart_rate_distribution_by_medication_and_weekday(
    daily_heart_rate_distribution: pl.DataFrame,
    medicine_log: pl.DataFrame,
) -> pl.DataFrame:
    medication_by_date = (
        medicine_log
        .filter(pl.col("taken"))
        .group_by("date")
        .agg(pl.col("medicine").sort().str.join(" + ").alias("medication_state"))
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
