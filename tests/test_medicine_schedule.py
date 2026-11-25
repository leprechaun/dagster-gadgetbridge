from dagster import AssetKey, AssetSelection, DefaultScheduleStatus

from gadgetbridge_pipeline.defs.medicine.schedule import (
    medicine_day_daily_schedule,
    medicine_day_job,
    medicine_log_daily_schedule,
    medicine_log_job,
)


def test_schedule_runs_daily_in_bangkok_time():
    assert medicine_log_daily_schedule.cron_schedule == "5 0 * * *"
    assert medicine_log_daily_schedule.execution_timezone == "Asia/Bangkok"


def test_schedule_is_running_by_default():
    assert medicine_log_daily_schedule.default_status == DefaultScheduleStatus.RUNNING


def test_schedule_targets_the_medicine_log_job():
    assert medicine_log_daily_schedule.job_name == medicine_log_job.name


def test_job_selects_only_medicine_log():
    assert medicine_log_job.selection == AssetSelection.assets(
        AssetKey(["medicine", "bronze", "medicine_log"])
    )


def test_derived_medicine_day_has_its_own_daily_bangkok_schedule_and_job():
    assert medicine_day_daily_schedule.cron_schedule == "5 0 * * *"
    assert medicine_day_daily_schedule.execution_timezone == "Asia/Bangkok"
    assert medicine_day_daily_schedule.job_name == medicine_day_job.name
    assert medicine_day_job.selection == AssetSelection.assets(
        AssetKey(["medicine", "gold", "derived_medicine_day"])
    )
