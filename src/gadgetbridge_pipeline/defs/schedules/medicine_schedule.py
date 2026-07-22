"""
medicine_schedule
------------------
Forces medicine_log to rematerialize once a day, independent of medicine_s3_sensor.

medicine_log's "taken" column is computed through date.today() at run time, but
its AutomationCondition.eager() only reacts to *new* materializations of its
inputs (prescriptions, medicine_skips). Those only rerun when the CSVs' S3
ETags change — so if the CSVs go untouched, medicine_log never reruns either,
and its "today" cutoff freezes at whatever day it last happened to run. This
schedule rematerializes medicine_log daily regardless, so the cutoff keeps
advancing even when nothing on S3 has changed.
"""

from __future__ import annotations

import dagster as dg
from dagster import AssetKey, AssetSelection, DefaultScheduleStatus, Definitions

medicine_log_job = dg.define_asset_job(
    name="medicine_log_daily_job",
    selection=AssetSelection.assets(AssetKey(["gadgetbridge", "bronze", "medicine_log"])),
)

medicine_log_daily_schedule = dg.ScheduleDefinition(
    name="medicine_log_daily_schedule",
    job=medicine_log_job,
    cron_schedule="5 0 * * *",
    execution_timezone="Asia/Bangkok",
    default_status=DefaultScheduleStatus.RUNNING,
)

defs = Definitions(jobs=[medicine_log_job], schedules=[medicine_log_daily_schedule])
