# dagster-gadgetbridge

[![Maintainability](https://qlty.sh/gh/leprechaun/projects/dagster-gadgetbridge/maintainability.svg)](https://qlty.sh/gh/leprechaun/projects/dagster-gadgetbridge)
[![Code Coverage](https://qlty.sh/gh/leprechaun/projects/dagster-gadgetbridge/coverage.svg)](https://qlty.sh/gh/leprechaun/projects/dagster-gadgetbridge)

A Dagster pipeline that ingests health data exported by the [Gadgetbridge](https://gadgetbridge.org/) Android app from an Amazfit/Huami wearable, [OwnTracks](https://owntracks.org/) location history, and a hand-curated points-of-interest file, transforming them into analytics-ready datasets stored in Delta Lake.

## How it works

Gadgetbridge syncs wearable data into a SQLite database on the phone and periodically backs it up to S3. An S3 sensor polls for changes every five minutes and, when the file's ETag changes, triggers a full pipeline run. A second sensor watches two small CSVs on S3 that record prescription schedules and skipped doses, triggering rematerialization of the medication adherence assets whenever either file changes.

Two more independent domains poll S3 the same way: an OwnTracks sensor lists `owntracks/raw/rec/` and triggers one run per calendar month whose `.rec` files changed (each run processes that month's partition), and a POI sensor triggers `points_of_interest` whenever the hand-curated `poi.geojson` changes.

All assets use `AutomationCondition.eager()` so downstream layers update automatically the moment their upstream data is ready.

`medicine_log` is also rematerialized daily at 00:05 Asia/Bangkok by a schedule, independent of the CSV sensor. Its adherence data is computed through "today", so if the CSVs go untouched for a while, the eager condition alone would never rerun it and its cutoff would silently freeze at whatever day it last happened to run — the schedule keeps it advancing regardless.

## Asset layers

The pipeline follows a medallion architecture: **raw → bronze → silver → gold**.

### Raw

| Asset | Description |
|---|---|
| `gadgetbridge_db_file` | Downloads the SQLite database from S3. Re-downloaded only when the ETag changes |

### Bronze

Each bronze asset reads a table from the SQLite file, converts the epoch timestamp to a timezone-aware datetime (Asia/Bangkok), and writes a Delta table to S3. Blocking asset checks validate schemas and enforce physiological range bounds before downstream assets can proceed.

| Asset | Description |
|---|---|
| `huami_extended_activity_sample` | Per-minute steps, sleep stages, raw movement intensity, and heart rate |
| `generic_temperature_sample` | Body/ambient temperature readings |
| `huami_sleep_respiratory_rate_sample` | Nighttime respiratory rate |
| `generic_hrv_value_sample` | Heart rate variability (HRV) |
| `huami_stress_sample` | HRV-derived stress score (1–100) |
| `huami_spo2_sample` | Blood oxygen saturation |
| `huami_pai_sample` | Amazfit's PAI health metric (low/moderate/high activity breakdown) |
| `battery_level` | Device battery level |
| `huami_sleep_session_sample` | Raw sleep session binary blobs |

Asset checks that block promotion on failure: heart rate in range, battery 0–100, SpO2 70–100, temperature 15–42 °C, stress 1–100, HRV positive and ≤ 300, respiratory rate 4–60 bpm.

### Silver

| Asset | Description |
|---|---|
| `per_minute_health_metrics` | Wide left-join of all bronze health tables at 1-minute resolution. Heart rate value 255 (device sentinel for "no reading") is nulled out. |
| `daily_heart_rate_distribution` | Daily histogram of heart rate in 5 bpm bins (40–160 range). |
| `sleep_periods_based_on_activity` | Individual sleep periods (start/end) derived from contiguous "asleep" activity samples |
| `daily_sleep_duration` | Nightly sleep duration (minutes), sleep start, and wake time, aggregated from `sleep_periods_based_on_activity` — a base for future moving-average and medication/stress join assets |
| `sleep_sessions` | Per-session sleep score, start/end time, and stage count, decoded from `huami_sleep_session_sample`'s binary blobs (parsing lives in `sleep_session.py`'s `SleepSession`). Sessions with `stage_count == 0` or `start == 0` — garbage/incomplete records — are dropped. |

Blocking asset checks, both defined as [pandera](https://pandera.readthedocs.io/) schemas: `daily_sleep_duration` (`DailySleepDurationSchema` in `silver.py`) validates total sleep 0–1440 minutes and sleep start before wake time; `sleep_sessions` (`SleepSessionsSchema`) validates `start` > 0, session length 0–1440 minutes, score 0–100 (unconfirmed against real device data — see the ASSUMPTION comment in `silver.py`), stage count > 0, and session start before end. On failure, every violated row/check is reported in one pass via pandera's lazy validation, not just the first.

### Gold

| Asset | Description |
|---|---|
| `daily_health_snapshot` | Per-day averages for HRV, SpO2, stress, temperature, and heart rate percentiles (p10/median/p90) |
| `steps_per_day` | Daily step totals with weekday/weekend flag |
| `steps_vs_stress` | Daily step totals joined with average and median stress, for correlation analysis |
| `heart_rate_distribution_by_medication_and_weekday` | Heart rate distribution grouped by active medication state and weekday vs. weekend |
| `daily_sleep_schedule` | Nightly sleep start/end times normalized onto a common date, split by weekday vs. weekend, for overlay charting |
| `sleep_score_stats` | Daily sleep score statistics (mean, max, session count) from `sleep_sessions`, with a 7-day rolling average and a weekday/weekend flag |

Blocking asset check on `sleep_score_stats`, defined as a pandera schema (`SleepScoreStatsSchema` in `gold.py`): `mean_score`/`max_score`/`score_7d_ma` in 0–100 (`score_7d_ma` may be null for the first 6 days of data), `session_count` > 0, and `mean_score` ≤ `max_score`.

## OwnTracks

A separate domain tracking device location history, independent of the medallion layers above. Raw `.rec` files (tab-separated timestamp + JSON, one per user/device/month) land in S3 under `owntracks/raw/rec/{user}/{device}/{year-month}.rec`; `location_records` is partitioned by month (`MonthlyPartitionsDefinition`, `partition_expr="year_month"`) and reads every user/device file for its partition.

| Asset | Description |
|---|---|
| `location_records` (bronze) | Parsed `.rec` lines (location entries only) for one monthly partition, across all users/devices |
| `location_records_with_poi` (silver) | Location records annotated with the POI(s) — circle or rectangle — each point falls within, one column per POI kind |

## Points of interest (POI)

Bronze-only domain, independent of the medallion layers above: a hand-curated GeoJSON file (`poi.geojson`) of named circles (`Point` + `radius_m`) and axis-aligned rectangles (`Polygon`), which `owntracks`'s silver layer joins location records against.

| Asset | Description |
|---|---|
| `points_of_interest` | Named circles and rectangles parsed from `poi.geojson`, each with a `kind` tier (`point-of-interest`, `area`, `region`, `territory`); kinds can nest, so a location can match POIs of several different kinds at once |

Blocking asset checks: POI names unique, `kind` present and valid, circle radius positive, rectangle bounds valid (min < max on both axes), no same-kind rectangle overlap (different kinds may nest freely).

## Medicine

A separate domain, independent of the medallion layers above — its inputs are two hand-maintained CSVs on S3 (`prescriptions.csv`, `medicine_skips.csv`), not the Gadgetbridge SQLite export. `gadgetbridge`'s `heart_rate_distribution_by_medication_and_weekday` (above) consumes its output across domains, the same way `owntracks` consumes `poi`.

| Asset | Description |
|---|---|
| `prescriptions` | Prescriptions CSV mirrored from S3, versioned in Delta Lake |
| `medicine_skips` | Medicine skip records mirrored from S3, versioned in Delta Lake |
| `medicine_log` | Daily medication adherence log derived from `prescriptions` and `medicine_skips` |

Blocking asset checks: medicine dosages positive, no orphaned skip records (a skip date outside every prescription's date range), no skip dates in the future.

## Tests

Tests live in `tests/` and run without any external dependencies — no S3, no Dagster instance, no database connection required.

| File | What it covers |
|---|---|
| `test_bronze.py` | Epoch-to-datetime conversion for second and millisecond timestamps; pass/fail behavior of every range-bound asset check |
| `test_silver.py` | Row count, minute truncation, left-join nulls for missing data, multi-sample aggregation within a minute, column set, sort order; sleep period detection, `daily_sleep_duration` aggregation across interrupted/multiple nights, and its range/invariant asset check; `sleep_sessions` decoding, dropping zero-`stage_count`/zero-`start` rows, and its range/invariant asset check |
| `test_sleep_session.py` | `SleepSession` binary blob parsing: scalar field offsets, per-stage decoding, stage timing relative to the previous midnight, total duration summation |
| `test_gold.py` | `daily_health_snapshot` cross-metric join and daily averaging; `sleep_score_stats` aggregation, 7-day rolling average, weekday/weekend flag, and its range/invariant asset check |
| `test_medicine.py` | Date-range expansion from prescriptions, null end-date handling, skip application, dosage calculation |
| `test_s3_watch.py` | Cursor parsing and skip-vs-run decision logic shared by every sensor: ETag/HEAD-based change detection and LIST-based prefix diffing/grouping into run requests |
| `test_s3_sensor.py` | Wiring for the SQLite S3 sensor: name, poll interval, asset selection |
| `test_medicine_s3_sensor.py` | Wiring for the medicine CSV sensor: name, poll interval, asset selection |
| `test_owntracks_s3_sensor.py` | Month/partition-key derivation for the OwnTracks sensor, plus sensor wiring |
| `test_owntracks_bronze.py` | `.rec` line parsing: location filtering, malformed JSON/timestamps, optional fields |
| `test_owntracks_silver.py` | Location-to-POI join: circle (haversine) and rectangle matching, multi-kind nesting, unmatched locations |
| `test_poi.py` | GeoJSON feature parsing (circles/rectangles), rejection of malformed geometry, same-kind rectangle overlap detection |
| `test_poi_s3_sensor.py` | Wiring for the POI GeoJSON sensor: name, poll interval, asset selection |
| `test_medicine_schedule.py` | `medicine_log` daily schedule's cron, timezone, default status, and asset selection |

Run tests locally:

```bash
uv sync --all-groups
uv run pytest -v tests/
```

Every run prints a coverage summary (via `pytest-cov`, configured in `pyproject.toml`). For a browsable line-by-line HTML report:

```bash
uv run pytest tests/ --cov-report=html
open htmlcov/index.html
```

## CI/CD

Every push to `master` runs the following GitHub Actions pipeline:

1. **Test** — `ruff check`, `dg check defs`, `pytest`, then exports coverage and uploads it to Qlty
2. **Build and push** — builds a Docker image and pushes it to `ghcr.io/leprechaun/dagster-gadgetbridge` tagged `latest` and with the run number
3. **Deploy** — opens a WireGuard tunnel to the private network, then runs `helm upgrade` against the Kubernetes cluster using the `dagster/dagster-user-deployments` chart

The Helm values in `helm-charts/values/prod.yaml` pin the image tag to the current run number (substituted by `sed` during the deploy step). Dependency updates are managed automatically by Renovate.

## Local development

```bash
uv sync
dg dev
```

Open http://localhost:3000 to access the Dagster UI.

Environment variables required (see `.env.k8s` for the Kubernetes set):

| Variable | Purpose |
|---|---|
| `AWS_ENDPOINT_URL_S3` | S3-compatible endpoint (e.g. MinIO) |
| `DELTALAKE_BUCKET` | Bucket for all Delta Lake tables plus hand-maintained raw inputs (medicine CSVs, `poi.geojson`, OwnTracks `.rec` files) (default: `deltalake`) |
