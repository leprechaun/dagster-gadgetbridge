# dagster-gadgetbridge

A Dagster pipeline that ingests health data exported by the [Gadgetbridge](https://gadgetbridge.org/) Android app from an Amazfit/Huami wearable and transforms it into analytics-ready datasets stored in Delta Lake.

## How it works

Gadgetbridge syncs wearable data into a SQLite database on the phone and periodically backs it up to S3. An S3 sensor polls for changes every five minutes and, when the file's ETag changes, triggers a full pipeline run. A second sensor watches two small CSVs on S3 that record prescription schedules and skipped doses, triggering rematerialization of the medication adherence assets whenever either file changes.

All assets use `AutomationCondition.eager()` so downstream layers update automatically the moment their upstream data is ready.

## Asset layers

The pipeline follows a medallion architecture: **raw → bronze → silver → gold**.

### Raw

| Asset | Description |
|---|---|
| `gadgetbridge_db_file` | Downloads the SQLite database from S3. Re-downloaded only when the ETag changes. |

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
| `medicine_log` | Daily medication adherence log derived from `prescriptions.csv` and `medicine_skips.csv` on S3 |

Asset checks that block promotion on failure: heart rate in range, battery 0–100, SpO2 70–100, temperature 15–42 °C, stress 1–100, HRV positive and ≤ 300, respiratory rate 4–60 bpm, medicine dosages positive, no orphaned skip records.

### Silver

| Asset | Description |
|---|---|
| `per_minute_health_metrics` | Wide left-join of all bronze health tables at 1-minute resolution. Heart rate value 255 (device sentinel for "no reading") is nulled out. |
| `daily_heart_rate_distribution` | Daily histogram of heart rate in 5 bpm bins (40–160 range). |

### Gold

| Asset | Description |
|---|---|
| `daily_health_snapshot` | Per-day averages for HRV, SpO2, stress, temperature, and heart rate percentiles (p10/median/p90) |
| `steps_per_day` | Daily step totals with weekday/weekend flag |
| `steps_vs_stress` | Daily step totals joined with average and median stress, for correlation analysis |
| `weekday_heart_rate_distribution_before_and_after` | Normalized weekday heart rate distribution split before and after a reference date (2026-05-24) |
| `heart_rate_distribution_by_medication_and_weekday` | Heart rate distribution grouped by active medication state and weekday vs. weekend |
| `daily_medicine_adherence` | Gold-layer copy of `medicine_log` for joining with health metrics |

## Tests

Tests live in `tests/` and run without any external dependencies — no S3, no Dagster instance, no database connection required.

| File | What it covers |
|---|---|
| `test_bronze.py` | Epoch-to-datetime conversion for second and millisecond timestamps; pass/fail behavior of every range-bound asset check |
| `test_silver.py` | Row count, minute truncation, left-join nulls for missing data, multi-sample aggregation within a minute, column set, sort order |
| `test_gold.py` | `daily_health_snapshot` cross-metric join and daily averaging |
| `test_medicine.py` | Date-range expansion from prescriptions, null end-date handling, skip application, dosage calculation |

Run tests locally:

```bash
uv sync --all-groups
uv run pytest -v tests/
```

## CI/CD

Every push to `master` runs the following GitHub Actions pipeline:

1. **Test** — `ruff check`, `dg check defs`, and `pytest`
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
| `DELTALAKE_BUCKET` | Bucket for Delta Lake tables and medicine CSVs (default: `deltalake`) |
