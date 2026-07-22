from datetime import date, timedelta

import polars as pl

from gadgetbridge_pipeline.defs.assets.medicine import (
    _today,
    build_medicine_log,
    medicine_log_dosage_positive,
    medicine_skips_not_in_future,
    medicine_skips_within_prescriptions,
)

_MED = "Tylenol"
_TODAY = date(2026, 7, 6)
_NO_SKIPS = pl.DataFrame({"date": pl.Series([], dtype=pl.Date)})


def _prescriptions(*rows):
    return pl.DataFrame({
        "start_date": [date.fromisoformat(r[0]) for r in rows],
        "end_date": [date.fromisoformat(r[1]) if r[1] else None for r in rows],
        "medicine": [r[2] for r in rows],
        "dosage_mg": [float(r[3]) for r in rows],
    })


def _skips(*date_strs):
    return pl.DataFrame({"date": [date.fromisoformat(d) for d in date_strs]})


# --- date range expansion ---

def test_generates_one_row_per_day():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    assert result["date"].to_list() == [date(2026, 1, 5), date(2026, 1, 6), date(2026, 1, 7)]


def test_null_end_date_extends_through_today():
    p = _prescriptions(("2026-07-04", None, _MED, 10.0))
    result = build_medicine_log(p, _NO_SKIPS, today=date(2026, 7, 6))
    assert result["date"].to_list() == [date(2026, 7, 4), date(2026, 7, 5), date(2026, 7, 6)]


def test_future_end_date_capped_at_today():
    p = _prescriptions(("2026-07-05", "2026-12-31", _MED, 10.0))
    result = build_medicine_log(p, _NO_SKIPS, today=date(2026, 7, 6))
    assert result["date"].to_list() == [date(2026, 7, 5), date(2026, 7, 6)]


def test_gap_between_prescriptions_produces_no_rows():
    p = _prescriptions(
        ("2026-01-01", "2026-01-03", _MED, 10.0),
        ("2026-01-06", "2026-01-07", _MED, 10.0),
    )
    result = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    dates = result["date"].to_list()
    assert date(2026, 1, 4) not in dates
    assert date(2026, 1, 5) not in dates
    assert len(dates) == 5


def test_empty_prescriptions_returns_empty_df():
    p = pl.DataFrame({
        "start_date": pl.Series([], dtype=pl.Date),
        "end_date": pl.Series([], dtype=pl.Date),
        "medicine": pl.Series([], dtype=pl.String),
        "dosage_mg": pl.Series([], dtype=pl.Float64),
    })
    result = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    assert result.is_empty()


# --- skip handling ---

def test_skip_date_marks_taken_false():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = build_medicine_log(p, _skips("2026-01-06"), today=_TODAY)
    taken = dict(zip(result["date"].to_list(), result["taken"].to_list()))
    assert taken[date(2026, 1, 5)] is True
    assert taken[date(2026, 1, 6)] is False
    assert taken[date(2026, 1, 7)] is True


def test_no_skips_all_taken():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    assert result["taken"].to_list() == [True, True, True]


# --- effective_dosage ---

def test_effective_dosage_is_dosage_when_taken():
    p = _prescriptions(("2026-01-05", "2026-01-05", _MED, 50.0))
    result = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    assert result["effective_dosage"][0] == 50.0


def test_effective_dosage_is_zero_when_skipped():
    p = _prescriptions(("2026-01-05", "2026-01-05", _MED, 50.0))
    result = build_medicine_log(p, _skips("2026-01-05"), today=_TODAY)
    assert result["effective_dosage"][0] == 0.0


# --- asset checks ---

def test_dosage_check_passes():
    p = _prescriptions(("2026-01-05", "2026-01-05", _MED, 50.0))
    df = build_medicine_log(p, _NO_SKIPS, today=_TODAY)
    assert medicine_log_dosage_positive(df).passed


def test_dosage_check_fails_on_zero():
    df = pl.DataFrame({
        "date": [date(2026, 1, 5)],
        "medicine": [_MED],
        "dosage_mg": [0.0],
        "taken": [True],
        "effective_dosage": [0.0],
    })
    assert not medicine_log_dosage_positive(df).passed


# --- medicine_skips_within_prescriptions ---

def test_skips_within_prescriptions_passes_when_skip_in_range():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = medicine_skips_within_prescriptions(p, _skips("2026-01-06"))
    assert result.passed


def test_skips_within_prescriptions_fails_when_skip_outside_range():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = medicine_skips_within_prescriptions(p, _skips("2026-02-01"))
    assert not result.passed
    assert result.metadata["orphaned_skips"].text == "['2026-02-01']"


def test_skips_within_prescriptions_passes_when_no_skips():
    p = _prescriptions(("2026-01-05", "2026-01-07", _MED, 10.0))
    result = medicine_skips_within_prescriptions(p, _NO_SKIPS)
    assert result.passed
    assert result.metadata["skip_count"].value == 0


def test_skips_within_prescriptions_covers_open_ended_prescription():
    p = _prescriptions(("2026-01-05", None, _MED, 10.0))
    result = medicine_skips_within_prescriptions(p, _skips("2026-06-01"))
    assert result.passed


# --- medicine_skips_not_in_future ---

def test_skips_not_in_future_passes_for_past_date():
    yesterday = _today() - timedelta(days=1)
    skips = pl.DataFrame({"date": [yesterday]})
    assert medicine_skips_not_in_future(skips).passed


def test_skips_not_in_future_passes_for_today():
    skips = pl.DataFrame({"date": [_today()]})
    assert medicine_skips_not_in_future(skips).passed


def test_skips_not_in_future_fails_for_future_date():
    tomorrow = _today() + timedelta(days=1)
    skips = pl.DataFrame({"date": [tomorrow]})
    result = medicine_skips_not_in_future(skips)
    assert not result.passed
    assert result.metadata["future_skips"].text == f"['{tomorrow}']"

