from datetime import date, datetime, timezone

import polars as pl
import pytest

from gadgetbridge_pipeline.defs.medicine.facts import (
    build_fact_medicine_checkin,
    build_fact_skipped_dose,
    latest_watermarks,
)


def test_existing_skip_rows_map_to_all_medicines_and_the_whole_day():
    skips = pl.DataFrame({
        "person_id": ["person-a", "person-b"],
        "date": [date(2000, 1, 3), date(2000, 1, 4)],
    })

    fact = build_fact_skipped_dose(skips)

    assert fact.columns == ["date_key", "person_key", "medicine_key", "time_key"]
    assert fact.rows() == [
        (date(2000, 1, 3), "person-a", "(all)", -1),
        (date(2000, 1, 4), "person-b", "(all)", -1),
    ]


def _raw_checkins(*rows):
    return pl.DataFrame(
        list(rows),
        schema={"person_id": pl.String, "checked_at": pl.String,
                "skips_complete_through": pl.String},
        orient="row",
    )


def test_a_checkin_keeps_its_person_converts_the_offset_timestamp_to_utc_and_types_the_date():
    raw = _raw_checkins(("person-a", "2000-01-08T09:00:00+07:00", "2000-01-07"))

    fact = build_fact_medicine_checkin(raw)

    assert fact.columns == ["person_key", "checked_at_utc", "skips_complete_through"]
    assert fact.row(0, named=True) == {
        "person_key": "person-a",
        "checked_at_utc": datetime(2000, 1, 8, 2, 0, tzinfo=timezone.utc),
        "skips_complete_through": date(2000, 1, 7),
    }


def test_a_checkin_with_an_unparseable_timestamp_or_date_fails_rather_than_becoming_null():
    with pytest.raises(Exception, match=r"checked_at|conversion|strptime|parse"):
        build_fact_medicine_checkin(_raw_checkins(("person-a", "not-a-time", "2000-01-07")))
    with pytest.raises(Exception, match=r"skips_complete_through|conversion|strptime|parse"):
        build_fact_medicine_checkin(
            _raw_checkins(("person-a", "2000-01-08T09:00:00+07:00", "yesterday"))
        )


def test_the_watermark_is_each_persons_latest_complete_through_date_not_their_latest_checkin():
    checkins = pl.DataFrame({
        "person_key": ["person-a", "person-a", "person-b"],
        "checked_at_utc": [
            datetime(2000, 1, 9, tzinfo=timezone.utc),
            datetime(2000, 1, 20, tzinfo=timezone.utc),   # later check-in, earlier coverage
            datetime(2000, 1, 9, tzinfo=timezone.utc),
        ],
        "skips_complete_through": [date(2000, 1, 8), date(2000, 1, 5), date(2000, 1, 2)],
    })

    assert latest_watermarks(checkins).sort("person_key").rows() == [
        ("person-a", date(2000, 1, 8)), ("person-b", date(2000, 1, 2)),
    ]


def test_building_from_a_stale_bronze_table_without_person_id_says_to_rematerialize_it():
    stale_skips = pl.DataFrame({"date": [date(2000, 1, 3)]})

    with pytest.raises(ValueError, match=r"person_id.*rematerialize"):
        build_fact_skipped_dose(stale_skips)
