"""Tunable policy constants shared across domains. Each lives here and
nowhere else; import it rather than restating the value.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

# Sleep merges across wakings shorter than this into one Sleep Block (ADR-0002).
SLEEP_BLOCK_GAP = timedelta(hours=3)

# A Night's midpoint falls between this time on the previous day and on its date (ADR-0002).
NIGHT_BOUNDARY = time(15, 0)

# A measure's window counts as covered when worn at least this many of 24 hours.
COVERAGE_MIN_HOURS = 20

# Fixes inside the same POI further apart than this end the Visit.
VISIT_GAP = timedelta(minutes=30)

# Reporting timezone until a per-Person source exists.
DEFAULT_TIMEZONE = "Asia/Bangkok"

# Start of every person_timezone chain, so it covers any instant in the data.
TIMEZONE_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# dim_date spans from here to a year past today, so late-zone dates still resolve.
DIM_DATE_START = date(2015, 1, 1)
