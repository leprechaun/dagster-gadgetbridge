"""
s3_watch
--------
Two reusable sensor factories covering every S3-polling sensor in this
project:

  • make_object_watch_sensor  — HEAD one or more individual, named S3 keys
    and diff their ETags against the cursor. Used by sensors that watch a
    small fixed set of files (the SQLite export, poi.geojson, the medicine
    CSVs).

  • make_prefix_watch_sensor  — LIST every object under a prefix matching a
    suffix and diff the resulting {key: etag} map against the cursor.
    Optionally groups matches (e.g. by month) into separate, optionally
    partitioned run requests. Used by sensors that watch a whole folder of
    files (OwnTracks .rec files).

Both factories share cursor parsing and run-key hashing so the actual S3
polling, cursor handling, and skip/run decision logic exists exactly once.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from collections.abc import Callable

from botocore.exceptions import ClientError
from dagster import (
    AssetSelection,
    DefaultSensorStatus,
    RunRequest,
    SensorEvaluationContext,
    SkipReason,
    sensor,
)

from gadgetbridge_pipeline.defs.resources import S3ClientResource


def parse_json_cursor(cursor: str | None) -> dict:
    if not cursor:
        return {}
    try:
        return json.loads(cursor)
    except (json.JSONDecodeError, ValueError):
        return {}


def _stable_hash(etags: dict[str, str]) -> str:
    return hashlib.md5(json.dumps(sorted(etags.items())).encode()).hexdigest()


# ---- object-watch (HEAD) ----------------------------------------------

def evaluate_object_change(name: str, current_etags: dict[str, str], cursor: dict) -> dict:
    """Pure decision logic shared by every HEAD-based watch sensor.

    run_key joins sorted etag values, so a single-key dict degenerates to
    the bare etag and a multi-key dict combines them all — one formula
    covers both shapes.
    """
    if cursor.get("etags") == current_etags:
        return {
            "action": "skip",
            "reason": f"ETag(s) unchanged for {name} — nothing to do.",
        }

    return {
        "action": "run",
        "run_key": "-".join(v for _, v in sorted(current_etags.items())),
        "new_cursor": {"etags": current_etags},
        "tags": {"triggered_by": name},
    }


def make_object_watch_sensor(
    *,
    name: str,
    description: str,
    keys: dict[str, tuple[str, str]],
    asset_selection: AssetSelection,
    minimum_interval_seconds: int = 300,
):
    """`keys` maps a logical name to a (bucket, key) pair to HEAD."""

    @sensor(
        name=name,
        description=description,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=DefaultSensorStatus.RUNNING,
        asset_selection=asset_selection,
    )
    def _sensor(context: SensorEvaluationContext, s3: S3ClientResource):
        client = s3.get_client()

        current_etags: dict[str, str] = {}
        for logical_name, (bucket, key) in keys.items():
            try:
                head = client.head_object(Bucket=bucket, Key=key)
                current_etags[logical_name] = head["ETag"]
            except ClientError as exc:
                code = exc.response["Error"]["Code"]
                yield SkipReason(f"S3 HEAD failed for {key} ({code}) — will retry next tick")
                return

        cursor = parse_json_cursor(context.cursor)
        decision = evaluate_object_change(name, current_etags, cursor)

        if decision["action"] == "skip":
            yield SkipReason(decision["reason"])
            return

        context.log.info(f"{name}: ETag(s) changed — {cursor.get('etags')!r} → {current_etags!r}")
        context.update_cursor(json.dumps(decision["new_cursor"]))

        yield RunRequest(run_key=decision["run_key"], tags=decision["tags"])

    return _sensor


# ---- prefix-watch (LIST) -----------------------------------------------

def plan_prefix_run_requests(
    current: dict[str, str],
    previous: dict[str, str],
    group_key_fn: Callable[[str], str] = lambda _key: "",
    partition_key_fn: Callable[[str], str] | None = None,
) -> list[dict]:
    """Pure decision logic shared by every LIST-based watch sensor.

    Groups keys via group_key_fn (default: one implicit group covering
    everything — the unpartitioned, single-run case) and returns one entry
    per group whose file set changed, sorted by group. Group membership is
    computed over the union of groups in `current` and `previous`, so a
    group that disappears entirely (its last file removed) is still
    detected as changed.
    """

    def grouped(files: dict[str, str]) -> dict[str, dict[str, str]]:
        g: dict[str, dict[str, str]] = defaultdict(dict)
        for k, v in files.items():
            g[group_key_fn(k)][k] = v
        return g

    current_g, previous_g = grouped(current), grouped(previous)
    all_groups = set(current_g) | set(previous_g)
    changed = sorted(g for g in all_groups if current_g.get(g, {}) != previous_g.get(g, {}))

    requests = []
    for group in changed:
        group_files = current_g.get(group, {})
        run_key = _stable_hash(group_files)
        entry = {"group": group, "run_key": run_key}
        if partition_key_fn is not None:
            pk = partition_key_fn(group)
            entry["partition_key"] = pk
            entry["run_key"] = f"{pk}::{run_key}"
        requests.append(entry)
    return requests


def make_prefix_watch_sensor(
    *,
    name: str,
    description: str,
    bucket: str,
    prefix: str,
    suffix: str,
    asset_selection: AssetSelection,
    group_key_fn: Callable[[str], str] = lambda _key: "",
    partition_key_fn: Callable[[str], str] | None = None,
    minimum_interval_seconds: int = 300,
):
    @sensor(
        name=name,
        description=description,
        minimum_interval_seconds=minimum_interval_seconds,
        default_status=DefaultSensorStatus.RUNNING,
        asset_selection=asset_selection,
    )
    def _sensor(context: SensorEvaluationContext, s3: S3ClientResource):
        client = s3.get_client()

        try:
            paginator = client.get_paginator("list_objects_v2")
            current: dict[str, str] = {}
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    key: str = obj["Key"]
                    if key.endswith(suffix):
                        current[key] = obj["ETag"]
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            yield SkipReason(f"S3 list_objects_v2 failed ({code}) — will retry next tick")
            return

        previous = parse_json_cursor(context.cursor)
        run_requests = plan_prefix_run_requests(current, previous, group_key_fn, partition_key_fn)

        if not run_requests:
            yield SkipReason(f"No changes detected across {len(current)} file(s).")
            return

        context.log.info(f"{name}: affected group(s): {[r['group'] for r in run_requests]}")
        context.update_cursor(json.dumps(current))

        for req in run_requests:
            yield RunRequest(
                partition_key=req.get("partition_key"),
                run_key=req["run_key"],
                tags={"triggered_by": name},
            )

    return _sensor
