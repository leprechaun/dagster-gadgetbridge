from gadgetbridge_pipeline.defs.sensors.waypoints_s3_sensor import plan_run_request


def test_plan_run_request_none_when_nothing_changed():
    files = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert plan_run_request(files, files) is None


def test_plan_run_request_triggers_new_file():
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    request = plan_run_request(current, {})
    assert request is not None
    assert "run_key" in request


def test_plan_run_request_triggers_changed_etag():
    previous = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag2"}
    assert plan_run_request(current, previous) is not None


def test_plan_run_request_triggers_removed_file():
    previous = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert plan_run_request({}, previous) is not None


def test_plan_run_request_run_key_differs_for_different_etags():
    current_a = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    current_b = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag2"}
    run_key_a = plan_run_request(current_a, {})["run_key"]
    run_key_b = plan_run_request(current_b, {})["run_key"]
    assert run_key_a != run_key_b


def test_plan_run_request_run_key_stable_for_same_input():
    current = {"owntracks/raw/waypoints/alice/phone/3fbee5a5.json": "etag1"}
    assert plan_run_request(current, {})["run_key"] == plan_run_request(current, {})["run_key"]
