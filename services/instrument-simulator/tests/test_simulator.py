from simulator import build_telemetry_timeline


def test_timeline_starts_at_zero_and_ends_shipped():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0)
    assert timeline[0]["event"] == "started"
    assert timeline[0]["progressPct"] == 0
    assert timeline[-1]["event"] == "shipped"
    assert timeline[-1]["progressPct"] == 100


def test_timeline_progress_is_monotonically_nondecreasing():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0)
    progresses = [e["progressPct"] for e in timeline]
    assert progresses == sorted(progresses)


def test_every_event_carries_the_batch_id():
    timeline = build_telemetry_timeline(batch_id="batch-42", tick_seconds=0)
    assert all(e["batchId"] == "batch-42" for e in timeline)


def test_qc_fail_variant_ends_without_shipped():
    timeline = build_telemetry_timeline(batch_id="batch-1", tick_seconds=0, qc_outcome="qc_fail")
    events = [e["event"] for e in timeline]
    assert "qc_fail" in events
    assert "shipped" not in events
