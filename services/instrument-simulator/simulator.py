import json
import threading
import time
import uuid
from datetime import datetime, timezone

import stomp


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_telemetry_timeline(batch_id: str, tick_seconds: float = 2.0, qc_outcome: str = "qc_pass") -> list[dict]:
    steps = [
        ("started", 0),
        ("running", 25),
        ("running", 50),
        ("running", 75),
        (qc_outcome, 90),
    ]
    if qc_outcome == "qc_pass":
        steps.append(("shipped", 100))

    timeline = []
    for event, progress in steps:
        timeline.append({
            "batchId": batch_id,
            "progressPct": progress,
            "event": event,
            "timestamp": _now_iso(),
        })
        if tick_seconds:
            time.sleep(tick_seconds)
    return timeline


def _connection(host: str, port: int) -> stomp.Connection:
    # heartbeats=(10000, 10000): stomp.py sends none by default, and this
    # broker's default STOMP acceptor enforces a connection TTL that
    # depends on them -- confirmed running Task 12's end-to-end demo, the
    # consumer on synthesis-jobs silently went from 1 to 0 a couple of
    # minutes after its last activity, well after the earlier per-job
    # blocking (also fixed, see SynthesisJobListener.on_message) stopped
    # being the cause. reconnect_attempts_max=-1: retry forever rather
    # than giving up after stomp.py's default of 3, so a broker restart
    # doesn't permanently kill the simulator.
    return stomp.Connection([(host, port)], heartbeats=(10000, 10000), reconnect_attempts_max=-1)


class TelemetryPublisher:
    def __init__(self, host: str, port: int, username: str, password: str):
        self._conn = _connection(host, port)
        self._conn.connect(username, password, wait=True)

    def publish(self, event: dict) -> None:
        # Plain destination name, no "/queue/" prefix: this broker's default
        # STOMP acceptor config doesn't map that prefix to the anycast
        # address of the same name, so a prefixed destination creates its
        # own separate (multicast) address instead of reaching the queue
        # Mule's JMS listener actually consumes from. Found running Task
        # 12's end-to-end demo: "/queue/synthesis-jobs" showed up as its
        # own address with 0 messages ever delivered to Mule's queue.
        self._conn.send(body=json.dumps(event), destination="batch-telemetry")

    def close(self) -> None:
        self._conn.disconnect()


class SynthesisJobListener(stomp.ConnectionListener):
    def __init__(self, on_job):
        self._on_job = on_job

    def on_message(self, frame):
        job = json.loads(frame.body)
        # Handling a job takes ~10s (build_telemetry_timeline sleeps
        # tick_seconds between each of its steps). Doing that inline on
        # this callback would block stomp.py's own receiver thread for
        # the whole span, which stops it from responding to heartbeats
        # during that window. Running the job on its own thread keeps
        # the receiver thread free to service heartbeats throughout.
        threading.Thread(target=self._on_job, args=(job,), daemon=True).start()


def run(host: str, port: int, username: str, password: str, tick_seconds: float = 2.0) -> None:
    publisher = TelemetryPublisher(host, port, username, password)

    def handle_job(job: dict) -> None:
        # tick_seconds=0 here: build the whole timeline's steps/progress
        # values at once, then do the actual pacing (and stamp each
        # event's real-time timestamp) in this loop, so each event
        # publishes as it "happens" rather than in one burst at the end
        # (the original shape called build_telemetry_timeline with the
        # real tick_seconds and only published after it had already
        # slept through the whole timeline internally, which also left
        # every event in a batch carrying the same timestamp).
        for event in build_telemetry_timeline(batch_id=job["batchId"], tick_seconds=0):
            event["timestamp"] = _now_iso()
            publisher.publish(event)
            if tick_seconds:
                time.sleep(tick_seconds)

    listener_conn = _connection(host, port)
    listener_conn.set_listener("", SynthesisJobListener(handle_job))
    listener_conn.connect(username, password, wait=True)
    listener_conn.subscribe(destination="synthesis-jobs", id=str(uuid.uuid4()), ack="auto")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    import os
    run(
        host=os.environ.get("ACTIVEMQ_STOMP_HOST", "activemq"),
        port=int(os.environ.get("ACTIVEMQ_STOMP_PORT", "61613")),
        username=os.environ.get("ACTIVEMQ_USERNAME", "admin"),
        password=os.environ.get("ACTIVEMQ_PASSWORD", "admin"),
        tick_seconds=float(os.environ.get("SIMULATOR_TICK_SECONDS", "2")),
    )
