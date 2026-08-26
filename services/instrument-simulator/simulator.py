import json
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


class TelemetryPublisher:
    def __init__(self, host: str, port: int, username: str = "admin", password: str = "admin"):
        self._conn = stomp.Connection([(host, port)])
        self._conn.connect(username, password, wait=True)

    def publish(self, event: dict) -> None:
        self._conn.send(body=json.dumps(event), destination="/queue/batch-telemetry")

    def close(self) -> None:
        self._conn.disconnect()


class SynthesisJobListener(stomp.ConnectionListener):
    def __init__(self, on_job):
        self._on_job = on_job

    def on_message(self, frame):
        job = json.loads(frame.body)
        self._on_job(job)


def run(host: str, port: int, tick_seconds: float = 2.0) -> None:
    publisher = TelemetryPublisher(host, port)

    def handle_job(job: dict) -> None:
        for event in build_telemetry_timeline(batch_id=job["batchId"], tick_seconds=tick_seconds):
            publisher.publish(event)

    listener_conn = stomp.Connection([(host, port)])
    listener_conn.set_listener("", SynthesisJobListener(handle_job))
    listener_conn.connect("admin", "admin", wait=True)
    listener_conn.subscribe(destination="/queue/synthesis-jobs", id=str(uuid.uuid4()), ack="auto")

    while True:
        time.sleep(1)


if __name__ == "__main__":
    import os
    run(
        host=os.environ.get("ACTIVEMQ_STOMP_HOST", "activemq"),
        port=int(os.environ.get("ACTIVEMQ_STOMP_PORT", "61613")),
        tick_seconds=float(os.environ.get("SIMULATOR_TICK_SECONDS", "2")),
    )
