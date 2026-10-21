"""Real CLI execution with metrics, tracing, notifications and file output."""

import argparse
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bytewax.operators as op
from bytewax._bytewax import cli_main
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.operators import StatefulBatchLogic
from bytewax.recovery import RecoveryConfig, init_db_dir
from bytewax.testing import TestingSink, TestingSource
from bytewax.tracing import JaegerConfig, OtlpTracingConfig, setup_tracing
from prometheus_client import Counter

CALLBACKS = Counter("audit_logic_callbacks", "Observed Python calls", ["method"])


class HoldPartition(StatefulSourcePartition):
    def __init__(self, stop):
        self.stop = stop

    def next_batch(self):
        if self.stop.exists():
            raise StopIteration
        return []

    def next_awake(self):
        return datetime.now(timezone.utc) + timedelta(milliseconds=10)

    def snapshot(self):
        return None


class Hold(FixedPartitionedSource):
    def __init__(self, stop):
        self.stop = stop

    def list_parts(self):
        return ["hold"]

    def build_part(self, _step, _part, _state):
        return HoldPartition(self.stop)


class Logic(StatefulBatchLogic):
    def __init__(self, state):
        self.state = state or 0
        self.notified = False

    def on_batch(self, values):
        CALLBACKS.labels("on_batch").inc()
        self.state += len(values)
        return values, self.RETAIN

    def on_notify(self):
        CALLBACKS.labels("on_notify").inc()
        self.notified = True
        return [], self.RETAIN

    def on_eof(self):
        CALLBACKS.labels("on_eof").inc()
        return [], self.RETAIN

    def notify_at(self):
        CALLBACKS.labels("notify_at").inc()
        return None if self.notified else datetime(2000, 1, 1, tzinfo=timezone.utc)

    def snapshot(self):
        CALLBACKS.labels("snapshot").inc()
        return self.state


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--trace", default="off")
    parser.add_argument("--endpoint", default="")
    args = parser.parse_args()
    tracer = None
    if args.trace == "logs":
        tracer = setup_tracing(log_level="TRACE")
    elif args.trace.startswith("otlp"):
        tracer = setup_tracing(
            OtlpTracingConfig(
                "bytewax-audit",
                args.endpoint,
                sampling_ratio=0.0 if args.trace == "otlp0" else 1.0,
            )
        )
    elif args.trace == "jaeger":
        tracer = setup_tracing(JaegerConfig("bytewax-audit", args.endpoint))
    counter = Counter("audit_events", "Mapper invocations", ["step"])

    def mapper(state, value):
        counter.labels("accumulate").inc()
        state = (state or 0) + value
        return state, state

    flow = Dataflow("audit")
    up = op.input(
        "input",
        flow,
        TestingSource(
            [(str(i % 4), i) for i in range(100)],
            batch_size=10,
        ),
    )
    up = op.stateful_batch("timers", up, Logic)
    up = op.stateful_map("sum", up, mapper)
    op.output("memory", up, TestingSink([]))
    up = op.map_value("encode", up, json.dumps)
    op.output("file", up, FileSink(args.directory / "output.jsonl"))
    hold = op.input("hold", flow, Hold(args.directory / "stop"))
    op.output("hold_out", hold, TestingSink([]))
    recovery = args.directory / "recovery"
    recovery.mkdir()
    init_db_dir(recovery, 1)
    cli_main(
        flow,
        workers_per_process=args.workers,
        epoch_interval=timedelta(0),
        recovery_config=RecoveryConfig(recovery),
    )
    # Give the real batch exporter time to send closed root spans too.
    time.sleep(0.25)
    assert counter.labels("accumulate")._value.get() == 100
    assert tracer is not None or args.trace == "off"


if __name__ == "__main__":
    main()
