"""Public dataflow: create timer state, discard most keys, then a small tail."""

import argparse
import json
import os
import resource
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.operators import StatefulBatchLogic
from bytewax.testing import TestingSink, run_main


class Partition(StatefulSourcePartition):
    def __init__(self, n):
        self.n = n
        self.i = 0

    def next_batch(self):
        i = self.i
        if i < self.n:
            end = min(i + 1000, self.n)
            out = [(f"k{j:08}", "create") for j in range(i, end)]
        elif i < 2 * self.n:
            end = min(i + 1000, 2 * self.n)
            out = [
                (f"k{j - self.n:08}", "delete") for j in range(i, end) if j != self.n
            ]  # Keep exactly one live timer.
        elif i < 2 * self.n + 1000:
            end = i + 1
            out = [("k00000000", "tail")]
        else:
            raise StopIteration
        self.i = end
        return out

    def snapshot(self):
        return self.i


class Source(FixedPartitionedSource):
    def __init__(self, n):
        self.n = n

    def list_parts(self):
        return ["one"]

    def build_part(self, *_):
        return Partition(self.n)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("n", type=int)
    parser.add_argument("--checkpoints", action="store_true")
    args = parser.parse_args()
    tail_start = None
    tail_end = None
    rss_at_tail = None
    counts = {"create": 0, "delete": 0, "tail": 0, "snapshot": 0}

    class Logic(StatefulBatchLogic):
        def on_batch(self, values):
            nonlocal tail_start, tail_end, rss_at_tail
            for value in values:
                if value == "tail" and tail_start is None:
                    rss_at_tail = (
                        int(
                            subprocess.check_output(
                                ["ps", "-o", "rss=", "-p", str(os.getpid())],
                                text=True,
                            )
                        )
                        / 1024
                    )
                    tail_start = time.perf_counter()
                counts[value] += 1
                if value == "tail" and counts[value] == 1000:
                    tail_end = time.perf_counter()
            return [], values[-1] == "delete"

        def on_notify(self):
            raise AssertionError("future timer fired early")

        def on_eof(self):
            return [], self.RETAIN

        def notify_at(self):
            return datetime(2100, 1, 1, tzinfo=timezone.utc)

        def snapshot(self):
            counts["snapshot"] += 1
            return None

    flow = Dataflow("churn")
    up = op.input("input", flow, Source(args.n))
    up = op.stateful_batch("state", up, lambda _: Logic())
    op.output("output", up, TestingSink([]))
    start = time.perf_counter()
    run_main(
        flow, epoch_interval=timedelta(0) if args.checkpoints else timedelta(days=1)
    )
    end = time.perf_counter()
    assert counts["create"] == args.n
    assert counts["delete"] == args.n - 1
    assert counts["tail"] == 1000
    print(
        json.dumps(
            {
                "n": args.n,
                "seconds": end - start,
                "tail_seconds": end - tail_start,
                "tail_callbacks_seconds": tail_end - tail_start,
                "finalization_seconds": end - tail_end,
                "rss_at_tail_mib": rss_at_tail,
                "peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                / (1024**2 if sys.platform == "darwin" else 1024),
                "callbacks": counts,
                "checkpoints": args.checkpoints,
            }
        )
    )


if __name__ == "__main__":
    main()
