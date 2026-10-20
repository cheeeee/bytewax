"""Fixed hot-path workloads shared by CodSpeed and native screening."""

import json
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main

ORIGIN = datetime(2024, 1, 1, tzinfo=timezone.utc)

# Keep this a shortlist, not a Cartesian product. Large native cases below
# confirm scale without making every simulated comparison prohibitively slow.
CASES = [
    ("queue_stalled", 10),
    ("queue_stalled", 100_000),
    ("queue_mixed", 100_000),
    ("window_stalled_file", 100_000),
    ("window_moving_file", 100_000),
    ("count_dense_file", 10),
    ("count_dense_file", 100),
    ("count_dense_file", 10_000),
    ("count_dense_file", 100_000),
    ("count_sparse_file", 10_000),
    ("count_keys_file", 10_000),
    ("filter_batch1", 100_000),
    ("map_chain", 100_000),
    ("stateful_1000keys", 100_000),
    ("stateful_100000keys", 100_000),
]
LARGE_CASES = [
    ("queue_stalled", 1_000_000),
    ("count_dense_file", 1_000_000),
]


class StalledClockLogic(win.ClockLogic):
    """A controlled watermark, independent of the machine clock."""

    watermark = ORIGIN - timedelta(days=10)

    def before_batch(self):
        pass

    def on_item(self, value):
        return value, self.watermark

    def on_notify(self):
        return self.watermark

    def on_eof(self):
        return win.UTC_MAX

    def to_system_utc(self, timestamp):
        return None

    def snapshot(self):
        return None


class StalledClock(win.Clock):
    def build(self, resume_state):
        return StalledClockLogic()


@dataclass
class Workload:
    action: object
    verify: object


def build_queue(case, n):
    data = [ORIGIN + timedelta(microseconds=(i * 7919) % n) for i in range(n)]
    batches = [data[i : i + 1000] for i in range(0, n, 1000)]

    def action():
        clock = StalledClockLogic()
        logic = win._WindowLogic(
            clock,
            win.TumblingWindower(timedelta(days=1), ORIGIN).build(None),
            lambda state: win._FoldWindowLogic(
                lambda count, _: count + 1,
                lambda a, b: a + b,
                0 if state is None else state,
            ),
            True,
        )
        emitted = []
        for batch in batches:
            emitted.extend(logic.on_batch(batch)[0])
        # Mixed timestamps force partial-block draining. Stalled watermarks
        # exercise retained-queue scans without changing retention semantics.
        for step in range(100):
            if case == "queue_mixed":
                clock.watermark = ORIGIN + timedelta(microseconds=step * n // 100)
            emitted.extend(logic.on_notify()[0])
            logic.notify_at()
        emitted.extend(logic.on_eof()[0])
        return emitted

    def verify(out):
        assert not any(kind == "L" for _, kind, _ in out)
        counts = [(wid, value) for wid, kind, value in out if kind == "E"]
        assert counts == [(0, n)]
        return counts

    return Workload(action, verify)


def build_window(case, n, path):
    stalled = case == "window_stalled_file"
    data = [
        ORIGIN + timedelta(microseconds=(i * 7919) % n if stalled else i)
        for i in range(n)
    ]
    clock = (
        StalledClock()
        if stalled
        else win.EventClock(
            lambda value: value,
            timedelta(microseconds=3000),
            now_getter=lambda: ORIGIN,
            to_system_utc=lambda _: None,
        )
    )
    flow = Dataflow("window_hotpath")
    up = op.input("input", flow, TestingSource(data, batch_size=1000))
    keyed = op.key_on("key", up, lambda _: "one")
    folded = win.fold_window(
        "fold",
        keyed,
        clock,
        win.TumblingWindower(timedelta(days=1), ORIGIN),
        lambda: 0,
        lambda count, _: count + 1,
        lambda a, b: a + b,
    )
    late = []
    op.output("late", folded.late, TestingSink(late))
    encoded = op.map("encode", folded.down, json.dumps)
    op.output("out", op.key_on("file_key", encoded, lambda _: "one"), FileSink(path))

    def action():
        late.clear()
        run_main(flow)

    def verify(_):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        assert rows == [["one", [0, n]]]
        assert not late
        return rows

    return Workload(action, verify)


def build_count(case, n, path):
    overlap = (
        10 if case == "count_keys_file" else 2 if case == "count_sparse_file" else 100
    )
    keys = 1000 if case == "count_keys_file" else 1
    spacing_ms = 1000 if case == "count_sparse_file" else 1
    data = [
        (str(i % keys), ORIGIN + timedelta(milliseconds=i * spacing_ms))
        for i in range(n)
    ]
    # Independent, pane-based oracle. Built outside the measured region.
    panes = Counter((str(i % keys), i * spacing_ms // 1000) for i in range(n))
    expected = Counter()
    for (key, pane), count in panes.items():
        for wid in range(pane - overlap + 1, pane + 1):
            expected[key, wid] += count
    flow = Dataflow("count_hotpath")
    up = op.input("input", flow, TestingSource(data, batch_size=1000))
    clock = win.EventClock(
        lambda value: value[1],
        timedelta(0),
        now_getter=lambda: ORIGIN,
        to_system_utc=lambda _: None,
    )
    counted = win.count_window(
        "count",
        up,
        clock,
        win.SlidingWindower(timedelta(seconds=overlap), timedelta(seconds=1), ORIGIN),
        lambda value: value[0],
    )
    late = []
    op.output("late", counted.late, TestingSink(late))
    encoded = op.map("encode", counted.down, json.dumps)
    op.output("out", op.key_on("file_key", encoded, lambda _: "one"), FileSink(path))

    def action():
        late.clear()
        run_main(flow)

    def verify(_):
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        actual = {(key, wid): count for key, (wid, count) in rows}
        assert len(rows) == len(actual), "duplicate windows"
        assert actual == expected
        assert not late
        return sorted((key, wid, count) for (key, wid), count in actual.items())

    return Workload(action, verify)


def build_core(case, n):
    flow = Dataflow("core_hotpath")
    keys = 1000 if case == "stateful_1000keys" else 100_000
    data = (
        [(str(i % keys), i) for i in range(n)]
        if case.startswith("stateful")
        else range(n)
    )
    batch_size = 1 if case == "filter_batch1" else 1000
    up = op.input("input", flow, TestingSource(data, batch_size=batch_size))
    if case == "filter_batch1":
        up = op.filter("filter", up, lambda _: False)
        expected = []
    elif case == "map_chain":
        for i in range(10):
            up = op.map("map_" + str(i), up, lambda value: value + 1)
        expected = list(range(10, n + 10))
    else:

        def mapper(state, _):
            value = (state or 0) + 1
            return value, value

        up = op.stateful_map("stateful", up, mapper)
        expected = [(str(i % keys), i // keys + 1) for i in range(n)]
    out = []
    op.output("out", up, TestingSink(out))

    def action():
        out.clear()
        run_main(flow)

    expected_counts = Counter(expected)

    def verify(_):
        assert len(out) == len(expected)
        assert Counter(out) == expected_counts
        return sorted(expected_counts.items())

    return Workload(action, verify)


def build(case, n, path):
    if case.startswith("queue_"):
        return build_queue(case, n)
    if case.startswith("window_"):
        return build_window(case, n, path)
    if case.startswith("count_"):
        return build_count(case, n, path)
    return build_core(case, n)
