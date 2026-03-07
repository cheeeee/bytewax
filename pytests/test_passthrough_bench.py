"""End-to-end throughput benchmarks.

Measures raw data-movement overhead through the bytewax dataflow
without meaningful Python computation. Regressions here indicate
overhead in the Rust TdPyAny wrapping layer.

Run: pytest pytests/test_passthrough_bench.py --benchmark-only -v
"""

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark

ENTRY_POINTS = ["run_main", "cluster_main-1thread"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _run(entry_point, flow, out, n):
    entry_point(flow)
    assert len(out) == n
    out.clear()


# ---------------------------------------------------------------------------
# Passthrough: map(identity) — purest data-movement test
# ---------------------------------------------------------------------------


def _build_passthrough(out, n, batch_size=1000):
    flow = Dataflow("passthrough")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.map("identity", s, lambda x: x)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_passthrough_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_passthrough(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_passthrough_1m(benchmark, entry_point):
    n = 1_000_000
    out = []
    flow = _build_passthrough(out, n, batch_size=10_000)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Keyed passthrough: key_on + map_value — tests extract_key + wrap_key path
# ---------------------------------------------------------------------------


def _build_keyed_passthrough(out, n, batch_size=1000):
    flow = Dataflow("keyed_passthrough")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.key_on("key", s, lambda x: str(x % 10))
    s = op.map_value("identity", s, lambda v: v)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_keyed_passthrough_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_keyed_passthrough(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Multi-operator chain — tests cumulative wrapping overhead
# ---------------------------------------------------------------------------


def _build_chain(out, n, batch_size=1000):
    flow = Dataflow("chain")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.map("map1", s, lambda x: x)
    s = op.map("map2", s, lambda x: x)
    s = op.map("map3", s, lambda x: x)
    s = op.filter("filter_none", s, lambda x: True)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_chain_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_chain(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Flat map: 1-to-2 expansion — tests iterable creation + output wrapping
# ---------------------------------------------------------------------------


def _build_flat_map(out, n, batch_size=1000):
    flow = Dataflow("flat_map")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.flat_map("double", s, lambda x: (x, x))
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_flat_map_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_flat_map(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n * 2))


# ---------------------------------------------------------------------------
# Flat map batch: batch-level transformation — tests batch wrapping path
# ---------------------------------------------------------------------------


def _build_flat_map_batch(out, n, batch_size=1000):
    flow = Dataflow("flat_map_batch")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.flat_map_batch("identity_batch", s, lambda batch: batch)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_flat_map_batch_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_flat_map_batch(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Stateful map: key_on + stateful_map — tests state persistence overhead
# ---------------------------------------------------------------------------


def _build_stateful_map(out, n, batch_size=1000):
    flow = Dataflow("stateful_map")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.key_on("key", s, lambda x: str(x % 10))
    s = op.stateful_map("counter", s, lambda _state, v: (_state, v))
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_stateful_map_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_stateful_map(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Filter chain: 5x filter(True) — predicate overhead without transformation
# ---------------------------------------------------------------------------


def _build_filter_chain(out, n, batch_size=1000):
    flow = Dataflow("filter_chain")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.filter("f1", s, lambda x: True)
    s = op.filter("f2", s, lambda x: True)
    s = op.filter("f3", s, lambda x: True)
    s = op.filter("f4", s, lambda x: True)
    s = op.filter("f5", s, lambda x: True)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_filter_chain_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_filter_chain(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Heavy payload: map with dict objects — wrapping overhead with larger objects
# ---------------------------------------------------------------------------


def _make_heavy_source(n, batch_size=1000):
    return TestingSource(
        [{"id": i, "name": f"item_{i}", "values": [i, i + 1, i + 2]} for i in range(n)],
        batch_size,
    )


def _build_heavy_payload(out, n, batch_size=1000):
    flow = Dataflow("heavy_payload")
    s = op.input("inp", flow, _make_heavy_source(n, batch_size))
    s = op.map("identity", s, lambda x: x)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_heavy_payload_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_heavy_payload(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Branch + merge: stream split/join — tests branch/merge overhead
# ---------------------------------------------------------------------------


def _build_branch_merge(out, n, batch_size=1000):
    flow = Dataflow("branch_merge")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    branch_out = op.branch("split", s, lambda x: x % 2 == 0)
    s = op.merge("join", branch_out.trues, branch_out.falses)
    op.output("out", s, TestingSink(out))
    return flow


@mark.parametrize("entry_point_name", ENTRY_POINTS)
def test_branch_merge_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_branch_merge(out, n)
    benchmark(lambda: _run(entry_point, flow, out, n))
