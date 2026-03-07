"""End-to-end passthrough throughput benchmarks.

Measures raw data-movement overhead through the bytewax dataflow
without meaningful Python computation. Regressions here indicate
overhead in the Rust TdPyAny wrapping layer.

Run: pytest pytests/test_passthrough_bench.py --benchmark-only -v
"""

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from pytest import mark


# ---------------------------------------------------------------------------
# Passthrough: map(identity) — purest data-movement test
# ---------------------------------------------------------------------------


def _build_passthrough(out, n, batch_size=1000):
    flow = Dataflow("passthrough")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.map("identity", s, lambda x: x)
    op.output("out", s, TestingSink(out))
    return flow


def _run_passthrough(entry_point, flow, out, n):
    entry_point(flow)
    assert len(out) == n
    out.clear()


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_passthrough_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_passthrough(out, n)
    benchmark(lambda: _run_passthrough(entry_point, flow, out, n))


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_passthrough_1m(benchmark, entry_point):
    n = 1_000_000
    out = []
    flow = _build_passthrough(out, n, batch_size=10_000)
    benchmark(lambda: _run_passthrough(entry_point, flow, out, n))


# ---------------------------------------------------------------------------
# Keyed passthrough: key_on + map — tests extract_key + wrap_key path
# ---------------------------------------------------------------------------


def _build_keyed_passthrough(out, n, batch_size=1000):
    flow = Dataflow("keyed_passthrough")
    s = op.input("inp", flow, TestingSource(range(n), batch_size))
    s = op.key_on("key", s, lambda x: str(x % 10))
    s = op.map_value("identity", s, lambda v: v)
    op.output("out", s, TestingSink(out))
    return flow


def _run_keyed(entry_point, flow, out, n):
    entry_point(flow)
    assert len(out) == n
    out.clear()


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_keyed_passthrough_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_keyed_passthrough(out, n)
    benchmark(lambda: _run_keyed(entry_point, flow, out, n))


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


def _run_chain(entry_point, flow, out, n):
    entry_point(flow)
    assert len(out) == n
    out.clear()


@mark.parametrize("entry_point_name", ["run_main", "cluster_main-1thread"])
def test_chain_100k(benchmark, entry_point):
    n = 100_000
    out = []
    flow = _build_chain(out, n)
    benchmark(lambda: _run_chain(entry_point, flow, out, n))
