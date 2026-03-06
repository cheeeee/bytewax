import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_redistribute():
    inp = list(range(10))
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.redistribute("redist", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == list(range(10))
