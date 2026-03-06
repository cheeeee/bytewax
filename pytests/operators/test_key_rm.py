import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_key_rm():
    inp = [{"key": "a", "val": 1}, {"key": "b", "val": 2}]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.key_on("key", s, lambda x: x["key"])
    s = op.key_rm("unkey", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [{"key": "a", "val": 1}, {"key": "b", "val": 2}]
