import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_flat_map_value():
    inp = [("key1", "hello world"), ("key2", "hi")]
    out = []

    def split_words(value):
        return value.split()

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map_value("split", s, split_words)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == sorted(
        [("key1", "hello"), ("key1", "world"), ("key2", "hi")]
    )


def test_flat_map_value_empty():
    inp = [("k", "a"), ("k", "")]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.flat_map_value("split", s, lambda v: v.split())
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert out == [("k", "a")]
