import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main


def test_inspect_debug():
    inp = [1, 2, 3]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.inspect_debug("dbg", s)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == [1, 2, 3]


def test_inspect_debug_with_custom_inspector():
    inp = [10, 20, 30]
    out = []
    inspected = []

    def my_inspector(step_id, item, worker_index, worker_count):
        inspected.append((step_id, item))

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.inspect_debug("dbg", s, inspector=my_inspector)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == [10, 20, 30]
    assert len(inspected) == 3
    assert all(step_id == "test_df.dbg" for step_id, _ in inspected)
    assert sorted(item for _, item in inspected) == [10, 20, 30]
