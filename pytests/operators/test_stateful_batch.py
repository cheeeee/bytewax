from typing import Any, List, Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulBatchLogic
from bytewax.testing import TestingSink, TestingSource, run_main
from typing_extensions import override


class SumBatchLogic(StatefulBatchLogic):
    """Sum values in batches."""

    def __init__(self, resume_state: Optional[int]):
        self._sum = resume_state if resume_state is not None else 0

    @override
    def on_batch(self, values: List[Any]) -> Tuple[List[Any], bool]:
        for v in values:
            self._sum += v
        return [self._sum], False

    @override
    def snapshot(self) -> int:
        return self._sum


def test_stateful_batch():
    inp = [("a", 1), ("b", 2), ("a", 3)]
    out = []

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_batch("sum", s, SumBatchLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert ("a", 1) in out
    assert ("a", 4) in out
    assert ("b", 2) in out


def test_stateful_batch_discard():
    inp = [("a", 1), ("a", 2), ("a", 3)]
    out = []

    class DiscardLogic(StatefulBatchLogic):
        def __init__(self, resume_state: Any):
            pass

        @override
        def on_batch(self, values: List[Any]) -> Tuple[List[Any], bool]:
            return values, True

        @override
        def snapshot(self) -> None:
            return None

    flow = Dataflow("test_df")
    s = op.input("inp", flow, TestingSource(inp))
    s = op.stateful_batch("pass", s, DiscardLogic)
    op.output("out", s, TestingSink(out))

    run_main(flow)
    assert sorted(out) == [("a", 1), ("a", 2), ("a", 3)]
