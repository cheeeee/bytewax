"""Measure the same workloads before and after candidate commits."""

import pytest
from workloads import CASES, build


@pytest.mark.parametrize("case,n", CASES, ids=[f"{case}-{n}" for case, n in CASES])
def test_hotpath(benchmark, tmp_path, case, n):
    workload = build(case, n, tmp_path / "output.jsonl")
    result = benchmark(workload.action)
    workload.verify(result)
