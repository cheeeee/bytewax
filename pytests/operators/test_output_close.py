"""Tests that sink close() errors propagate correctly."""

import subprocess
import sys


def test_stateless_sink_close_error_propagates():
    """A DynamicSink partition's close() error must fail the dataflow."""
    script = (
        "import bytewax.operators as op\n"
        "from bytewax.dataflow import Dataflow\n"
        "from bytewax.outputs import DynamicSink, StatelessSinkPartition\n"
        "from bytewax.testing import TestingSource, run_main\n"
        "\n"
        "class FailClosePartition(StatelessSinkPartition):\n"
        "    def write_batch(self, items):\n"
        "        pass\n"
        "    def close(self):\n"
        "        raise RuntimeError('flush failed in close')\n"
        "\n"
        "class FailCloseSink(DynamicSink):\n"
        "    def build(self, step_id, worker_index, worker_count):\n"
        "        return FailClosePartition()\n"
        "\n"
        "flow = Dataflow('test_df')\n"
        "s = op.input('inp', flow, TestingSource([1, 2, 3]))\n"
        "op.output('out', s, FailCloseSink())\n"
        "run_main(flow)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        check=False,
    )
    # The dataflow should fail (non-zero exit) because close() raised.
    assert result.returncode != 0, (
        "Dataflow should fail when sink close() raises, but it exited 0.\n"
        f"stdout: {result.stdout.decode(errors='replace')}\n"
        f"stderr: {result.stderr.decode(errors='replace')}"
    )
    stderr = result.stderr.decode(errors="replace")
    assert "flush failed in close" in stderr, (
        f"Expected 'flush failed in close' in stderr, got:\n{stderr}"
    )


def test_successful_sink_close_no_error():
    """A well-behaved sink's close() should not cause failures."""
    script = (
        "import bytewax.operators as op\n"
        "from bytewax.dataflow import Dataflow\n"
        "from bytewax.outputs import DynamicSink, StatelessSinkPartition\n"
        "from bytewax.testing import TestingSource, run_main\n"
        "\n"
        "class GoodPartition(StatelessSinkPartition):\n"
        "    def write_batch(self, items):\n"
        "        pass\n"
        "    def close(self):\n"
        "        pass  # clean close\n"
        "\n"
        "class GoodSink(DynamicSink):\n"
        "    def build(self, step_id, worker_index, worker_count):\n"
        "        return GoodPartition()\n"
        "\n"
        "flow = Dataflow('test_df')\n"
        "s = op.input('inp', flow, TestingSource([1, 2, 3]))\n"
        "op.output('out', s, GoodSink())\n"
        "run_main(flow)\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, (
        f"Subprocess exited with code {result.returncode}.\n"
        f"stdout: {result.stdout.decode(errors='replace')}\n"
        f"stderr: {result.stderr.decode(errors='replace')}"
    )


def test_multiple_sequential_runs_no_hook_leak():
    """Multiple sequential run_main calls should all succeed.

    Validates that panic hooks don't leak between runs.
    """
    script = (
        "import bytewax.operators as op\n"
        "from bytewax.dataflow import Dataflow\n"
        "from bytewax.testing import TestingSink, TestingSource, run_main\n"
        "\n"
        "for i in range(3):\n"
        "    out = []\n"
        "    flow = Dataflow(f'run_{i}')\n"
        "    s = op.input('inp', flow, TestingSource([1, 2, 3]))\n"
        "    op.output('out', s, TestingSink(out))\n"
        "    run_main(flow)\n"
        "    assert sorted(out) == [1, 2, 3], f'Run {i} failed: {out}'\n"
        "print('all_runs_ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, (
        f"Subprocess exited with code {result.returncode}.\n"
        f"stdout: {result.stdout.decode(errors='replace')}\n"
        f"stderr: {result.stderr.decode(errors='replace')}"
    )
    assert "all_runs_ok" in result.stdout.decode(errors="replace")
