"""Diagnostic wrapper around the unchanged four-worker merge benchmark."""

import faulthandler
import os
from pathlib import Path
import runpy
import sys
import time

faulthandler.enable()
faulthandler.dump_traceback_later(20, repeat=True)
started = time.monotonic()


def log(message):
    print(f"DIAG {time.monotonic() - started:.3f}s {message}", flush=True)


source = os.environ.get("BENCHMARK_SOURCE") or str(
    Path(__file__).resolve().parents[2] / "pytests/operators/test_merge.py"
)
module = runpy.run_path(source)
out = []
flow = module["build_fan_out_merge_dataflow"](out)
calls = 0


def target():
    global calls
    calls += 1
    log(f"target {calls} begin")
    module["run_fan_out_merge_dataflow"](
        module["_cluster_main_4thread"], flow, out
    )
    log(f"target {calls} end")


log(f"Python {sys.version}; mode={sys.argv[1]}; pid={os.getpid()}")
if sys.argv[1] == "plain":
    target()
    target()
else:
    if os.environ.get("DIAG_NO_PERF"):
        sys.activate_stack_trampoline = lambda *_: log("perf trampoline suppressed")
    from pytest_codspeed.instruments import MeasurementMode
    from pytest_codspeed.instruments.analysis import AnalysisInstrument

    instrument = AnalysisInstrument(None, MeasurementMode.Simulation)
    hooks = instrument.instrument_hooks
    for name in ("callgrind_start_instrumentation", "callgrind_stop_instrumentation"):
        original = getattr(hooks, name)

        def wrapped(original=original, name=name):
            log(f"{name} begin")
            original()
            log(f"{name} end")

        setattr(hooks, name, wrapped)
    log(f"instrument initialized; perf={sys.is_stack_trampoline_active()}")
    instrument.measure(None, "merge", "diagnostic::merge", target)
faulthandler.cancel_dump_traceback_later()
log("finished")
