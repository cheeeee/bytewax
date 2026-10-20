Hot-path A/B shortlist on main-upstream-clone
===========================================

Existing CodSpeed coverage: branch/flat_map_batch (batch 10), unordered
tumbling fold, four-worker fan-out/merge, and broker-free Kafka connectors.
Missing coverage addressed here: retained ordered queues, globally mixed
timestamps, overlapping count windows, sparse timestamps, many keys,
stateful maps, tiny batches, and long map chains.

All candidate commits use this same 15-case test file and input generation.
The window/count dataflows use the public Python API and a real FileSink.
Only queue_* directly exercises internal logic. Outputs are checked after
measurement against explicit or independent pane-based expectations.
FileSink retains its ordinary flush/fsync. CodSpeed simulation measures
userspace CPU cost; it does not measure disk latency or broker throughput.

Run locally with the matching release extension:
  PYTHONHASHSEED=0 pytest --benchmark-disable benchmarks/codspeed/test_hotpaths.py
  PYTHONHASHSEED=0 python benchmarks/codspeed/native.py --output native.json

Native screening adds 1,000,000-event queue/count cases. Each case runs in
a fresh process, includes runtime startup in its action timer, and checks
its output afterward. Input/graph preparation and verification are untimed.
Peak RSS includes prepared input. The entire native shortlist has a 55-second
budget; unfinished cases are explicitly recorded, never treated as passes.
Single samples screen large effects; they cannot establish small gains.

Dispatch the existing benchmarks workflow with suite=hotpaths on this fork.
Manual runs are not cancelled when the branch advances. GitHub pins each
run's commit; ab-results contains this SHA, CodSpeed run ID, raw counters,
compressed callgraphs and the native results. Compare exactly matching
benchmark names, runner 5.5.0, Python 3.12 and fair scheduling on both sides.
The default all suite runs the original 12 plus the new 15 benchmarks.

Selected candidates (all preserve public API signatures):
  #621: window lookup (also included in #636's queue change).
  #636: block queue summaries, then count reuse as a separate variant.
  #617: stateful hash maps and batch-level Python logic.
  #620: skip timers/spans with no collector.
Do not assume dense-window gains generalize: sparse and many-key controls
are deliberately included. #619 changes Python thread-local lifetime and
#618 adds an unsafe object representation; neither belongs to this first
behavior-preserving integration series.
