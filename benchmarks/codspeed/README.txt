Hot-path A/B shortlist on main-upstream-clone
===========================================

Existing CodSpeed coverage: branch/flat_map_batch (batch 10), unordered
tumbling fold, four-worker fan-out/merge, and broker-free Kafka connectors.
Missing coverage addressed here: retained ordered queues, globally mixed
timestamps, overlapping count windows, sparse timestamps, many keys,
stateful maps, tiny batches, and long map chains.

Every runtime revision uses one common 15-case harness, copied from the
workflow commit. Artifacts record both runtime/harness commits and a workload
SHA-256. This avoids accidentally comparing different inputs or harness fixes.
The window/count dataflows use the public Python API and a real FileSink.
Only queue_* directly exercises internal logic. Public dataflows explicitly use
epoch_interval=timedelta(days=1): the bounded run has one epoch and still
snapshots at EOF. Default wall-clock epochs are unsuitable for CPU simulation:
a slower simulated run would otherwise execute more recovery snapshots.
Outputs are checked after measurement against explicit or independent
pane-based expectations.
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
Use revision=<runtime SHA> to compare historical commits on the same fork
branch. Use part=remainder and part=dense-large for slow baselines; their
disjoint union is the identical full 15-case shortlist. part=all runs it in one
job. The larger simulated count is costly before optimization, so the CI job
limit is 45 minutes; this is separate from the 55-second native deadline.
Manual runs are not cancelled when the branch advances. GitHub pins each
run's commit; ab-results contains this SHA, CodSpeed run ID, raw counters,
compressed callgraphs and the native results. Compare exactly matching
benchmark names, workload hashes, fixed epoch configuration, runner 5.5.0,
Python 3.12 and fair scheduling on both sides. profile_counts.py audits source
snapshot calls in captured callgraphs; each public single-source scenario
should have one, and queue-only scenarios should have zero.
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

Measurement evidence and plots
------------------------------
collect.py MANIFEST DESTINATION downloads pinned GitHub artifacts and public
CodSpeed results. A split variant retains original part receipts and explicitly
assembles disjoint benchmark rows; it does not average unrelated tests.
compare.py RESULTS_DIRECTORY rebuilds CPU/native CSV tables and PNG plots
(matplotlib required only for plotting). profile_counts.py ARTIFACT_DIRECTORY
prints source snapshot counts from the compressed raw callgraphs.

Runtime source candidates:
  https://github.com/bytewax/bytewax/pull/636
  https://github.com/bytewax/bytewax/pull/621
  https://github.com/bytewax/bytewax/pull/617
  https://github.com/bytewax/bytewax/pull/620

Theoretical background for the two algorithmic changes:
  Moerkotte, Small Materialized Aggregates (VLDB 1998), sections 2.1-2.2:
  https://www.vldb.org/conf/1998/p476.pdf
  Block min/max summaries adapt its bucket-pruning principle to retained events.
  Li et al., No Pane, No Gain (SIGMOD Record 2005):
  https://sigmodrecord.org/?download_id=5003&smd_process_download=1
  Count reuse adapts subaggregate sharing within a batch. It does not implement
  persistent panes or claim the paper's state-memory reduction. The paper also
  explains why sparse panes offer little reuse. Stateful batching and inactive
  timers are separate implementation improvements; ISOP is not involved.
