Fork hot-path experiments, 2026-10-20
===================================

Historical A-E results. The subsequent count-bound reuse experiment is in
../2026-10-20-count-bounds; it addresses the sparse-count regression below.

Scope and decision
------------------
These are cumulative experiments on cheeeee/bytewax:main-upstream-clone.
A is our merged fork/upstream-main baseline, not pristine upstream main.
The runtime currently includes B, C, D and E. Public API signatures remain
unchanged. Count reuse is still experimental: its dense-window improvement
comes with a sparse-timestamp regression. It needs a cheap sparse fallback
before a general-purpose merge. No upstream PR was changed for this experiment.

Final CPU results (E versus A)
-----------------------------
Speedup is baseline CPU cost / new CPU cost. Values below 1 mean slower.

Scenario                    Events        A seconds     E seconds   Speedup
Retained queue                  10          0.0050        0.0051      0.98x
Retained queue             100,000         21.8555        1.5831     13.81x
Globally mixed queue       100,000         15.1853        7.0098      2.17x
Stalled fold + FileSink    100,000         10.1198        2.3285      4.35x
Moving fold + FileSink     100,000          3.7585        2.3936      1.57x
Dense count + FileSink          10          0.0219        0.0158      1.38x
Dense count + FileSink         100          0.0874        0.0177      4.94x
Dense count + FileSink      10,000          7.4637        0.2463     30.31x
Dense count + FileSink     100,000         71.1914        2.3042     30.90x
Sparse count + FileSink     10,000          1.3754        1.5264      0.90x
Count, 1,000 keys           10,000          3.3839        3.0015      1.13x
Filter, batch size 1       100,000          3.1220        2.9913      1.04x
Ten-map chain              100,000          2.3320        2.3305      1.00x
Stateful, 1,000 keys       100,000          2.2197        1.7797      1.25x
Stateful, 100,000 keys     100,000          5.6640        4.6434      1.22x

Incremental effects explain the combined result:
  B versus A: retained queue 13.81x; mixed queue 2.17x; stalled/moving
    FileSink fold 4.37x/1.58x; dense count 1.98-2.03x at 10k/100k.
  C versus B: dense count 15.29-15.41x at 10k/100k, but sparse count uses
    17.27% more CPU. The final sparse case uses 10.98% more CPU than A.
  D versus C: stateful workloads 1.19-1.23x.
  E versus D: filter with batches of one 1.053x; stateful 1.018-1.022x.
    Other differences are mostly small; the full-run repeat also varies.
There is no evidence of a universal speedup for ordinary map callbacks.
cpu-comparison.csv preserves every variant and incremental ratio.

Native confirmation at 1,000,000 events
--------------------------------------
Two-sample medians on the same machine:
  Retained queue:       17.576 -> 0.512 seconds, 34.34x.
  Dense count/FileSink: 18.166 -> 0.720 seconds, 25.24x.
  Queue peak RSS:       185.16 -> 184.94 MiB.
  Count peak RSS:       197.58 -> 197.54 MiB.
The large time savings repeat; these RSS differences do not establish a
memory improvement. native-comparison.csv also contains the 10-100,000-event
single-sample results. million-confirmation.json contains the second samples.

Coverage
--------

The previous suite covered branch/flat_map_batch, unordered tumbling fold,
four-worker fan-out/merge and broker-free Kafka connector operations. It did
not cover retained ordered queues, global timestamp disorder, overlapping
counts, sparse timestamps, many keys or stateful-map cardinality. The new
15-case shortlist adds those scenarios, tiny batches and a ten-map control.
The complete suite now runs all 27 benchmarks.

Variants and attribution
------------------------
A  22753ccc  merged main baseline, with the common benchmark harness
B  37e21699  + block queue summaries and sliding-window lookup (#636, #621)
C  5beaf30f  + batch-local overlapping-count reuse (#636)
D  68276acd  + stateful hash maps and batch-level Python logic (#617)
E  8aec6528  + bypass inactive metrics timers and tracing spans (#620)

Each variant includes its predecessors. Comparing E with A measures the
combined result; adjacent comparisons isolate each addition. Source PRs:
  https://github.com/bytewax/bytewax/pull/636
  https://github.com/bytewax/bytewax/pull/621
  https://github.com/bytewax/bytewax/pull/617
  https://github.com/bytewax/bytewax/pull/620
Unsafe GIL/lifetime/object-representation experiments #619/#618 and Kafka
fetch batching #622 are outside this integration. Kafka needs a broker/I/O
measurement before a throughput claim.

Why the changes help
--------------------
B stores timestamp min/max summaries per queue block. A watermark that
cannot release any event can skip the retained data. Fully due blocks drain
together; partially due disordered blocks still require scanning. Sliding
windows also avoid rebuilding metadata for windows already open.

C groups events with equal first/last window IDs within a batch and adds one
subtotal to each affected window. Dense input repeats far fewer window
lookups and Python counter calls. Sparse input offers little reuse but still
pays to calculate bounds and build the grouping dictionary. The many-key
control similarly offers little opportunity to combine events per key.

D reduces repeated Python calls and state lookup overhead while preserving
observable ordering and snapshot format. E avoids starting timers/spans when
no relevant collector is active. These are smaller gains than the dense
count algorithm; they do not make arbitrary Python callbacks faster.

Measurement method
------------------
All five runtime revisions use the same harness ab51102e and workload SHA256
eecf2106c699d9b2c1e6f30451097d4038335fa254a3cd9ca42f78e6dfe8c0ec.
CPU runs use Ubuntu 24.04 x86_64, Python 3.12.3, CodSpeed runner 5.5.0,
pytest-codspeed 5.0.3, PYTHONHASHSEED=0 and fair scheduling. CPU numbers come
from CodSpeed's public results API: simulated CPU seconds, not CI wall time.
A and B use two disjoint jobs because the baseline is expensive to simulate;
their unchanged benchmark cases are assembled, not averaged. Raw service
responses, runtime/harness hashes and original job receipts are retained.

Public dataflows use epoch_interval=1 day for these bounded measurements.
EOF snapshots still run. Callgraph audits confirm exactly one source snapshot
for every single-source dataflow, and zero for internal queue-only cases.
Initial default-epoch runs performed unequal numbers of snapshots under slow
simulation (e.g. 7 versus 8 in the many-key case). Those CPU comparisons are
discarded. discarded-epoch-diagnostics.json retains the evidence. Periodic
recovery performance is deliberately outside this CPU comparison.

Native results use release extensions on macOS ARM64 / Python 3.12.13. Each
case runs in a fresh process with prepared input and graph. The action timer
includes runtime startup and normal FileSink flush/fsync, while setup and
output validation are untimed. Peak process RSS includes prepared input.
Every completed result is validated; output digests match across A-E.
The 17-case native shortlist has a 55-second deadline. Local A completed in
41.30 seconds, E in 4.29 seconds. Smaller cases have one sample per variant;
the million-event A/E cases were repeated once, reported as two-sample medians
with min/max whiskers. These native samples establish large effects, not
precise percentages for small differences.

FileSink is real, but output volume matters: fold scenarios emit one record
for 100,000 events; dense count emits 199 records at 100,000 events and 1,099
at 1,000,000. Sparse count emits 10,001 records and many-key count 19,000.
These demonstrate complete Python dataflows with file output, not saturated
disk throughput. CodSpeed excludes syscall latency. No real-broker Kafka
throughput claim follows from this suite.

Validation and limitations
--------------------------
347 Python tests passed; 18 real-Kafka tests skipped because no broker was
configured. Nine Rust unit tests and one doctest passed; four existing
doctests remain ignored. Ruff, rustfmt, Clippy, mypy (23 source files) and
actionlint passed. AST comparison found 76 unchanged public Python function
and method signatures in operators/windowing, including annotations/defaults.
This is evidence of compatibility, not a formal proof of every API behavior.

The final complete CI run passed all 27 benchmarks; its 338 deselected cases
are ordinary correctness tests, which run separately, not skipped benchmarks:
  https://github.com/cheeeee/bytewax/actions/runs/39511529251
  https://app.codspeed.io/cheeeee/bytewax/runs/6ad7f641ad0bd23fa756abbf
The common 15 cases repeat within 2.3% of E's separate run (large window/count
cases within 0.7%). This is a sanity check, not a statistical confidence bound.
The original 12 cases were checked for completion; their historical CPU
values use a different harness and are not included in this A/B claim.

Retention remains unbounded when an application stalls its watermark.
Block summaries reduce repeated work, not the number of retained events.
This harness shows no material peak-RSS reduction at a million events.

Evidence and reproduction
-------------------------
manifest.json records full runtime/workflow SHAs and every GitHub run ID.
Per-variant service.json contains official CPU values; split variants retain
each original response and receipt beneath dense-large/ and remainder/.
capture.json includes raw counters and the native extension hash. Compressed
callgraphs remain downloadable from the corresponding GitHub artifacts;
source-snapshots.json contains their epoch audit. native-local.json records
wall time, peak RSS and output digests. native-ci.json is a separate Linux
screening and is not mixed into the macOS native table.
On the slower CI machine, A's two million-event native cases exhausted the
55-second budget and are explicitly marked budget_exceeded, not passed.
All 17 cases completed locally; the million-event claims use those local runs.

Rebuild tables and figures from this directory:
  python benchmarks/codspeed/compare.py benchmarks/codspeed/results/2026-10-20
Re-fetch raw artifacts and service results (gh authentication required):
  python benchmarks/codspeed/collect.py \
    benchmarks/codspeed/results/2026-10-20/manifest.json /tmp/bytewax-ab
See ../../README.txt for workload and workflow commands.

Scientific background
---------------------
Moerkotte, Small Materialized Aggregates (VLDB 1998), sections 2.1-2.2:
  https://www.vldb.org/conf/1998/p476.pdf
The queue adapts the bucket min/max pruning principle to retained events.
Li et al., No Pane, No Gain (SIGMOD Record 2005):
  https://sigmodrecord.org/?download_id=5003&smd_process_download=1
Count reuse adapts subaggregate sharing inside one batch. It does not add
persistent panes or claim the paper's state-memory reduction. Sparse panes
provide little reuse, which explains why a fallback matters here. The other
two changes are implementation improvements. Minato-Morreale ISOP is not
used: Bytewax has no relevant BDD/Boolean-cover hot path.
