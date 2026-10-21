Count-window regression follow-up
================================

This experiment remains on cheeeee/bytewax:main-upstream-clone. The runtime
fix is d9f89ed57a9abe98e6e97374512e6eef6f6105f6. Variant E is the previous
combined optimization; F adds this fix. A is the earlier merged main baseline.
The previous results are retained in ../2026-10-20 as historical evidence.

Result and decision
-------------------
The observed sparse-count regression is resolved on the fixed workload.
CodSpeed simulated CPU seconds (lower is better):

  Count + FileSink             A: main base    E: before     F: fixed
  Sparse, 10,000 events            1.375386     1.526363     1.248505
  Dense, 10,000 events             7.463665     0.246279     0.233649
  Dense, 100,000 events           71.191424     2.304228     2.190584
  Many keys, 10,000 events         3.383892     3.001542     2.998614

Sparse count now uses 18.2% less CPU than E and 9.2% less than A. The previous
11.0% CPU overhead relative to A has become a 1.10x speedup. Dense 100k uses
4.9% less CPU than E, preserving the combined optimizations' 32.5x speedup
over A. That 32.5x is the cumulative A-to-F gain, not the effect of this fix.
These are CPU simulation estimates; FileSink is exercised, but these values
do not represent elapsed disk I/O time. See the separate native confirmation.

All 15 CodSpeed cases passed in 586.53 seconds. The 17-case CI native screen
completed in 11.80 seconds without deadline skips; all output digests match
the local F screen. The full CPU table is in cpu-comparison.csv.

Unmodified control paths mostly changed by less than 0.2%; filter_batch1
changed by -1.2% and stateful_100000keys by +1.0%. These paths do not execute
the modified count logic. One paired CPU run does not establish that these
small differences are caused by this fix. The tiny queue_stalled-10 case
also remains about 1% above A; this report resolves the sparse-count case,
not a claim that every workload must improve over every baseline.

Keep F on the fork integration branch. It removes known redundant operations
and preserves the dense case, without an input-density heuristic. This run
does not change upstream PRs and does not establish a new memory reduction.
Sparse 1M native peak RSS remains about 546 MiB, including prepared input.

F workflow: https://github.com/cheeeee/bytewax/actions/runs/39526572685
F CPU run: https://app.codspeed.io/cheeeee/bytewax/runs/6ad8161dcc2a3c65387d817b
E CPU run: https://app.codspeed.io/cheeeee/bytewax/runs/6ad7f409549063d964020050
A CPU runs (disjoint parts of the same workload):
  https://app.codspeed.io/cheeeee/bytewax/runs/6ad7f3de549063d96402004a
  https://app.codspeed.io/cheeeee/bytewax/runs/6ad7f69d1643291516080250

Options considered
------------------
1. Sample a batch prefix, then bypass grouping if few slices repeat. This
   is cheap on sparse input, but a sparse prefix can precede a dense suffix.
   It introduces a tuning threshold and can discard profitable reuse.
2. Combine adjacent slices only. This avoids a grouping dictionary, but
   sacrifices reuse when equal slices are interleaved by timestamp disorder.
3. Reuse the exact window bounds already computed for grouping. Chosen:
   it removes redundant work without estimating density or changing grouping.

Previously each entry computed its first/last window IDs, then each group
called open_for(timestamp), recalculating the same IDs, allocating a list,
opening its windows and walking that list again to update counts. When every
group contained one event, this repetition outweighed the grouping benefit.

The new code stores a count per bounds pair, then opens and updates windows
directly from range(first, last + 1). It creates metadata only for new windows,
retains insertion order and retains the ordinary per-window integer snapshots.
It also removes the per-event (timestamp, count) tuple and per-window cast call.
The specialization still applies only to the exact built-in sliding windower
and built-in counter. Custom windower callbacks and the scalar one-item path
remain as before. No public API, clock semantics or persisted-state format
changes are required.

Native confirmation
-------------------
count-native.json contains 52 sequential fresh-process measurements on macOS
ARM64 / Python 3.12.13, completed in 22.76 seconds with a 55-second budget.
Input sizes: 10, 100, 10,000, 100,000 and 1,000,000 for both dense and sparse
count + real FileSink. Three samples per variant/case except one at 1M.
Before/after order alternates for repeated cases. Output digests match.

Sparse count action seconds (before -> after):
     10,000: 0.047331 -> 0.039660, 16.2% less time
    100,000: 0.476324 -> 0.381946, 19.8% less time
  1,000,000: 5.317955 -> 4.342277, 18.3% less time (one sample)
Dense count:
     10,000: 0.007994 -> 0.007643
    100,000: 0.068197 -> 0.068388
  1,000,000: 0.683360 -> 0.651644 (one sample)
The 0.3% increase at dense 100k is not evidence of a reproducible regression.
Small inputs spend about 1 ms per action; single-digit percentage changes
there should not be interpreted as established gains or regressions.

native-before.json and native-after.json separately contain the unchanged
17-case shortlist. Both completed and all output digests match. Those first
screens are not mixed into the medians above. Binary SHA256 is identical:
this fix changes only Python logic. Setup and validation are untimed; action
time includes runtime startup and FileSink flush/fsync. RSS includes prepared
input, so this is not a measure of operator-only state or saturated disk I/O.

Profiling evidence
------------------
On the sparse 10k case, cProfile counts show open_for/intersects calls falling
from 10,000 each to zero. Metadata construction remains 10,001 in both cases.
The total typing.cast count falls from 40,012 to 20,032 (the rest belongs to
other pipeline code). See profile-before.json and profile-after.json. Profiler
wall times are not used as performance results.

Validation and measurement controls
----------------------------------
350 Python tests passed; 18 real-Kafka tests skipped because no broker was
configured. This includes 53 focused window tests, randomized scalar-reference
comparisons, snapshot compatibility and three new density-transition cases.
Ruff and mypy (23 source files) passed. Public windowing signatures are
unchanged. Rust sources are unchanged from the previous validated version.

CodSpeed uses the same 15 cases, inputs and fixed epoch as the original series:
workload SHA256 eecf2106c699d9b2c1e6f30451097d4038335fa254a3cd9ca42f78e6dfe8c0ec.
The workflow retains runner 5.5.0, Python 3.12, simulation and fair scheduling.
F's service values are compared with the saved A/E results, not CI wall time.
The full hashes, source-snapshot audit and original run receipt are preserved.
The first dispatch failed in checkout because an abbreviated SHA was treated
as a branch name; no benchmark ran. The manifest records that discarded setup
attempt and the replacement dispatch using the full commit SHA.

Reproduction
------------
Use release extensions built for each checkout. Runtime E is available at
94b2dcbf79e2deef654a089d2d7f085aabd25739; F is at d9f89ed5 above.
Both must use the same native extension because Rust is unchanged.

  python benchmarks/codspeed/count_native.py BEFORE_CHECKOUT AFTER_CHECKOUT \
    --output count-native.json
  python benchmarks/codspeed/collect.py \
    benchmarks/codspeed/results/2026-10-20-count-bounds/manifest.json \
    /tmp/bytewax-count-bounds
  python benchmarks/codspeed/count_report.py \
    benchmarks/codspeed/results/2026-10-20-count-bounds

count_report.py checks matching workloads, source snapshot counts and native
output digests before generating the CSVs and count-bounds.png. Matplotlib
is needed only for rebuilding that plot.
