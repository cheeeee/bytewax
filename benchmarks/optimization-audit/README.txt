Optimization transfer audit — 20 October 2026
============================================

Verdict
-------
Do not transfer the original A-F package unchanged. The reviewed transfer
runtime is 19047985748b0bbad902db29e4d1152afef5a549 on the fork's
main-upstream-clone branch. It retains the window/count algorithms, Python
stateful batching with corrected object lifetime, and a log-aware span guard.
It restores unconditional histogram recording and ALL baseline Rust stateful
maps, sets, grouping, lookup and snapshot traversal.

This narrowed diff passes the compatibility checks below. No Python API or
snapshot format change is needed. This is a recommendation to transfer that
specific diff, not an assertion that every workload is faster or that the
pre-existing monitoring subsystem has no bugs. No upstream PR was modified.
GitHub CI and CodSpeed were not rerun for this final selection; historical
A-F CodSpeed results must not be presented as measurements of this runtime.

Baseline A is 22753ccc7eaa45e2c300cc3da8b9d5012c25f61f: merged fork main and
upstream main plus the common benchmark harness, NOT pristine upstream main.
Original F is runtime d9f89ed57a9abe98e6e97374512e6eef6f6105f6 (report HEAD
3b29e0d72830f7fd42453809f3bc83541c186e6b).
To view only runtime source changes:
  git diff 22753ccc 19047985 -- Cargo.toml pysrc src

Confirmed defects and decisions
------------------------------
1. Original #620 timer bypass loses duration histograms when a Rust host
   installs an OpenTelemetry provider with global::set_meter_provider rather
   than Bytewax's initialize_metrics. The private AtomicBool stays false even
   though the instrument is active. Process-isolated reproduction: the
   built-in provider records two observations; the external provider records
   none. Removed the flag/bypass. with_timer always measures and records again.
   A Python OTel provider is a separate SDK; this failure concerns Rust's
   provider. The normal Bytewax Prometheus setup was not broken by this flag.

2. Original #620 span bypass loses Rust log records with a log logger and no
   tracing subscriber. has_been_set() does not detect a logger. tracing's log
   compatibility normally emits span creation/enter/exit/close records; the
   bypass emitted only the body event. The new guard also checks log's maximum
   level. Explicitly enable tracing's log feature (already enabled transitively
   in the measured dependency graph). Dynamic log levels, late setup, global
   and scoped subscribers, worker-thread propagation and filtering pass.
   has_been_set is sticky: a previously installed subscriber can eliminate the
   optimization after removal, but does not suppress telemetry.

3. Python stateful batching kept self.state unchanged until batch completion.
   Replacing a large initial state retained it unnecessarily; a weakref or
   output generator could observe different lifetime. Fixed to release a
   replaced state before consuming outputs, and a discarded state after output
   consumption, matching the original item wrapper. Tests compare both paths
   with retained replacements and discards. Batching already existed around
   the old item loop; this change does not introduce a new batch-wide GIL hold.

4. Rust #617 persistent HashMaps/HashSet trade lookup speed for retained bucket
   memory, scans proportional to capacity, and sorting at notifications/EOF/
   snapshots. With one million keys created then all but one discarded, the
   original F used about 533 MiB peak versus 331 MiB for A. Its finalization
   took 93 ms versus about 29-31 ms for A. No data loss was observed, but this
   is a significant resource/latency tradeoff. Shrinking sparse maps restored
   timer-tail throughput without fixing peak RSS or EOF sorting. A partial
   rollback retained temporary hash grouping; its million-key screen was
   12.814 s versus 11.357 s for A (one sample, not causal attribution).
   Rather than ship an inadequately justified Rust subset, the transfer
   version restores all Rust stateful data structures/processing to A.
   Python batching remains independently useful.

Actual telemetry delivery checks
-------------------------------
observe.py starts real Python dataflows via cli_main, uses a real FileSink,
enables the HTTP API, scrapes /metrics, and receives/decodes OTLP gRPC and
Jaeger UDP. A separate source keeps the process alive after the measured
branch reaches EOF. Scraping waits for EOF on all four keys in BOTH stateful
steps, not just file output; otherwise a legitimately not-yet-called EOF
method can look like a missing histogram.

Eight configurations on each of A and transfer:
  metrics off, tracing off, one worker;
  metrics on, tracing off, one and two workers;
  metrics on, TRACE console logging, one worker;
  metrics on, OTLP sampling=1, one and two workers;
  metrics on, OTLP sampling=0, one worker;
  metrics on, Jaeger, one worker.

Each flow verifies 100 file records against a separate sum oracle and the
custom Python counter against 100 mapper calls. Histogram sample counts for
on_batch, on_notify, on_eof, notify_at and snapshot must equal independently
counted Python invocations. Eight core duration histogram families are checked
with step_id/worker_index labels. Both workers are represented when configured.
Unique metric identities, span field schemas and output digests match A.
OTLP parent links and operator/partition attributes are present; sampling=0
exports no spans. Counts of scheduling spans and duration values are naturally
time-dependent and are not expected to match numerically. Receiving UDP spans
is not a guarantee of lossless UDP delivery under arbitrary load.

Two independently reproduced, pre-existing monitoring defects remain
-------------------------------------------------------------------
These occur on A as well as the transfer version; they are not introduced by
the performance delta and were not silently fixed by changing metric schemas.

Recovery metrics: three recovery writers initialize
bytewax_partd_write_duration_seconds with identical part_id/worker_id/scope
labels. Captured counts were 1, 53 and 10 for the same series identity.
Prometheus 3.15.0 reported up=1 but warned that it dropped 38 conflicting
samples per scrape. A query returned count=1, not the combined 64. Thus HTTP
success and target health alone do NOT validate recovery duration metrics.
scrape.py replays exact captured bytes into a real Prometheus server; raw
metrics, its logs, query results and config are saved. promtool check metrics
alone did not detect the ingestion loss.

Correct permanent remedies: distinguish writers with a label such as name,
or share a single instrument if an aggregate histogram is intended. Adding a
label changes the metric schema/cardinality and needs explicit consideration
for dashboards. The tested temporary workaround drops only this broken family:

  metric_relabel_configs:
    - source_labels: [__name__]
      regex: bytewax_partd_write_duration_seconds_(bucket|sum|count)
      action: drop

This deliberately removes recovery-write timings; it does not restore them.
Other core durations/counters remain available. No conflicting-sample warning
was emitted for the workaround target.

Jaeger: the default 512-span export batch produced UDP payloads around
156-173 KB, over the exporter's 65 KB limit, on baseline A under full tracing.
Those batches were dropped with explicit exporter errors. The validated
configuration uses OTEL_BSP_MAX_EXPORT_BATCH_SIZE=16, plus a 50 ms schedule
delay and a queue of 65536 for the test. Batch 16 exports successfully here;
it is not a universal size guarantee for arbitrarily large span attributes.
OTLP gRPC is the available alternative without UDP's packet-size restriction.
No tracing backend defaults were changed by this audit.

Compatibility and validation
----------------------------
352 Python tests/benchmark correctness cases passed; 18 real-Kafka integration
tests skipped because TEST_KAFKA_BROKER was unset. Broker-free Kafka tests and
benchmarks ran. This is not a real Kafka-broker transport validation.
10 Rust unit tests and one Rust doctest passed; four existing doctests ignored.
Ruff, mypy (23 package files and 70 test/doc files), Clippy and pre-commit passed.
76 public signatures in the changed Python modules match A; the extension
stub is byte-identical. Tests cover window ties/disorder/late events, custom
windower callbacks, notification order, metadata and snapshot equivalence.

recovery.py checks a real SQLite interruption after 4096 events, including a
retained ordered queue above its promotion threshold, then resumes to 8192.
A->transfer and transfer->A produce byte-identical FileSink results to an
uninterrupted A run (8192 state outputs, 18 count outputs, one ordered fold).
The serialized dataflow topology also matches. The GIL remains in use.

Performance of the selected transfer version
--------------------------------------------
Native release builds, macOS arm64, Python 3.12.13, Rust 1.86.0. Fresh processes;
paired/interleaved builds; verification outside timing. Timers include runtime
startup; RSS includes prepared Python inputs. Small gains are screening results,
not confidence intervals or p99 latency claims. No concurrent benchmark jobs.

Representative seconds (A -> transfer):
  Dense count + FileSink, 10,000:       0.185916 -> 0.007646  (24.3x; one sample)
  Dense count + FileSink, 100,000:      1.868799 -> 0.064264  (29.1x; one sample)
  Dense count + FileSink, 1,000,000:   18.238235 -> 0.728173  (25.0x; one sample)
  Mixed queue, 100,000:                0.364231 -> 0.226465  (1.61x; median of 3)
  Stateful, 1,000 keys / 100,000:       0.088829 -> 0.082250  (1.08x; median of 3)
  Stateful, 100,000 keys / 100,000:     0.249077 -> 0.218473  (1.14x; median of 3)

With metrics enabled and a normal ERROR logging subscriber, actual cli_main:
  Dense count + FileSink, 100,000:      1.814571 -> 0.068361  (26.5x; median of 3)
  Stateful, 1,000 keys / 100,000:       0.152419 -> 0.146292  (1.04x; median of 3)
These runs record real Rust metrics but do not include concurrent scrapes or
full TRACE export load. The large window benefit is not from suppressing
instrumentation. Stateful gains under active instrumentation are modest.

Million-key create/discard/tail, final paired run:
  Total:               11.587452 -> 11.430629 s
  Peak RSS:            330.97 -> 331.02 MiB
  RSS at tail:         275.09 -> 274.11 MiB
  1000 tail callbacks: 2.03 -> 2.15 ms
  Finalization:        28.94 -> 33.73 ms
The earlier 533 MiB footprint is gone, but an assertion of universally better
latency would be false. The frequent-checkpoint churn control (100,000 keys,
101001 snapshot calls on both builds) was 0.474714 -> 0.492526 s, 3.8% slower
in one sample. Tiny cases are startup dominated: count(10) was 2.77 -> 3.12 ms.
Treat these as controls/limits; do not market a universal speedup.

The default shortlist includes 10, 100, 10k, 100k and 1M and completed in
30.49 s. Optional metrics-enabled and churn audits completed in 7.97 s and
25.28 s separately. Every invocation has a 55-second deadline and records
timeouts explicitly. An initial combined exploratory run reached its cap;
it was excluded and replaced by these complete groups. Raw rows, sample
counts, output digests, file fingerprints and comparison.csv are included.

Reproduce
---------
Build matching release extensions in each checkout first. Set RUNTIME_PYTHON
to the pinned Bytewax environment's absolute Python path. Use a separate
observer environment with grpcio, opentelemetry-proto, prometheus-client and
thrift so instrumentation dependencies do not alter the runtime environment.

  $RUNTIME_PYTHON benchmarks/optimization-audit/screen.py \
    --python "$RUNTIME_PYTHON" --build A=/absolute/base/pysrc \
    --build transfer=/absolute/current/pysrc --output /tmp/hotpaths.json

Add --group enabled or --group churn for the optional bounded audits.
observe.py accepts the same --python/--build arguments plus --output DIR.
recovery.py uses --python, --before BASE_PYSRC, --after CURRENT_PYSRC and
--directory DIR. scrape.py uses --prometheus BINARY, --before BASE.prom,
--after CURRENT.prom and --directory DIR. Output directories must be fresh.

  PYO3_PYTHON="$RUNTIME_PYTHON" cargo test --no-default-features
  PYTHONPATH=/absolute/current/pysrc "$RUNTIME_PYTHON" -m pytest \
    --benchmark-disable -rs pytests benchmarks/codspeed/test_hotpaths.py

Algorithmic background remains unchanged: block min/max pruning relates to
Moerkotte, Small Materialized Aggregates (VLDB 1998),
https://www.vldb.org/conf/1998/p476.pdf ; count sharing relates to Li et al.,
No Pane, No Gain (SIGMOD Record 2005),
https://sigmodrecord.org/?download_id=5003&smd_process_download=1 .
These are related principles, not claims of identical implementations. ISOP
is not involved. Batching and instrumentation are separate implementation work.
