"""Compare real Prometheus, OTLP, Jaeger and file output across builds.

Controller deps: grpcio, opentelemetry-proto, prometheus-client, thrift.
The child runs in an independent, unchanged Bytewax runtime environment.
"""

import argparse
import concurrent.futures
import hashlib
import json
import os
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

import grpc
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2 as pb
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc as rpc
from prometheus_client.parser import text_string_to_metric_families
from thrift.Thrift import TType
from thrift.protocol.TCompactProtocol import TCompactProtocol
from thrift.transport.TTransport import TMemoryBuffer


class Receiver(rpc.TraceServiceServicer):
    def __init__(self):
        self.spans = []

    def Export(self, request, context):
        for resource in request.resource_spans:
            for scope in resource.scope_spans:
                self.spans.extend(scope.spans)
        return pb.ExportTraceServiceResponse()


def read_thrift(protocol, kind):
    if kind == TType.STRUCT:
        protocol.readStructBegin()
        fields = {}
        while True:
            _, field_type, field_id = protocol.readFieldBegin()
            if field_type == TType.STOP:
                break
            fields[str(field_id)] = read_thrift(protocol, field_type)
            protocol.readFieldEnd()
        protocol.readStructEnd()
        return fields
    if kind == TType.LIST:
        element_type, count = protocol.readListBegin()
        result = [read_thrift(protocol, element_type) for _ in range(count)]
        protocol.readListEnd()
        return result
    return {
        TType.STRING: protocol.readString,
        TType.BOOL: protocol.readBool,
        TType.BYTE: protocol.readByte,
        TType.I16: protocol.readI16,
        TType.I32: protocol.readI32,
        TType.I64: protocol.readI64,
        TType.DOUBLE: protocol.readDouble,
    }[kind]()


def udp_receiver(sock, batches, done):
    sock.settimeout(0.1)
    while not done.is_set():
        try:
            packet = sock.recv(65536)
        except socket.timeout:
            continue
        protocol = TCompactProtocol(TMemoryBuffer(packet))
        name, _, _ = protocol.readMessageBegin()
        assert name == "emitBatch"
        batches.append(read_thrift(protocol, TType.STRUCT)["1"])


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def metrics(text):
    samples = []
    for family in text_string_to_metric_families(text):
        for sample in family.samples:
            if sample.name.startswith(("bytewax_", "audit_")):
                samples.append(
                    {
                        "name": sample.name,
                        "labels": sample.labels,
                        "value": sample.value,
                    }
                )
    return samples


def run(args, name, source, enabled, trace, workers):
    directory = args.output / f"{name}-metrics{int(enabled)}-{trace}-w{workers}"
    directory.mkdir(parents=True)
    receiver = Receiver()
    server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=2))
    rpc.add_TraceServiceServicer_to_server(receiver, server)
    grpc_port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(("127.0.0.1", 0))
    batches, done = [], threading.Event()
    thread = threading.Thread(target=udp_receiver, args=(udp, batches, done))
    thread.start()
    endpoint = (
        f"127.0.0.1:{udp.getsockname()[1]}"
        if trace == "jaeger"
        else f"http://127.0.0.1:{grpc_port}"
    )
    port = free_port()
    env = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("BYTEWAX_", "OTEL_"))
    }
    env.update(
        PYTHONPATH=str(source.resolve()),
        PYTHONHASHSEED="0",
        OTEL_BSP_SCHEDULE_DELAY="50",
        OTEL_BSP_MAX_QUEUE_SIZE="65536",
        OTEL_BSP_MAX_EXPORT_BATCH_SIZE="16",
        BYTEWAX_DATAFLOW_API_PORT=str(port),
        BYTEWAX_DATAFLOW_API_CACHE_PATH=str(directory.resolve()),
    )
    if enabled:
        env["BYTEWAX_DATAFLOW_API_ENABLED"] = "true"
    output = directory / "output.jsonl"
    log = (directory / "process.log").open("w")
    process = subprocess.Popen(
        [
            args.python,
            str(Path(__file__).with_name("flow.py").resolve()),
            "--directory",
            str(directory.resolve()),
            "--workers",
            str(workers),
            "--trace",
            trace,
            "--endpoint",
            endpoint,
        ],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        close_fds=False,
    )
    text = ""
    try:
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            assert process.poll() is None, (directory, process.returncode)
            if output.exists() and len(output.read_text().splitlines()) == 100:
                if enabled:
                    try:
                        text = (
                            urllib.request.urlopen(
                                f"http://127.0.0.1:{port}/metrics",
                                timeout=1,
                            )
                            .read()
                            .decode()
                        )
                    except (urllib.error.URLError, TimeoutError):
                        continue
                    samples = metrics(text)

                    # File output can precede downstream EOF. Wait until all
                    # four keys in BOTH stateful steps have finished, including
                    # keys owned by another worker.
                    def completed(step):
                        return (
                            sum(
                                s["value"]
                                for s in samples
                                if s["name"]
                                == "bytewax_stateful_batch_on_eof_duration_seconds_count"
                                and s["labels"].get("step_id") == step
                            )
                            == 4
                        )

                    if not all(
                        completed(step)
                        for step in (
                            "audit.timers",
                            "audit.sum.stateful_flat_map.stateful.stateful_batch",
                        )
                    ):
                        continue
                break
            time.sleep(0.02)
        else:
            raise AssertionError(f"flow/metrics did not become ready: {directory}")
        if not enabled:
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=0.1)
            except urllib.error.URLError:
                pass
            else:
                raise AssertionError("metrics endpoint enabled unexpectedly")
        (directory / "stop").touch()
        assert process.wait(timeout=10) == 0, directory
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
        log.close()
        server.stop(0).wait()
        done.set()
        thread.join()
        udp.close()
    sums = [0] * 4
    expected = []
    for i in range(100):
        sums[i % 4] += i
        expected.append(sums[i % 4])
    actual = [json.loads(line) for line in output.read_text().splitlines()]
    assert sorted(actual) == sorted(expected)
    samples = metrics(text)
    (directory / "metrics.prom").write_text(text)
    if enabled:
        assert (
            next(s["value"] for s in samples if s["name"] == "audit_events_total")
            == 100
        )
        durations = {
            s["name"]
            for s in samples
            if "duration_seconds_count" in s["name"] and s["value"] > 0
        }
        assert {
            f"bytewax_{name}_duration_seconds_count"
            for name in (
                "inp_part_next_batch",
                "out_part_write_batch",
                "flat_map_batch",
                "stateful_batch_on_batch",
                "stateful_batch_on_notify",
                "stateful_batch_on_eof",
                "stateful_batch_notify_at",
                "snapshot",
            )
        } <= durations, durations
        for sample in samples:
            if "duration_seconds_count" in sample["name"] and not sample[
                "name"
            ].startswith("bytewax_partd_"):
                assert {"step_id", "worker_index"} <= sample["labels"].keys()
        assert (
            sum(
                s["value"]
                for s in samples
                if s["name"] == "bytewax_item_inp_count_total"
                and s["labels"].get("step_id") == "audit.file"
            )
            == 100
        )
        for method in ("on_batch", "on_notify", "on_eof", "notify_at", "snapshot"):
            hist = "snapshot" if method == "snapshot" else "stateful_batch_" + method
            recorded = sum(
                s["value"]
                for s in samples
                if s["name"] == f"bytewax_{hist}_duration_seconds_count"
                and s["labels"].get("step_id") == "audit.timers"
            )
            invoked = sum(
                s["value"]
                for s in samples
                if s["name"] == "audit_logic_callbacks_total"
                and s["labels"].get("method") == method
            )
            assert recorded == invoked > 0, (method, recorded, invoked)
    spans = receiver.spans
    if trace == "jaeger":
        # Jaeger thrift Batch: process=1, spans=2; Span.operationName=5.
        assert batches and all(b["1"]["1"] == "bytewax-audit" for b in batches)
        names = Counter(s["5"] for b in batches for s in b["2"])
    else:
        names = Counter(s.name for s in spans)
    if trace in ("otlp", "jaeger"):
        assert {"operator", "partition", "step"} <= names.keys(), names
    else:
        assert not spans and not batches
    if trace == "otlp":
        ids = {span.span_id for span in spans}
        assert any(span.parent_span_id in ids for span in spans)
        assert any(attr.key == "operator" for span in spans for attr in span.attributes)
        assert any(attr.key == "part_key" for span in spans for attr in span.attributes)
    if trace == "logs":
        assert "Running" in (directory / "process.log").read_text()
    result = {
        "build": name,
        "source": str(source),
        "metrics": enabled,
        "trace": trace,
        "workers": workers,
        "file_records": len(actual),
        "metrics_samples": samples,
        "spans_by_name": dict(names),
        "span_schemas": sorted(
            {
                (span.name, tuple(sorted(a.key for a in span.attributes)))
                for span in spans
            }
        ),
        "output_digest": hashlib.sha256(
            json.dumps(sorted(actual)).encode()
        ).hexdigest(),
        "duplicate_metric_identities": sum(
            count > 1
            for count in Counter(
                (s["name"], tuple(sorted(s["labels"].items()))) for s in samples
            ).values()
        ),
        "status": "emission_checks_passed",
        "note": "Wire emission/callback assertions only; scrape.py separately tests ingestion.",
    }
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(
        f"PASS {directory.name}: {len(samples)} samples, {sum(names.values())} spans",
        flush=True,
    )
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--python", required=True)
    parser.add_argument(
        "--build", action="append", required=True, help="name=path/to/pysrc"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--modes", nargs="+")
    args = parser.parse_args()
    results = []
    for build in args.build:
        name, source = build.split("=", 1)
        for enabled, trace, workers in [
            (False, "off", 1),
            (True, "off", 1),
            (True, "off", 2),
            (True, "logs", 1),
            (True, "otlp", 1),
            (True, "otlp", 2),
            (True, "otlp0", 1),
            (True, "jaeger", 1),
        ]:
            if args.modes and trace not in args.modes:
                continue
            results.append(run(args, name, Path(source), enabled, trace, workers))
    (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")


if __name__ == "__main__":
    main()
