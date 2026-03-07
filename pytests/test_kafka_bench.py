"""Kafka produce/consume benchmarks.

Measures raw confluent-kafka and bytewax connector throughput against
a real Kafka broker. Requires a running Kafka instance; skipped when
``TEST_KAFKA_BROKER`` is not set.

Run::

    TEST_KAFKA_BROKER=localhost:19092 \
        pytest pytests/test_kafka_bench.py --benchmark-only -v
"""

import os
import uuid
from concurrent.futures import wait

import bytewax.operators as op
from bytewax.connectors.kafka import KafkaSink, KafkaSinkMessage, KafkaSource
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSink, TestingSource, run_main
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark

pytestmark = mark.skipif(
    "TEST_KAFKA_BROKER" not in os.environ,
    reason="Set `TEST_KAFKA_BROKER` env var to run",
)

KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost:19092")
KAFKA_CONFIG = {"bootstrap.servers": KAFKA_BROKER}

N_MESSAGES = 10_000
PAYLOAD = f"msg-{'x' * 120}".encode()  # ~128 bytes


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _produce_messages(topic, n):
    """Produce *n* messages to *topic* and flush."""
    producer = Producer(KAFKA_CONFIG)
    for i in range(n):
        producer.produce(topic, value=PAYLOAD, key=f"k{i}".encode())
        if i % 1000 == 0:
            producer.poll(0)
    producer.flush(timeout=30)


def _consume_all(topic, n):
    """Consume *n* messages from *topic* using a unique consumer group."""
    config = {
        **KAFKA_CONFIG,
        "group.id": f"bench-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.partition.eof": "true",
        "enable.auto.commit": "false",
    }
    consumer = Consumer(config)
    consumer.subscribe([topic])
    count = 0
    while count < n:
        msg = consumer.poll(timeout=10.0)
        if msg is None:
            break
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                break
            continue
        count += 1
    consumer.close()
    assert count == n, f"Expected {n} messages, got {count}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@fixture
def bench_topic(request):
    """Ephemeral single-partition Kafka topic, deleted on teardown."""
    client = AdminClient(KAFKA_CONFIG)
    name = f"bench_{request.node.name}_{uuid.uuid4().hex[:8]}"
    wait(
        client.create_topics(
            [NewTopic(name, num_partitions=1, replication_factor=1)],
            operation_timeout=5.0,
        ).values()
    )
    yield name
    wait(client.delete_topics([name], operation_timeout=5.0).values())


@fixture
def prepopulated_topic(bench_topic):
    """Topic pre-loaded with N_MESSAGES for consume benchmarks."""
    _produce_messages(bench_topic, N_MESSAGES)
    return bench_topic


# ---------------------------------------------------------------------------
# Raw confluent-kafka benchmarks
# ---------------------------------------------------------------------------


def test_kafka_produce_10k(benchmark, bench_topic):
    """Raw produce: 10k messages via confluent_kafka.Producer."""
    producer = Producer(KAFKA_CONFIG)

    def run():
        for i in range(N_MESSAGES):
            producer.produce(bench_topic, value=PAYLOAD, key=f"k{i}".encode())
            if i % 1000 == 0:
                producer.poll(0)
        producer.flush(timeout=30)

    benchmark(run)


def test_kafka_consume_10k(benchmark, prepopulated_topic):
    """Raw consume: 10k messages via confluent_kafka.Consumer."""
    benchmark(_consume_all, prepopulated_topic, N_MESSAGES)


# ---------------------------------------------------------------------------
# Bytewax connector benchmarks
# ---------------------------------------------------------------------------


def test_bytewax_kafka_source_10k(benchmark, prepopulated_topic):
    """Bytewax KafkaSource: read 10k messages through a dataflow."""

    def run():
        out = []
        flow = Dataflow("kafka_source_bench")
        s = op.input(
            "inp",
            flow,
            KafkaSource(
                [KAFKA_BROKER],
                [prepopulated_topic],
                tail=False,
                add_config=KAFKA_CONFIG,
            ),
        )
        op.output("out", s, TestingSink(out))
        run_main(flow)
        assert len(out) == N_MESSAGES

    benchmark(run)


def test_bytewax_kafka_sink_10k(benchmark, bench_topic):
    """Bytewax KafkaSink: write 10k messages through a dataflow."""
    messages = [
        KafkaSinkMessage(
            key=f"k{i}".encode(),
            value=PAYLOAD,
        )
        for i in range(N_MESSAGES)
    ]

    def run():
        flow = Dataflow("kafka_sink_bench")
        s = op.input("inp", flow, TestingSource(messages))
        op.output(
            "out",
            s,
            KafkaSink([KAFKA_BROKER], bench_topic, add_config=KAFKA_CONFIG),
        )
        run_main(flow)

    benchmark(run)
