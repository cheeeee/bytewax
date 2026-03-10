import os
import re
import uuid
from concurrent.futures import wait
from typing import Tuple

import bytewax.operators as op
from bytewax.connectors.kafka import (
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
    StatefulKafkaSink,
)
from bytewax.connectors.kafka import operators as kop
from bytewax.dataflow import Dataflow
from bytewax.errors import BytewaxRuntimeError
from bytewax.testing import TestingSink, TestingSource, poll_next_batch, run_main
from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    Producer,
    TopicPartition,
)
from confluent_kafka.admin import AdminClient, NewTopic
from pytest import fixture, mark, raises

pytestmark = mark.skipif(
    "TEST_KAFKA_BROKER" not in os.environ,
    reason="Set `TEST_KAFKA_BROKER` env var to run",
)
KAFKA_BROKER = os.environ.get("TEST_KAFKA_BROKER", "localhost")
CLUSTER_API_KEY = os.environ.get("CLUSTER_API_KEY")
CLUSTER_API_SECRET = os.environ.get("CLUSTER_API_SECRET")

if CLUSTER_API_KEY is not None and CLUSTER_API_SECRET is not None:
    config = {
        "bootstrap.servers": KAFKA_BROKER,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": CLUSTER_API_KEY,
        "sasl.password": CLUSTER_API_SECRET,
        "debug": "all",
    }
else:
    config = {
        "bootstrap.servers": KAFKA_BROKER,
    }


@fixture
def tmp_topic(request):
    client = AdminClient(config)
    topic_name = f"pytest_{request.node.name}_{uuid.uuid4()}"
    wait(
        # 3 partitions.
        client.create_topics([NewTopic(topic_name, 3)], operation_timeout=5.0).values()
    )
    yield topic_name
    wait(client.delete_topics([topic_name], operation_timeout=5.0).values())


tmp_topic1 = tmp_topic
tmp_topic2 = tmp_topic


def as_k_v(m: KafkaSourceMessage) -> Tuple[bytes, bytes]:
    return m.key, m.value


def test_input(tmp_topic1, tmp_topic2):
    topics = [tmp_topic1, tmp_topic2]
    producer = Producer(config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key)
            inp.append((key, value))
    producer.flush()
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp", flow, KafkaSource([KAFKA_BROKER], topics, tail=False, add_config=config)
    )
    vals = op.map("vals", s, as_k_v)
    op.output("out", vals, TestingSink(out))

    run_main(flow)

    assert sorted(out) == sorted(inp)


def test_input_resume_state(tmp_topic):
    topics = [tmp_topic]
    partition = 0
    producer = Producer(config)
    inp = []
    for i, topic in enumerate(topics):
        for j in range(3):
            key = f"key-{i}-{j}".encode()
            value = f"value-{i}-{j}".encode()
            producer.produce(topic, value, key, partition=partition)
            inp.append((key, value))
    producer.flush()

    inp = KafkaSource(
        [KAFKA_BROKER], topics, batch_size=1, tail=False, add_config=config
    )
    part = inp.build_part("test", f"{partition}-{tmp_topic}", None)
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-0", b"value-0-0")]
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-1", b"value-0-1")]
    resume_state = part.snapshot()
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    part.close()

    inp = KafkaSource([KAFKA_BROKER], topics, tail=False, add_config=config)
    part = inp.build_part("test", f"{partition}-{tmp_topic}", resume_state)
    assert part.snapshot() == resume_state
    assert list(map(as_k_v, poll_next_batch(part))) == [(b"key-0-2", b"value-0-2")]
    with raises(StopIteration):
        poll_next_batch(part)
    part.close()


def test_input_raises_on_topic_not_exist():
    out = []

    flow = Dataflow("test_df")
    s = op.input(
        "inp",
        flow,
        KafkaSource([KAFKA_BROKER], ["missing-topic"], tail=False, add_config=config),
    )
    op.output("out", s, TestingSink(out))

    with raises(BytewaxRuntimeError):
        run_main(flow)


def test_input_raises_on_str_brokers(tmp_topic):
    expect = "brokers must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource(KAFKA_BROKER, [tmp_topic], tail=False)


def test_input_raises_on_str_topics(tmp_topic):
    expect = "topics must be an iterable and not a string"
    with raises(TypeError, match=re.escape(expect)):
        KafkaSource([KAFKA_BROKER], tmp_topic, tail=False)


def test_output(tmp_topic):
    flow = Dataflow("test_df")

    inp = [
        KafkaSinkMessage(b"key-0-0", b"value-0-0", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-1", b"value-0-2", topic=tmp_topic),
        KafkaSinkMessage(b"key-0-2", b"value-0-2", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([KAFKA_BROKER], tmp_topic, add_config=config))

    run_main(flow)

    group_config = config.copy()
    group_config["group.id"] = "BYTEWAX_UNIT_TEST"
    # Don't leave around a consumer group for this.
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(tmp_topic)
    topic_metadata = cluster_metadata.topics[tmp_topic]
    # Assign does not activate consumer grouping.
    consumer.assign(
        [
            TopicPartition(tmp_topic, i, OFFSET_BEGINNING)
            for i in topic_metadata.partitions
        ]
    )
    out = []
    for msg in consumer.consume(num_messages=100, timeout=5.0):
        if msg.error() is not None and msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        assert msg.error() is None
        out.append((msg.key(), msg.value()))
    consumer.close()

    assert sorted(out) == sorted(list(map(as_k_v, inp)))


# --- Delivery callbacks integration tests ---


def _consume_topic(topic, expected_count, timeout=10.0):
    """Helper: consume all messages from a topic and return (key, value) pairs."""
    group_config = config.copy()
    group_config["group.id"] = "BYTEWAX_UNIT_TEST"
    group_config["enable.auto.commit"] = "false"
    group_config["enable.partition.eof"] = "true"
    consumer = Consumer(group_config)
    cluster_metadata = consumer.list_topics(topic)
    topic_metadata = cluster_metadata.topics[topic]
    consumer.assign(
        [TopicPartition(topic, i, OFFSET_BEGINNING) for i in topic_metadata.partitions]
    )
    out = []
    eof_partitions = set()
    total_partitions = len(topic_metadata.partitions)
    # Keep consuming until we have seen EOF on all partitions.
    while len(eof_partitions) < total_partitions:
        msgs = consumer.consume(num_messages=100, timeout=timeout)
        if not msgs:
            break
        for msg in msgs:
            err = msg.error()
            if err is not None and err.code() == KafkaError._PARTITION_EOF:
                eof_partitions.add(msg.partition())
                continue
            assert msg.error() is None
            out.append((msg.key(), msg.value()))
    consumer.close()
    return out


def test_output_with_delivery_callbacks(tmp_topic):
    """Produce messages via KafkaSink (which now uses delivery callbacks).

    Verify all messages arrive at the broker, confirming no regression
    with the on_delivery callback mechanism in _produce_batch.
    """
    flow = Dataflow("test_df")

    inp = [
        KafkaSinkMessage(b"cb-key-0", b"cb-value-0", topic=tmp_topic),
        KafkaSinkMessage(b"cb-key-1", b"cb-value-1", topic=tmp_topic),
        KafkaSinkMessage(b"cb-key-2", b"cb-value-2", topic=tmp_topic),
        KafkaSinkMessage(b"cb-key-3", b"cb-value-3", topic=tmp_topic),
        KafkaSinkMessage(b"cb-key-4", b"cb-value-4", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output("out", s, KafkaSink([KAFKA_BROKER], tmp_topic, add_config=config))

    run_main(flow)

    out = _consume_topic(tmp_topic, len(inp))
    expected = [(m.key, m.value) for m in inp]
    assert sorted(out) == sorted(expected)


def test_output_delivery_error_raises(tmp_topic):
    """Produce an oversized message that exceeds message.max.bytes.

    The delivery callback should detect the failure and raise
    KafkaProduceError.
    """
    # Restrict max message size so that our payload triggers a delivery error.
    sink_config = config.copy()
    sink_config["message.max.bytes"] = "1000"

    flow = Dataflow("test_df")
    # Create a value larger than 1000 bytes to exceed the limit.
    large_value = b"x" * 2000
    inp = [
        KafkaSinkMessage(b"too-big-key", large_value, topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    op.output(
        "out",
        s,
        KafkaSink([KAFKA_BROKER], tmp_topic, add_config=sink_config),
    )

    with raises(BytewaxRuntimeError):
        run_main(flow)


# --- StatefulKafkaSink integration tests ---


def test_stateful_sink_list_parts(tmp_topic):
    """Create StatefulKafkaSink with a real broker and verify list_parts().

    The tmp_topic fixture creates 3 partitions, so we expect partition
    strings "0-<topic>", "1-<topic>", "2-<topic>".
    """
    sink = StatefulKafkaSink([KAFKA_BROKER], [tmp_topic], add_config=config)
    parts = sink.list_parts()
    expected = [f"{i}-{tmp_topic}" for i in range(3)]
    assert sorted(parts) == sorted(expected)


def test_stateful_sink_list_parts_multi_topic(tmp_topic1, tmp_topic2):
    """Create StatefulKafkaSink with two topics and verify list_parts().

    Each topic has 3 partitions, so we expect 6 total partition strings
    spanning both topics.
    """
    sink = StatefulKafkaSink(
        [KAFKA_BROKER], [tmp_topic1, tmp_topic2], add_config=config
    )
    parts = sink.list_parts()
    expected = [f"{i}-{t}" for t in [tmp_topic1, tmp_topic2] for i in range(3)]
    assert sorted(parts) == sorted(expected)


def test_stateful_sink_produce_and_consume(tmp_topic):
    """Build a partition from StatefulKafkaSink, write messages, and consume.

    Verify the messages actually land in Kafka and are readable.
    """
    sink = StatefulKafkaSink([KAFKA_BROKER], [tmp_topic], add_config=config)
    part = sink.build_part("test", f"0-{tmp_topic}", None)

    messages = [
        KafkaSinkMessage(key=b"k1", value=b"v1", topic=tmp_topic),
        KafkaSinkMessage(key=b"k2", value=b"v2", topic=tmp_topic),
        KafkaSinkMessage(key=b"k3", value=b"v3", topic=tmp_topic),
    ]
    part.write_batch(messages)
    part.close()

    out = _consume_topic(tmp_topic, len(messages))
    expected = [(m.key, m.value) for m in messages]
    assert sorted(out) == sorted(expected)


def test_stateful_sink_snapshot_lifecycle(tmp_topic):
    """Build a partition, write messages, and verify snapshot() tracks count."""
    sink = StatefulKafkaSink([KAFKA_BROKER], [tmp_topic], add_config=config)
    part = sink.build_part("test", f"0-{tmp_topic}", None)

    # Initial state: no messages written yet.
    assert part.snapshot() == 0

    messages = [
        KafkaSinkMessage(key=b"k1", value=b"v1", topic=tmp_topic),
        KafkaSinkMessage(key=b"k2", value=b"v2", topic=tmp_topic),
        KafkaSinkMessage(key=b"k3", value=b"v3", topic=tmp_topic),
        KafkaSinkMessage(key=b"k4", value=b"v4", topic=tmp_topic),
        KafkaSinkMessage(key=b"k5", value=b"v5", topic=tmp_topic),
    ]
    part.write_batch(messages)

    assert part.snapshot() == 5
    part.close()


def test_stateful_sink_resume(tmp_topic):
    """Build a partition with resume_state=5, write 3 messages, snapshot returns 8."""
    sink = StatefulKafkaSink([KAFKA_BROKER], [tmp_topic], add_config=config)
    part = sink.build_part("test", f"0-{tmp_topic}", 5)

    # After resume with state 5, snapshot should reflect the resume value.
    assert part.snapshot() == 5

    messages = [
        KafkaSinkMessage(key=b"r1", value=b"rv1", topic=tmp_topic),
        KafkaSinkMessage(key=b"r2", value=b"rv2", topic=tmp_topic),
        KafkaSinkMessage(key=b"r3", value=b"rv3", topic=tmp_topic),
    ]
    part.write_batch(messages)

    assert part.snapshot() == 8
    part.close()


def test_stateful_output_operator(tmp_topic):
    """Full dataflow: TestingSource -> kop.stateful_output() -> consume from Kafka.

    Verify messages produced via the stateful_output operator are
    readable from the broker.
    """
    flow = Dataflow("test_df")
    inp = [
        KafkaSinkMessage(b"so-key-0", b"so-val-0", topic=tmp_topic),
        KafkaSinkMessage(b"so-key-1", b"so-val-1", topic=tmp_topic),
        KafkaSinkMessage(b"so-key-2", b"so-val-2", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    kop.stateful_output(
        "kafka_out",
        s,
        brokers=[KAFKA_BROKER],
        topics=[tmp_topic],
        add_config=config,
    )

    run_main(flow)

    out = _consume_topic(tmp_topic, len(inp))
    expected = [(m.key, m.value) for m in inp]
    assert sorted(out) == sorted(expected)


def test_stateful_output_multi_topic(tmp_topic1, tmp_topic2):
    """Produce to 2 topics via stateful_output; verify messages land correctly."""
    flow = Dataflow("test_df")
    inp = [
        KafkaSinkMessage(b"mt-key-0", b"mt-val-0", topic=tmp_topic1),
        KafkaSinkMessage(b"mt-key-1", b"mt-val-1", topic=tmp_topic1),
        KafkaSinkMessage(b"mt-key-2", b"mt-val-2", topic=tmp_topic2),
        KafkaSinkMessage(b"mt-key-3", b"mt-val-3", topic=tmp_topic2),
    ]
    s = op.input("inp", flow, TestingSource(inp))
    kop.stateful_output(
        "kafka_out",
        s,
        brokers=[KAFKA_BROKER],
        topics=[tmp_topic1, tmp_topic2],
        add_config=config,
    )

    run_main(flow)

    out1 = _consume_topic(tmp_topic1, 2)
    out2 = _consume_topic(tmp_topic2, 2)

    expected1 = [(m.key, m.value) for m in inp if m.topic == tmp_topic1]
    expected2 = [(m.key, m.value) for m in inp if m.topic == tmp_topic2]

    assert sorted(out1) == sorted(expected1)
    assert sorted(out2) == sorted(expected2)


def test_stateful_output_custom_key_fn(tmp_topic):
    """stateful_output with custom key_fn uses it for routing."""
    flow = Dataflow("test_df")
    inp = [
        KafkaSinkMessage(b"k1", b"v1", topic=tmp_topic),
        KafkaSinkMessage(b"k2", b"v2", topic=tmp_topic),
        KafkaSinkMessage(b"k3", b"v3", topic=tmp_topic),
    ]
    s = op.input("inp", flow, TestingSource(inp))

    def custom_key(msg):
        return f"{msg.topic}:{msg.value or b''}"

    kop.stateful_output(
        "kafka_out",
        s,
        brokers=[KAFKA_BROKER],
        topics=[tmp_topic],
        add_config=config,
        key_fn=custom_key,
    )

    run_main(flow)

    out = _consume_topic(tmp_topic, len(inp))
    expected = [(m.key, m.value) for m in inp]
    assert sorted(out) == sorted(expected)
