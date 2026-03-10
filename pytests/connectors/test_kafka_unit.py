"""Unit tests for Kafka connector fixes (no broker required)."""

import json
from unittest.mock import MagicMock, patch
from zlib import adler32

import pytest
from bytewax.connectors.kafka import (
    KafkaProduceError,
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    KafkaSourceMessage,
    StatefulKafkaSink,
    _build_producer,
    _KafkaSinkPartition,
    _KafkaSourcePartition,
    _produce_batch,
    _StatefulKafkaSinkPartition,
)
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka import KafkaException
from confluent_kafka.serialization import MessageField, SerializationContext

try:
    from bytewax.connectors.kafka.serde import _parse_avro_schema

    _has_serde = True
except ImportError:
    _has_serde = False


class TestKafkaSinkBufferError:
    """Tests for Fix #522: KafkaSink should handle BufferError gracefully."""

    def test_write_batch_normal(self):
        """Normal produce calls work without error."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
        ]
        partition.write_batch(msgs)

        assert producer.produce.call_count == 2
        producer.flush.assert_called()

    def test_write_batch_buffer_error_retry(self):
        """BufferError triggers flush then retry."""
        producer = MagicMock()
        # First produce raises BufferError, second (retry) succeeds
        producer.produce.side_effect = [BufferError("queue full"), None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        partition.write_batch(msgs)

        # produce called twice: initial + retry
        assert producer.produce.call_count == 2
        # flush called: once for BufferError recovery + once at end
        assert producer.flush.call_count == 2

    def test_write_batch_buffer_error_multiple_messages(self):
        """BufferError on one message doesn't affect others."""
        producer = MagicMock()
        # First msg: produce OK, second msg: BufferError then OK, third msg: OK
        producer.produce.side_effect = [None, BufferError("queue full"), None, None]
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        partition.write_batch(msgs)

        # 3 initial + 1 retry = 4
        assert producer.produce.call_count == 4

    def test_write_batch_with_explicit_topic(self):
        """Messages with explicit topic override default."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, "default-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1", topic="custom-topic")]
        partition.write_batch(msgs)

        producer.produce.assert_called_once()
        call_kwargs = producer.produce.call_args.kwargs
        assert call_kwargs["value"] == b"v1"
        assert call_kwargs["key"] == b"k1"
        assert call_kwargs["headers"] == []
        assert call_kwargs["topic"] == "custom-topic"
        assert call_kwargs["timestamp"] == 0
        assert "on_delivery" in call_kwargs

    def test_write_batch_no_topic_raises(self):
        """Missing topic raises RuntimeError."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, None)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(RuntimeError, match="No topic"):
            partition.write_batch(msgs)


class TestKafkaSourceOAuthPoll:
    """Tests for Fix #541: KafkaSource.list_parts() should poll for OAUTHBEARER."""

    @patch("bytewax.connectors.kafka.AdminClient")
    @patch("bytewax.connectors.kafka._list_parts")
    def test_list_parts_calls_poll_before_list(self, mock_list_parts, mock_admin_cls):
        """poll(0) is called on AdminClient before _list_parts."""
        mock_client = MagicMock()
        mock_admin_cls.return_value = mock_client
        mock_list_parts.return_value = ["0-test-topic"]

        source = KafkaSource(["localhost:9092"], ["test-topic"], tail=False)
        parts = source.list_parts()

        mock_admin_cls.assert_called_once()
        mock_client.poll.assert_called_once_with(0)
        mock_list_parts.assert_called_once_with(mock_client, ["test-topic"])
        assert parts == ["0-test-topic"]

    @patch("bytewax.connectors.kafka.AdminClient")
    @patch("bytewax.connectors.kafka._list_parts")
    def test_list_parts_passes_add_config(self, mock_list_parts, mock_admin_cls):
        """add_config is forwarded to AdminClient."""
        mock_client = MagicMock()
        mock_admin_cls.return_value = mock_client
        mock_list_parts.return_value = []

        add_config = {
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "OAUTHBEARER",
        }
        source = KafkaSource(
            ["broker1:9092"], ["topic1"], tail=False, add_config=add_config
        )
        source.list_parts()

        call_config = mock_admin_cls.call_args[0][0]
        assert call_config["security.protocol"] == "SASL_SSL"
        assert call_config["sasl.mechanisms"] == "OAUTHBEARER"
        assert call_config["bootstrap.servers"] == "broker1:9092"


class TestKafkaGroupIdLeak:
    """Tests for Fix #377: group.id should not leak to AdminClient/Producer."""

    @patch("bytewax.connectors.kafka._list_parts", return_value=["0-test-topic"])
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_strips_group_id(self, mock_admin_cls, mock_list_parts):
        """group.id is stripped from AdminClient config."""
        mock_admin_cls.return_value = MagicMock()

        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"group.id": "my-group", "security.protocol": "SASL_SSL"},
        )
        source.list_parts()

        created_config = mock_admin_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["security.protocol"] == "SASL_SSL"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_ignores_user_group_id(self, mock_partition_cls):
        """User group.id is overridden to BYTEWAX_IGNORED."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"group.id": "my-consumer-group"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["group.id"] == "BYTEWAX_IGNORED"
        assert config["enable.auto.commit"] == "false"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_preserves_other_config(self, mock_partition_cls):
        """Non-group config from add_config is preserved."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"security.protocol": "SASL_SSL", "group.id": "ignored"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["security.protocol"] == "SASL_SSL"
        assert config["group.id"] == "BYTEWAX_IGNORED"

    @patch("bytewax.connectors.kafka._KafkaSourcePartition")
    def test_build_part_enforces_auto_commit_false(self, mock_partition_cls):
        """enable.auto.commit=false cannot be overridden by user config."""
        source = KafkaSource(
            ["localhost:9092"],
            ["test-topic"],
            tail=False,
            add_config={"enable.auto.commit": "true"},
        )
        source.build_part("step-1", "0-test-topic", None)

        config = mock_partition_cls.call_args[0][1]
        assert config["enable.auto.commit"] == "false"

    @patch("bytewax.connectors.kafka.Producer")
    def test_kafka_sink_strips_group_id(self, mock_producer_cls):
        """group.id is stripped from Producer config."""
        mock_producer_cls.return_value = MagicMock()

        sink = KafkaSink(
            ["localhost:9092"],
            "test-topic",
            add_config={"group.id": "my-group", "linger.ms": "100"},
        )
        sink.build("step-1", 0, 1)

        created_config = mock_producer_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["linger.ms"] == "100"


def _make_mock_producer(
    *, delivery_error=None, delivery_topic="test-topic", delivery_partition=0
):
    """Create a mock Producer with callback-capture for _produce_batch tests.

    On flush, triggers all captured on_delivery callbacks with the given
    ``delivery_error`` (None for success) and a mock Message whose
    ``.topic()`` / ``.partition()`` return ``delivery_topic`` / ``delivery_partition``.
    """
    producer = MagicMock()
    callbacks = []

    def mock_produce(**kwargs):
        if "on_delivery" in kwargs:
            callbacks.append(kwargs["on_delivery"])

    producer.produce = MagicMock(side_effect=mock_produce)

    def mock_flush():
        mock_msg = MagicMock()
        mock_msg.topic.return_value = delivery_topic
        mock_msg.partition.return_value = delivery_partition
        for cb in callbacks:
            cb(delivery_error, mock_msg)
        callbacks.clear()

    producer.flush = MagicMock(side_effect=mock_flush)
    return producer


class TestDeliveryCallbacks:
    """Tests for _produce_batch delivery callback error tracking."""

    def test_produce_batch_success(self):
        """All deliveries succeed — no exception raised."""
        producer = _make_mock_producer()

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
        ]
        # Should not raise
        _produce_batch(producer, "test-topic", msgs)

        assert producer.produce.call_count == 2
        producer.flush.assert_called()

    def test_produce_batch_delivery_error(self):
        """A delivery error triggers KafkaProduceError."""
        mock_error = MagicMock()
        mock_error.__str__ = lambda self: "MSG_TIMED_OUT"
        producer = _make_mock_producer(delivery_error=mock_error)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(KafkaProduceError) as exc_info:
            _produce_batch(producer, "test-topic", msgs)

        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0][0] is mock_error
        assert "test-topic[0]" in exc_info.value.errors[0][1]

    def test_produce_batch_multiple_errors(self):
        """Multiple delivery failures are all collected in errors list."""
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                callbacks.append(kwargs["on_delivery"])

        producer.produce = MagicMock(side_effect=mock_produce)

        mock_error_1 = MagicMock()
        mock_error_2 = MagicMock()
        mock_error_3 = MagicMock()

        def mock_flush():
            errors = [mock_error_1, mock_error_2, mock_error_3]
            for i, cb in enumerate(callbacks):
                mock_msg = MagicMock()
                mock_msg.topic.return_value = "test-topic"
                mock_msg.partition.return_value = i
                cb(errors[i] if i < len(errors) else None, mock_msg)
            callbacks.clear()

        producer.flush = MagicMock(side_effect=mock_flush)

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        with pytest.raises(KafkaProduceError) as exc_info:
            _produce_batch(producer, "test-topic", msgs)

        assert len(exc_info.value.errors) == 3
        assert exc_info.value.errors[0][0] is mock_error_1
        assert exc_info.value.errors[1][0] is mock_error_2
        assert exc_info.value.errors[2][0] is mock_error_3

    def test_produce_batch_buffer_error_with_delivery_callback(self):
        """BufferError triggers flush+retry, and delivery callback still works."""
        producer = MagicMock()
        callbacks = []
        produce_call_count = 0

        def mock_produce(**kwargs):
            nonlocal produce_call_count
            produce_call_count += 1
            # First call: BufferError; second call (retry): success
            if produce_call_count == 1:
                msg = "queue full"
                raise BufferError(msg)
            if "on_delivery" in kwargs:
                callbacks.append(kwargs["on_delivery"])

        producer.produce = MagicMock(side_effect=mock_produce)

        def mock_flush():
            mock_msg = MagicMock()
            mock_msg.topic.return_value = "test-topic"
            mock_msg.partition.return_value = 0
            for cb in callbacks:
                cb(None, mock_msg)
            callbacks.clear()

        producer.flush = MagicMock(side_effect=mock_flush)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        # Should not raise — the retry after BufferError succeeds
        _produce_batch(producer, "test-topic", msgs)

        # produce called twice: initial (BufferError) + retry
        assert producer.produce.call_count == 2
        # flush called twice: once for BufferError recovery + once at end
        assert producer.flush.call_count == 2

    def test_produce_batch_no_topic_raises(self):
        """No default topic and no message topic raises RuntimeError."""
        producer = MagicMock()

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(RuntimeError, match="No topic"):
            _produce_batch(producer, None, msgs)

    def test_write_batch_uses_produce_batch(self):
        """write_batch() delegates to _produce_batch with callbacks."""
        producer = _make_mock_producer()
        partition = _KafkaSinkPartition(producer, "test-topic")

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
        ]
        partition.write_batch(msgs)

        assert producer.produce.call_count == 2
        producer.flush.assert_called()
        # Verify on_delivery was passed to produce calls
        for c in producer.produce.call_args_list:
            assert "on_delivery" in c.kwargs

    def test_produce_batch_kafka_exception(self):
        """KafkaException during produce is caught and surfaces as KafkaProduceError."""
        producer = MagicMock()
        err = ConfluentKafkaError(ConfluentKafkaError.MSG_SIZE_TOO_LARGE)
        producer.produce.side_effect = KafkaException(err)

        msgs = [KafkaSinkMessage(key=b"k", value=b"v", topic="t")]
        with pytest.raises(KafkaProduceError) as exc_info:
            _produce_batch(producer, "t", msgs)
        assert len(exc_info.value.errors) == 1
        assert exc_info.value.errors[0][1] == "t"

    def test_produce_batch_kafka_exception_continues(self):
        """After KafkaException, remaining messages still processed."""
        producer = MagicMock()
        err = ConfluentKafkaError(ConfluentKafkaError.MSG_SIZE_TOO_LARGE)
        producer.produce.side_effect = [KafkaException(err), None]

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1", topic="t"),
            KafkaSinkMessage(key=b"k2", value=b"v2", topic="t"),
        ]
        with pytest.raises(KafkaProduceError) as exc_info:
            _produce_batch(producer, "t", msgs)
        assert producer.produce.call_count == 2
        assert len(exc_info.value.errors) == 1

    def test_produce_batch_empty_list(self):
        """Empty items list returns immediately without any calls."""
        producer = MagicMock()
        _produce_batch(producer, "test-topic", [])
        producer.produce.assert_not_called()
        producer.flush.assert_not_called()

    def test_produce_batch_explicit_topic_override(self):
        """Message topic overrides default_topic."""
        producer = _make_mock_producer(delivery_topic="custom-topic")

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1", topic="custom-topic")]
        _produce_batch(producer, "default-topic", msgs)

        call_kwargs = producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "custom-topic"


class TestStatefulKafkaSinkPartition:
    """Tests for _StatefulKafkaSinkPartition snapshot and state tracking."""

    def test_initial_snapshot_zero(self):
        """Fresh partition with no resume state has snapshot() == 0."""
        producer = MagicMock()
        partition = _StatefulKafkaSinkPartition(
            producer, "test-topic", resume_state=None
        )

        assert partition.snapshot() == 0

    def test_snapshot_after_writes(self):
        """Snapshot reflects the number of messages written."""
        producer = _make_mock_producer()
        partition = _StatefulKafkaSinkPartition(
            producer, "test-topic", resume_state=None
        )

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        partition.write_batch(msgs)

        assert partition.snapshot() == 3

    def test_snapshot_after_resume(self):
        """Snapshot accumulates on top of the resume state."""
        producer = _make_mock_producer()
        partition = _StatefulKafkaSinkPartition(producer, "test-topic", resume_state=42)

        msgs = [
            KafkaSinkMessage(key=b"k1", value=b"v1"),
            KafkaSinkMessage(key=b"k2", value=b"v2"),
            KafkaSinkMessage(key=b"k3", value=b"v3"),
        ]
        partition.write_batch(msgs)

        assert partition.snapshot() == 45

    def test_close_flushes(self):
        """close() calls producer.flush() to ensure all messages are delivered."""
        producer = MagicMock()
        partition = _StatefulKafkaSinkPartition(
            producer, "test-topic", resume_state=None
        )

        partition.close()

        producer.flush.assert_called_once()


class TestStatefulKafkaSink:
    """Tests for StatefulKafkaSink partition discovery, routing, and build."""

    def test_raises_on_str_brokers(self):
        """Passing brokers as a string raises TypeError."""
        with pytest.raises(TypeError, match="brokers must be an iterable"):
            StatefulKafkaSink("localhost:9092", ["topicA"])

    def test_raises_on_str_topics(self):
        """Passing topics as a string raises TypeError."""
        with pytest.raises(TypeError, match="topics must be an iterable"):
            StatefulKafkaSink(["localhost:9092"], "topicA")

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_discovers_partitions(self, mock_admin_cls, mock_list_parts):
        """list_parts() returns partition strings from AdminClient discovery."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA", "2-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        parts = sink.list_parts()

        assert parts == ["0-topicA", "1-topicA", "2-topicA"]
        mock_admin_cls.assert_called_once()
        mock_list_parts.assert_called_once()

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_multi_topic(self, mock_admin_cls, mock_list_parts):
        """Multiple topics yield partitions from all of them."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = [
            "0-topicA",
            "1-topicA",
            "0-topicB",
            "1-topicB",
            "2-topicB",
        ]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA", "topicB"])
        parts = sink.list_parts()

        assert len(parts) == 5
        assert "0-topicA" in parts
        assert "1-topicA" in parts
        assert "0-topicB" in parts
        assert "1-topicB" in parts
        assert "2-topicB" in parts

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_cached(self, mock_admin_cls, mock_list_parts):
        """list_parts() only discovers partitions once."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        result1 = sink.list_parts()
        result2 = sink.list_parts()

        assert result1 is result2
        mock_list_parts.assert_called_once()

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_part_fn_triggers_discovery(self, mock_admin_cls, mock_list_parts):
        """part_fn() triggers lazy partition discovery if not yet called."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        idx = sink.part_fn("topicA:key")
        assert 0 <= idx < 2
        mock_list_parts.assert_called_once()

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_part_fn_single_topic(self, mock_admin_cls, mock_list_parts):
        """part_fn routes a key to a valid index within the topic's range."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA", "2-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        idx = sink.part_fn("topicA:key1")

        # Should be in range [0, 3)
        assert 0 <= idx < 3

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_part_fn_multi_topic_routes_correctly(
        self, mock_admin_cls, mock_list_parts
    ):
        """Keys for different topics route to their respective partition ranges."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = [
            "0-topicA",
            "1-topicA",
            "0-topicB",
            "1-topicB",
            "2-topicB",
        ]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA", "topicB"])

        # topicA has offset=0, count=2 → valid indices [0, 1]
        idx_a = sink.part_fn("topicA:somekey")
        assert 0 <= idx_a < 2

        # topicB has offset=2, count=3 → valid indices [2, 3, 4]
        idx_b = sink.part_fn("topicB:anotherkey")
        assert 2 <= idx_b < 5

        # Verify deterministic routing by checking adler32 math
        expected_a = 0 + (adler32(b"somekey") % 2)
        assert idx_a == expected_a
        expected_b = 2 + (adler32(b"anotherkey") % 3)
        assert idx_b == expected_b

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_part_fn_unknown_topic_raises(self, mock_admin_cls, mock_list_parts):
        """Unknown topic in routing key raises ValueError."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])

        with pytest.raises(ValueError, match="Unknown topic"):
            sink.part_fn("unknownTopic:key1")

    def test_raises_on_colon_in_topic(self):
        """Topic names containing ':' raise ValueError."""
        with pytest.raises(ValueError, match="contains ':'"):
            StatefulKafkaSink(["localhost:9092"], ["prod:events"])

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_creates_partition(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """build_part() returns _StatefulKafkaSinkPartition."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        part = sink.build_part("step-1", "0-topicA", None)

        assert isinstance(part, _StatefulKafkaSinkPartition)
        mock_build_producer.assert_called_once()

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_strips_group_id(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """group.id is stripped from the Producer config via _build_producer."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(
            ["localhost:9092"],
            ["topicA"],
            add_config={"group.id": "my-group", "linger.ms": "50"},
        )
        sink.build_part("step-1", "0-topicA", None)

        call_args = mock_build_producer.call_args
        passed_add_config = call_args[0][1]
        assert passed_add_config["group.id"] == "my-group"
        assert passed_add_config["linger.ms"] == "50"

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_enables_idempotence(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """_build_producer is called (which sets enable.idempotence)."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        sink.build_part("step-1", "0-topicA", None)

        mock_build_producer.assert_called_once()

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_sets_error_cb(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """build_part() delegates to _build_producer which sets error_cb."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        sink.build_part("step-1", "0-topicA", None)

        mock_build_producer.assert_called_once()
        assert mock_build_producer.call_args[0][2] == "StatefulKafkaSink"

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_with_resume_state(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """Resume state forwarded to partition, in snapshot()."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        part = sink.build_part("step-1", "0-topicA", 99)

        assert isinstance(part, _StatefulKafkaSinkPartition)
        assert part.snapshot() == 99

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_producer_pool_shared(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """Default pool_size=1: single Producer shared across build_part calls."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        sink.build_part("step-1", "0-topicA", None)
        sink.build_part("step-1", "1-topicA", None)

        mock_build_producer.assert_called_once()

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_producer_pool_per_partition(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """pool_size=None: one Producer per partition."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]
        mock_build_producer.return_value = MagicMock()

        sink = StatefulKafkaSink(
            ["localhost:9092"], ["topicA"], producer_pool_size=None
        )
        sink.build_part("step-1", "0-topicA", None)
        sink.build_part("step-1", "1-topicA", None)

        assert mock_build_producer.call_count == 2

    @patch("bytewax.connectors.kafka._build_producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_producer_pool_size_n(
        self, mock_admin_cls, mock_list_parts, mock_build_producer
    ):
        """pool_size=2: creates 2 Producers, then round-robins."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA", "2-topicA"]
        producers = [MagicMock(), MagicMock()]
        mock_build_producer.side_effect = producers

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"], producer_pool_size=2)
        p0 = sink.build_part("step-1", "0-topicA", None)
        p1 = sink.build_part("step-1", "1-topicA", None)
        p2 = sink.build_part("step-1", "2-topicA", None)

        assert mock_build_producer.call_count == 2
        assert p0._producer is producers[0]
        assert p1._producer is producers[1]
        assert p2._producer is producers[0]

    def test_raises_on_pool_size_zero(self):
        """producer_pool_size=0 raises ValueError."""
        with pytest.raises(ValueError, match="producer_pool_size must be >= 1"):
            StatefulKafkaSink(["localhost:9092"], ["topicA"], producer_pool_size=0)

    def test_raises_on_pool_size_negative(self):
        """producer_pool_size=-1 raises ValueError."""
        with pytest.raises(ValueError, match="producer_pool_size must be >= 1"):
            StatefulKafkaSink(["localhost:9092"], ["topicA"], producer_pool_size=-1)

    def test_colon_in_second_topic(self):
        """Colon validation catches the invalid topic even with a valid first topic."""
        with pytest.raises(ValueError, match="contains ':'"):
            StatefulKafkaSink(["localhost:9092"], ["valid-topic", "invalid:topic"])

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_list_parts_topic_with_dashes(self, mock_admin_cls, mock_list_parts):
        """Topic with dashes in name correctly counted via split('-', 1)."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = [
            "0-my-topic",
            "1-my-topic",
            "2-my-topic",
        ]

        sink = StatefulKafkaSink(["localhost:9092"], ["my-topic"])
        parts = sink.list_parts()

        assert len(parts) == 3
        idx = sink.part_fn("my-topic:key1")
        assert 0 <= idx < 3

    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_part_fn_empty_key(self, mock_admin_cls, mock_list_parts):
        """Empty key routes deterministically via adler32(b'')."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA", "2-topicA"]

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        idx = sink.part_fn("topicA:")

        expected = adler32(b"") % 3
        assert idx == expected


class TestKafkaSinkErrorCb:
    """Tests for KafkaSink error_cb and idempotence via _build_producer."""

    @patch("bytewax.connectors.kafka._build_producer")
    def test_kafka_sink_uses_build_producer(self, mock_build_producer):
        """KafkaSink.build() delegates to _build_producer."""
        mock_build_producer.return_value = MagicMock()

        sink = KafkaSink(["localhost:9092"], "test-topic")
        sink.build("step-1", 0, 1)

        mock_build_producer.assert_called_once()
        assert mock_build_producer.call_args[0][2] == "KafkaSink"

    @patch("bytewax.connectors.kafka.Producer")
    def test_build_producer_sets_error_cb(self, mock_producer_cls):
        """_build_producer passes error_cb as a keyword arg to Producer."""
        mock_producer_cls.return_value = MagicMock()

        _build_producer(["localhost:9092"], {}, "Test")

        call_kwargs = mock_producer_cls.call_args.kwargs
        assert "error_cb" in call_kwargs
        assert callable(call_kwargs["error_cb"])

    @patch("bytewax.connectors.kafka.Producer")
    def test_build_producer_enables_idempotence(self, mock_producer_cls):
        """_build_producer sets enable.idempotence=true."""
        mock_producer_cls.return_value = MagicMock()

        _build_producer(["localhost:9092"], {}, "Test")

        created_config = mock_producer_cls.call_args[0][0]
        assert created_config["enable.idempotence"] == "true"

    @patch("bytewax.connectors.kafka.Producer")
    def test_build_producer_strips_group_id(self, mock_producer_cls):
        """_build_producer strips group.id from config."""
        mock_producer_cls.return_value = MagicMock()

        add_config = {
            "group.id": "my-group",
            "linger.ms": "50",
        }
        _build_producer(["localhost:9092"], add_config, "Test")

        created_config = mock_producer_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["linger.ms"] == "50"

    @patch("bytewax.connectors.kafka.logger")
    @patch("bytewax.connectors.kafka.Producer")
    def test_build_producer_error_cb_logs(self, mock_producer_cls, mock_logger):
        """error_cb triggers logger.error with the label."""
        mock_producer_cls.return_value = MagicMock()

        _build_producer(["localhost:9092"], {}, "MyLabel")

        call_kwargs = mock_producer_cls.call_args.kwargs
        error_cb = call_kwargs["error_cb"]
        mock_err = MagicMock()
        error_cb(mock_err)
        mock_logger.error.assert_called_once()
        assert "MyLabel" in str(mock_logger.error.call_args)

    @patch("bytewax.connectors.kafka.Producer")
    def test_build_producer_add_config_overrides(self, mock_producer_cls):
        """add_config can override bootstrap.servers."""
        mock_producer_cls.return_value = MagicMock()

        add_config = {"bootstrap.servers": "override:9092"}
        _build_producer(["original:9092"], add_config, "Test")

        created_config = mock_producer_cls.call_args[0][0]
        assert created_config["bootstrap.servers"] == "override:9092"


class TestKafkaProduceErrorException:
    """Tests for KafkaProduceError exception attributes and string representation."""

    def test_str_representation(self):
        """String includes count and first error description."""
        mock_err = MagicMock()
        mock_err.__str__ = lambda self: "MSG_TIMED_OUT"

        error = KafkaProduceError([(mock_err, "test-topic[0]")])

        error_str = str(error)
        assert "1 message(s) failed delivery" in error_str

    def test_errors_attribute(self):
        """The .errors attribute contains the full list of (error, summary) tuples."""
        err1 = MagicMock()
        err2 = MagicMock()
        err3 = MagicMock()

        errors_list = [
            (err1, "topicA[0]"),
            (err2, "topicA[1]"),
            (err3, "topicB[0]"),
        ]
        error = KafkaProduceError(errors_list)

        assert error.errors is errors_list
        assert len(error.errors) == 3
        assert error.errors[0] == (err1, "topicA[0]")
        assert error.errors[1] == (err2, "topicA[1]")
        assert error.errors[2] == (err3, "topicB[0]")


class TestConfigDefensiveCopy:
    """Tests that _KafkaSourcePartition does not mutate the caller's config dict."""

    @patch("bytewax.connectors.kafka.Consumer")
    def test_config_not_mutated(self, mock_consumer_cls):
        """Original config dict should not gain 'stats_cb' key."""
        mock_consumer_cls.return_value = MagicMock()
        original_config = {
            "bootstrap.servers": "localhost:9092",
            "group.id": "BYTEWAX_IGNORED",
            "enable.auto.commit": "false",
        }
        original_keys = set(original_config.keys())

        _KafkaSourcePartition(
            "step-1",
            original_config,
            "test-topic",
            0,
            -2,
            None,
            1000,
            True,
        )

        assert set(original_config.keys()) == original_keys
        assert "stats_cb" not in original_config


class TestProcessStats:
    """Tests for _KafkaSourcePartition._process_stats error handling."""

    _BASE_CONFIG: dict = {  # noqa: RUF012
        "bootstrap.servers": "localhost:9092",
        "group.id": "X",
        "enable.auto.commit": "false",
    }

    @patch("bytewax.connectors.kafka.Consumer")
    def test_missing_topics_key(self, mock_consumer_cls):
        """Missing 'topics' key in stats JSON returns without error."""
        mock_consumer_cls.return_value = MagicMock()
        partition = _KafkaSourcePartition(
            "step-1",
            self._BASE_CONFIG,
            "test-topic",
            0,
            -2,
            None,
            1000,
            True,
        )
        # Should not raise
        partition._process_stats(json.dumps({"no_topics_here": {}}))

    @patch("bytewax.connectors.kafka.Consumer")
    def test_wrong_type_in_nested_stats(self, mock_consumer_cls):
        """TypeError from None value returns without error."""
        mock_consumer_cls.return_value = MagicMock()
        partition = _KafkaSourcePartition(
            "step-1",
            self._BASE_CONFIG,
            "test-topic",
            0,
            -2,
            None,
            1000,
            True,
        )
        partition._process_stats(json.dumps({"topics": None}))

    @patch("bytewax.connectors.kafka.Consumer")
    def test_missing_partition_key(self, mock_consumer_cls):
        """Missing partition key in stats returns without error."""
        mock_consumer_cls.return_value = MagicMock()
        partition = _KafkaSourcePartition(
            "step-1",
            self._BASE_CONFIG,
            "test-topic",
            0,
            -2,
            None,
            1000,
            True,
        )
        stats = {"topics": {"test-topic": {"partitions": {}}}}
        partition._process_stats(json.dumps(stats))


class TestGeneratorInput:
    """Tests that KafkaSource and StatefulKafkaSink accept generators."""

    def test_kafka_source_generator_brokers(self):
        """KafkaSource materializes generator brokers into a list."""
        source = KafkaSource(
            (b for b in ["broker1:9092", "broker2:9092"]),
            ["test-topic"],
            tail=False,
        )
        assert source._brokers == ["broker1:9092", "broker2:9092"]
        assert isinstance(source._brokers, list)

    def test_kafka_source_generator_topics(self):
        """KafkaSource materializes generator topics into a list."""
        source = KafkaSource(
            ["broker1:9092"],
            (t for t in ["topic1", "topic2"]),
            tail=False,
        )
        assert source._topics == ["topic1", "topic2"]
        assert isinstance(source._topics, list)

    def test_stateful_sink_generator_brokers(self):
        """StatefulKafkaSink materializes generator brokers into a list."""
        sink = StatefulKafkaSink(
            (b for b in ["broker1:9092", "broker2:9092"]),
            ["test-topic"],
        )
        assert sink._brokers == ["broker1:9092", "broker2:9092"]
        assert isinstance(sink._brokers, list)


def _default_key_fn(msg):
    """Replicate the default key_fn from operators.stateful_output."""
    if msg.topic is None:
        err = (
            "stateful_output requires each message to have "
            "a topic set, or provide a custom key_fn"
        )
        raise ValueError(err)
    key = (msg.key or b"").decode("utf-8", errors="surrogateescape")
    return f"{msg.topic}:{key}"


class TestStatefulOutputKeyFn:
    """Tests for the default key_fn in stateful_output operator."""

    def test_none_topic_raises_value_error(self):
        """Default key_fn raises ValueError when msg.topic is None."""
        msg = KafkaSinkMessage(key=b"k1", value=b"v1", topic=None)
        with pytest.raises(ValueError, match="stateful_output requires"):
            _default_key_fn(msg)

    def test_default_key_fn_with_topic(self):
        """Default key_fn returns 'topic:key' when topic is set."""
        msg = KafkaSinkMessage(key=b"mykey", value=b"v1", topic="my-topic")
        assert _default_key_fn(msg) == "my-topic:mykey"

    def test_default_key_fn_none_key(self):
        """Default key_fn handles None key as empty string."""
        msg = KafkaSinkMessage(key=None, value=b"v1", topic="my-topic")
        assert _default_key_fn(msg) == "my-topic:"


class TestSerializationContextTopicFallback:
    """Tests that SerializationContext gets '' when msg.topic is None."""

    def test_deserialize_key_none_topic_uses_empty_string(self):
        """deserialize_key passes '' to SerializationContext for None topic."""
        msg = KafkaSourceMessage(key=b"raw", value=b"v", topic=None)
        calls = []

        def tracking_deserializer(data, ctx=None, **kwargs):
            calls.append(ctx)
            return b"deserialized"

        # Replicate the shim_mapper from operators.py deserialize_key
        tracking_deserializer(
            msg.key,
            SerializationContext(topic=msg.topic or "", field=MessageField.KEY),
        )

        assert len(calls) == 1
        assert calls[0].topic == ""

    def test_serialize_key_none_topic_uses_empty_string(self):
        """serialize_key passes '' to SerializationContext for None topic."""
        msg = KafkaSinkMessage(key="obj", value=b"v", topic=None)
        calls = []

        def tracking_serializer(data, ctx=None):
            calls.append(ctx)
            return b"serialized"

        tracking_serializer(
            msg.key,
            ctx=SerializationContext(msg.topic or "", MessageField.KEY),
        )

        assert len(calls) == 1
        assert calls[0].topic == ""


_SKIP_SERDE = "serde deps (certifi/schema_registry) not installed"


@pytest.mark.skipif(not _has_serde, reason=_SKIP_SERDE)
class TestParseAvroSchema:
    """Tests for _parse_avro_schema helper in serde.py."""

    def test_parse_from_string(self):
        """Parses a JSON schema string into a fastavro schema."""
        schema_str = json.dumps(
            {
                "type": "record",
                "name": "Test",
                "fields": [{"name": "x", "type": "int"}],
            }
        )
        result = _parse_avro_schema(schema_str)
        assert result is not None
        assert result["name"] == "Test"

    def test_parse_from_schema_object(self):
        """Parses from a confluent Schema object."""
        from confluent_kafka.schema_registry import Schema  # noqa: PLC0415

        schema_str = json.dumps(
            {
                "type": "record",
                "name": "Test2",
                "fields": [{"name": "y", "type": "string"}],
            }
        )
        schema_obj = Schema(schema_str, "AVRO")
        result = _parse_avro_schema(schema_obj)
        assert result is not None
        assert result["name"] == "Test2"

    def test_none_schema_str_raises(self):
        """Schema with None schema_str raises ValueError."""
        mock_schema = MagicMock()
        mock_schema.schema_str = None
        with patch(  # noqa: SIM117
            "bytewax.connectors.kafka.serde.Schema",
            type(mock_schema),
        ):
            with pytest.raises(ValueError, match="schema_str must not be None"):
                _parse_avro_schema(mock_schema)
