"""Unit tests for Kafka connector fixes (no broker required)."""

from unittest.mock import MagicMock, patch
from zlib import adler32

import pytest
from bytewax.connectors.kafka import (
    KafkaProduceError,
    KafkaSink,
    KafkaSinkMessage,
    KafkaSource,
    StatefulKafkaSink,
    _KafkaSinkPartition,
    _produce_batch,
    _StatefulKafkaSinkPartition,
)


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
        assert producer.poll.call_count == 2
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
        assert producer.poll.call_count == 3

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


class TestDeliveryCallbacks:
    """Tests for _produce_batch delivery callback error tracking."""

    def test_produce_batch_success(self):
        """All deliveries succeed — no exception raised."""
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                callbacks.append(kwargs["on_delivery"])

        producer.produce = MagicMock(side_effect=mock_produce)

        # When flush is called, trigger all callbacks with no error
        def mock_flush():
            mock_msg = MagicMock()
            mock_msg.topic.return_value = "test-topic"
            mock_msg.partition.return_value = 0
            for cb in callbacks:
                cb(None, mock_msg)
            callbacks.clear()

        producer.flush = MagicMock(side_effect=mock_flush)

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
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
            if "on_delivery" in kwargs:
                callbacks.append(kwargs["on_delivery"])

        producer.produce = MagicMock(side_effect=mock_produce)

        mock_error = MagicMock()
        mock_error.__str__ = lambda self: "MSG_TIMED_OUT"

        def mock_flush():
            mock_msg = MagicMock()
            mock_msg.topic.return_value = "test-topic"
            mock_msg.partition.return_value = 0
            for cb in callbacks:
                cb(mock_error, mock_msg)
            callbacks.clear()

        producer.flush = MagicMock(side_effect=mock_flush)

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
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
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
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
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
        producer = MagicMock()
        callbacks = []

        def mock_produce(**kwargs):
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

    @patch("bytewax.connectors.kafka.Producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_creates_partition(
        self, mock_admin_cls, mock_list_parts, mock_producer_cls
    ):
        """build_part() returns _StatefulKafkaSinkPartition."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA", "1-topicA"]
        mock_producer_cls.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        part = sink.build_part("step-1", "0-topicA", None)

        assert isinstance(part, _StatefulKafkaSinkPartition)
        mock_producer_cls.assert_called_once()

    @patch("bytewax.connectors.kafka.Producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_strips_group_id(
        self, mock_admin_cls, mock_list_parts, mock_producer_cls
    ):
        """group.id is stripped from the Producer config."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_producer_cls.return_value = MagicMock()

        sink = StatefulKafkaSink(
            ["localhost:9092"],
            ["topicA"],
            add_config={"group.id": "my-group", "linger.ms": "50"},
        )
        sink.build_part("step-1", "0-topicA", None)

        created_config = mock_producer_cls.call_args[0][0]
        assert "group.id" not in created_config
        assert created_config["linger.ms"] == "50"

    @patch("bytewax.connectors.kafka.Producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_enables_idempotence(
        self, mock_admin_cls, mock_list_parts, mock_producer_cls
    ):
        """enable.idempotence is set to 'true' in Producer config."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_producer_cls.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        sink.build_part("step-1", "0-topicA", None)

        created_config = mock_producer_cls.call_args[0][0]
        assert created_config["enable.idempotence"] == "true"

    @patch("bytewax.connectors.kafka.Producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_sets_error_cb(
        self, mock_admin_cls, mock_list_parts, mock_producer_cls
    ):
        """build_part() passes error_cb as a keyword arg to Producer."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_producer_cls.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        sink.build_part("step-1", "0-topicA", None)

        call_kwargs = mock_producer_cls.call_args.kwargs
        assert "error_cb" in call_kwargs
        assert callable(call_kwargs["error_cb"])

    @patch("bytewax.connectors.kafka.Producer")
    @patch("bytewax.connectors.kafka._list_parts")
    @patch("bytewax.connectors.kafka.AdminClient")
    def test_build_part_with_resume_state(
        self, mock_admin_cls, mock_list_parts, mock_producer_cls
    ):
        """Resume state forwarded to partition, in snapshot()."""
        mock_admin_cls.return_value = MagicMock()
        mock_list_parts.return_value = ["0-topicA"]
        mock_producer_cls.return_value = MagicMock()

        sink = StatefulKafkaSink(["localhost:9092"], ["topicA"])
        part = sink.build_part("step-1", "0-topicA", 99)

        assert isinstance(part, _StatefulKafkaSinkPartition)
        assert part.snapshot() == 99


class TestKafkaSinkErrorCb:
    """Tests for KafkaSink error_cb and idempotence configuration."""

    @patch("bytewax.connectors.kafka.Producer")
    def test_kafka_sink_sets_error_cb(self, mock_producer_cls):
        """KafkaSink.build() passes error_cb as a keyword arg to Producer."""
        mock_producer_cls.return_value = MagicMock()

        sink = KafkaSink(["localhost:9092"], "test-topic")
        sink.build("step-1", 0, 1)

        call_kwargs = mock_producer_cls.call_args.kwargs
        assert "error_cb" in call_kwargs
        assert callable(call_kwargs["error_cb"])

    @patch("bytewax.connectors.kafka.Producer")
    def test_kafka_sink_enables_idempotence(self, mock_producer_cls):
        """KafkaSink.build() sets enable.idempotence=true."""
        mock_producer_cls.return_value = MagicMock()

        sink = KafkaSink(["localhost:9092"], "test-topic")
        sink.build("step-1", 0, 1)

        created_config = mock_producer_cls.call_args[0][0]
        assert created_config["enable.idempotence"] == "true"


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
