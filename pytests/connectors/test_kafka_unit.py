"""Unit tests for Kafka connector fixes (no broker required)."""

from unittest.mock import MagicMock

import pytest
from bytewax.connectors.kafka import KafkaSinkMessage, _KafkaSinkPartition


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

        producer.produce.assert_called_once_with(
            value=b"v1",
            key=b"k1",
            headers=[],
            topic="custom-topic",
            timestamp=0,
        )

    def test_write_batch_no_topic_raises(self):
        """Missing topic raises RuntimeError."""
        producer = MagicMock()
        partition = _KafkaSinkPartition(producer, None)

        msgs = [KafkaSinkMessage(key=b"k1", value=b"v1")]
        with pytest.raises(RuntimeError, match="No topic"):
            partition.write_batch(msgs)
