"""Connectors for [Kafka](https://kafka.apache.org).

Importing this module requires the
[`confluent-kafka`](https://github.com/confluentinc/confluent-kafka-python)
package to be installed.

The input source returns a stream of
{py:obj}`~bytewax.connectors.kafka.KafkaSourceMessage`. See the
docstring for its use.

You can use {py:obj}`~bytewax.connectors.kafka.KafkaSource` and
{py:obj}`~bytewax.connectors.kafka.KafkaSink` directly:

```{testcode}
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = op.input("kafka-in", flow, KafkaSource(brokers, ["in-topic"]))
processed = op.map("map", kinp, lambda x: KafkaSinkMessage(x.key, x.value))
op.output("kafka-out", processed, KafkaSink(brokers, "out-topic"))
```

Or the custom operators:

```{testcode}
from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")
processed = op.map("map", kinp.oks, lambda x: KafkaSinkMessage(x.key, x.value))
kop.output("kafka-out", processed, brokers=brokers, topic="out-topic")
```

"""

import json
import logging
from dataclasses import dataclass, field
from typing import Dict, Generic, Iterable, List, Optional, Tuple, TypeVar, Union
from zlib import adler32

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import (
    DynamicSink,
    FixedPartitionedSink,
    StatefulSinkPartition,
    StatelessSinkPartition,
)
from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition
from confluent_kafka import KafkaError as ConfluentKafkaError
from confluent_kafka.admin import AdminClient
from prometheus_client import Gauge

logger = logging.getLogger(__name__)
K = TypeVar("K")
"""Type of key in Kafka message."""

V = TypeVar("V")
"""Type of value in a Kafka message."""

K_co = TypeVar("K_co", covariant=True)
"""Type of key in Kafka message."""

V_co = TypeVar("V_co", covariant=True)
"""Type of value in a Kafka message."""

K2 = TypeVar("K2")
"""Type of key in a modified Kafka message."""

V2 = TypeVar("V2")
"""Type of value in a modified Kafka message."""


class KafkaProduceError(RuntimeError):
    """Raised when one or more messages fail delivery to Kafka.

    :arg errors: List of `(KafkaError, topic_partition_summary)` tuples
        for each failed delivery.

    """

    def __init__(self, errors: List[Tuple[ConfluentKafkaError, str]]):
        """Initialize with a list of delivery errors."""
        self.errors = errors
        msg = f"{len(errors)} message(s) failed delivery: {errors[0][0]}"
        super().__init__(msg)


# Set up metrics for Kafka
#
# This is a global var, since the Prometheus REGISTRY
# is also global.
BYTEWAX_CONSUMER_LAG_GAUGE = Gauge(
    "bytewax_kafka_consumer_lag",
    "Difference between last offset on the broker and the currently consumed offset.",
    ["step_id", "topic", "partition"],
)


@dataclass(frozen=True)
class KafkaSourceMessage(Generic[K, V]):
    """Message read from Kafka."""

    key: K
    value: V

    topic: Optional[str] = field(default=None)
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    latency: Optional[float] = field(default=None)
    offset: Optional[int] = field(default=None)
    partition: Optional[int] = field(default=None)
    timestamp: Optional[Tuple[int, int]] = field(default=None)

    def to_sink(self) -> "KafkaSinkMessage[K, V]":
        """Convert a source message to be used with a sink.

        Only {py:obj}`key`, {py:obj}`value` and {py:obj}`timestamp`
        are used.

        """
        return KafkaSinkMessage(key=self.key, value=self.value, headers=self.headers)

    def _with_key(self, key: K2) -> "KafkaSourceMessage[K2, V]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSourceMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSourceMessage[K, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSourceMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSourceMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSourceMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            latency=self.latency,
            offset=self.offset,
            partition=self.partition,
            timestamp=self.timestamp,
        )


@dataclass(frozen=True)
class KafkaError(Generic[K, V]):
    """Error from a {py:obj}`KafkaSource`."""

    err: ConfluentKafkaError
    """Underlying error from the consumer."""

    msg: KafkaSourceMessage[K, V]
    """Message attached to that error."""


def _list_parts(client: AdminClient, topics: Iterable[str]) -> Iterable[str]:
    for topic in topics:
        # List topics one-by-one so if auto-create is turned on,
        # we respect that.
        cluster_metadata = client.list_topics(topic)
        assert cluster_metadata.topics is not None
        topic_metadata = cluster_metadata.topics[topic]
        if topic_metadata.error is not None:
            msg = (
                f"error listing partitions for Kafka topic `{topic!r}`: "
                f"{topic_metadata.error.str()}"
            )
            raise RuntimeError(msg)
        assert topic_metadata.partitions is not None
        part_idxs = topic_metadata.partitions.keys()
        for i in part_idxs:
            yield f"{i}-{topic}"


class _KafkaSourcePartition(
    StatefulSourcePartition[
        Union[
            KafkaSourceMessage[Optional[bytes], Optional[bytes]],
            KafkaError[Optional[bytes], Optional[bytes]],
        ],
        Optional[int],
    ]
):
    def __init__(
        self,
        step_id: str,
        config: dict,
        topic: str,
        part_idx: int,
        starting_offset: int,
        resume_state: Optional[int],
        batch_size: int,
        raise_on_errors: bool,
    ):
        self._offset = starting_offset if resume_state is None else resume_state
        # Collect metrics from Kafka every 1s
        config.update({"stats_cb": self._process_stats})
        consumer = Consumer(config)
        # Assign does not activate consumer grouping.
        consumer.assign([TopicPartition(topic, part_idx, self._offset)])
        self._consumer = consumer
        self._topic = topic
        self._part_idx = part_idx
        self._batch_size = batch_size
        self._eof = False
        self._raise_on_errors = raise_on_errors
        # Labels to use when recording metrics
        self._metrics_labels = {
            "step_id": step_id,
            "topic": topic,
            "partition": part_idx,
        }

    def _process_stats(self, json_stats: str):
        """Process stats collected by librdkafka.

        This function is called by librdkafka based on the
        `statistics.interval.ms` config setting.

        For more information about the `json_stats` payload, see
        https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md
        """
        partition_stats = json.loads(json_stats)["topics"][self._topic]["partitions"][
            str(self._part_idx)
        ]
        # The lag value here would be calculated incorrectly when using values
        # like OFFSET_STORED, or OFFSET_BEGINNING
        if self._offset > 0:
            BYTEWAX_CONSUMER_LAG_GAUGE.labels(**self._metrics_labels).set(
                partition_stats["ls_offset"] - self._offset
            )

    def next_batch(
        self,
    ) -> List[
        Union[
            KafkaSourceMessage[Optional[bytes], Optional[bytes]],
            KafkaError[Optional[bytes], Optional[bytes]],
        ]
    ]:
        if self._eof:
            raise StopIteration()

        msgs = self._consumer.consume(self._batch_size, 0.001)

        batch: List[
            Union[
                KafkaSourceMessage[Optional[bytes], Optional[bytes]],
                KafkaError[Optional[bytes], Optional[bytes]],
            ]
        ] = []
        last_offset = None
        for msg in msgs:
            error = msg.error()
            if error is not None:
                if error.code() == ConfluentKafkaError._PARTITION_EOF:
                    # Set self._eof to True and only raise StopIteration
                    # at the next cycle, so that we can emit messages in
                    # this batch
                    self._eof = True
                    error = None
                    break
                elif self._raise_on_errors:
                    # Discard all the messages in this batch too
                    err_msg = (
                        f"error consuming from Kafka topic `{self._topic!r}`: {error}"
                    )
                    raise RuntimeError(err_msg)

            headers = msg.headers()
            if headers is None:
                headers = []
            kafka_msg = KafkaSourceMessage(
                key=msg.key(),
                value=msg.value(),
                topic=msg.topic(),
                headers=headers,
                latency=msg.latency(),
                offset=msg.offset(),
                partition=msg.partition(),
                timestamp=msg.timestamp(),
            )
            if error is None:
                batch.append(kafka_msg)
            else:
                batch.append(KafkaError(error, kafka_msg))
            last_offset = msg.offset()

        # Resume reading from the next message, not this one.
        if last_offset is not None:
            self._offset = last_offset + 1
        return batch

    def snapshot(self) -> Optional[int]:
        return self._offset

    def close(self) -> None:
        self._consumer.close()


class KafkaSource(
    FixedPartitionedSource[
        Union[
            KafkaSourceMessage[Optional[bytes], Optional[bytes]],
            KafkaError[Optional[bytes], Optional[bytes]],
        ],
        Optional[int],
    ]
):
    """Use a set of Kafka topics as an input source.

    Partitions are the unit of parallelism.
    Can support exactly-once processing.

    Messages are emitted into the dataflow as
    {py:obj}`KafkaSourceMessage` objects with both keys and values as
    optional bytes.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: Iterable[str],
        tail: bool = True,
        starting_offset: int = OFFSET_BEGINNING,
        add_config: Optional[Dict[str, str]] = None,
        batch_size: int = 1000,
        raise_on_errors: bool = True,
    ):
        """Init.

        :arg brokers: List of `host:port` strings of Kafka brokers.

        :arg topics: List of topics to consume from.

        :arg tail: Whether to wait for new data on this topic when the end
            is initially reached.

        :arg starting_offset: Can be either
            `confluent_kafka.OFFSET_BEGINNING` or
            `confluent_kafka.OFFSET_END`. Defaults to beginning of
            topic.

        :arg add_config: Any additional configuration properties. See
            [the `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

        :arg batch_size: How many messages to consume at most at each
            poll. This is 1000 by default. The default setting is a
            suitable starting point for higher throughput dataflows,
            but can be tuned lower to potentially decrease individual
            message processing latency.

        :arg raise_on_errors: If set to False, errors won't stop the
            dataflow, and will be emitted into the dataflow.

        """
        if isinstance(brokers, str):
            msg = "brokers must be an iterable and not a string"
            raise TypeError(msg)
        self._brokers = brokers
        if isinstance(topics, str):
            msg = "topics must be an iterable and not a string"
            raise TypeError(msg)
        self._topics = topics
        self._tail = tail
        self._starting_offset = starting_offset
        self._add_config = {} if add_config is None else add_config
        self._batch_size = batch_size
        self._raise_on_errors = raise_on_errors

    def list_parts(self) -> List[str]:
        """Each Kafka partition is an input partition."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
        }
        config.update(self._add_config)
        client = AdminClient(config)

        return list(_list_parts(client, self._topics))

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> _KafkaSourcePartition:
        """See ABC docstring."""
        idx, topic = for_part.split("-", 1)
        part_idx = int(idx)
        # TODO: Warn and then return None. This might be an indication
        # of dataflow continuation with a new topic (to enable
        # re-partitioning), which is fine.
        assert topic in self._topics, "Can't resume from different set of Kafka topics"

        config = {
            # We'll manage our own "consumer group" via the recovery
            # system.
            "group.id": "BYTEWAX_IGNORED",
            "enable.auto.commit": "false",
            "bootstrap.servers": ",".join(self._brokers),
            "enable.partition.eof": str(not self._tail),
            "statistics.interval.ms": 1000,
        }
        config.update(self._add_config)
        return _KafkaSourcePartition(
            step_id,
            config,
            topic,
            part_idx,
            self._starting_offset,
            resume_state,
            self._batch_size,
            self._raise_on_errors,
        )


@dataclass(frozen=True)
class KafkaSinkMessage(Generic[K_co, V_co]):
    """Message to be written to Kafka."""

    key: K_co
    value: V_co

    topic: Optional[str] = None
    headers: List[Tuple[str, bytes]] = field(default_factory=list)
    partition: Optional[int] = None
    timestamp: int = 0

    def _with_key(self, key: K2) -> "KafkaSinkMessage[K2, V_co]":
        """Returns a new instance with the specified key."""
        # Can't use `dataclasses.replace` directly since it requires
        # the fields you change to be the same type.
        return KafkaSinkMessage(
            key=key,
            value=self.value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_value(self, value: V2) -> "KafkaSinkMessage[K_co, V2]":
        """Returns a new instance with the specified value."""
        return KafkaSinkMessage(
            key=self.key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )

    def _with_key_and_value(self, key: K2, value: V2) -> "KafkaSinkMessage[K2, V2]":
        """Returns a new instance with the specified key and value."""
        return KafkaSinkMessage(
            key=key,
            value=value,
            topic=self.topic,
            headers=self.headers,
            partition=self.partition,
            timestamp=self.timestamp,
        )


def _produce_batch(
    producer: Producer,
    default_topic: Optional[str],
    items: List[KafkaSinkMessage[Optional[bytes], Optional[bytes]]],
) -> None:
    """Produce a batch of messages with delivery error tracking.

    Calls ``producer.produce()`` for each item with an ``on_delivery``
    callback, flushes at the end, and raises :class:`KafkaProduceError`
    if any deliveries failed.
    """
    errors: List[Tuple[ConfluentKafkaError, str]] = []

    def _on_delivery(err, msg):
        if err is not None:
            errors.append((err, f"{msg.topic()}[{msg.partition()}]"))

    for msg in items:
        topic = default_topic if msg.topic is None else msg.topic
        if topic is None:
            err = f"No topic to produce to for {msg}"
            raise RuntimeError(err)

        try:
            producer.produce(
                value=msg.value,
                key=msg.key,
                headers=msg.headers,
                topic=topic,
                timestamp=msg.timestamp,
                on_delivery=_on_delivery,
            )
        except BufferError:
            producer.flush()
            producer.produce(
                value=msg.value,
                key=msg.key,
                headers=msg.headers,
                topic=topic,
                timestamp=msg.timestamp,
                on_delivery=_on_delivery,
            )
        producer.poll(0)
    producer.flush()

    if errors:
        raise KafkaProduceError(errors)


class _KafkaSinkPartition(
    StatelessSinkPartition[KafkaSinkMessage[Optional[bytes], Optional[bytes]]]
):
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic

    def write_batch(
        self, items: List[KafkaSinkMessage[Optional[bytes], Optional[bytes]]]
    ) -> None:
        _produce_batch(self._producer, self._topic, items)

    def close(self) -> None:
        self._producer.flush()


class KafkaSink(DynamicSink[KafkaSinkMessage[Optional[bytes], Optional[bytes]]]):
    """Use a single Kafka topic as an output sink.

    Items consumed from the dataflow must be
    {py:obj}`KafkaSinkMessage` with both keys and values as optional
    bytes.

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        # Optional with no defaults, so you have to explicitly pass
        # `topic=None` if you want to use the topic from the messages
        topic: Optional[str],
        add_config: Optional[Dict[str, str]] = None,
    ):
        """Init.

        :arg brokers: List of `host:port` strings of Kafka brokers.

        :arg topic: Topic to produce to. If it's `None`, the topic to
            produce to will be read in each
            {py:obj}`~bytewax.connectors.kafka.KafkaSinkMessage`.

        :arg add_config: Any additional configuration properties. See
            [the `rdkafka`
            documentation](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)
            for options.

        """
        self._brokers = brokers
        self._topic = topic
        self._add_config = {} if add_config is None else add_config

    def build(
        self, _step_id: str, worker_index: int, worker_count: int
    ) -> _KafkaSinkPartition:
        """See ABC docstring."""
        config = {
            "bootstrap.servers": ",".join(self._brokers),
            "enable.idempotence": "true",
        }
        config.update(self._add_config)
        config.pop("group.id", None)  # Producer doesn't use consumer groups
        config["error_cb"] = lambda err: logger.error(
            "KafkaSink librdkafka error: %s", err
        )
        producer = Producer(config)

        return _KafkaSinkPartition(producer, self._topic)


class _StatefulKafkaSinkPartition(
    StatefulSinkPartition[KafkaSinkMessage[Optional[bytes], Optional[bytes]], int]
):
    """Partition for :class:`StatefulKafkaSink`.

    Tracks the total number of messages written so that the recovery
    system can gate epoch advancement on successful writes.
    """

    def __init__(self, producer: Producer, topic: str, resume_state: Optional[int]):
        self._producer = producer
        self._topic = topic
        self._write_count: int = 0 if resume_state is None else resume_state

    def write_batch(
        self, values: List[KafkaSinkMessage[Optional[bytes], Optional[bytes]]]
    ) -> None:
        _produce_batch(self._producer, self._topic, values)
        self._write_count += len(values)

    def snapshot(self) -> int:
        return self._write_count

    def close(self) -> None:
        self._producer.flush()


class StatefulKafkaSink(
    FixedPartitionedSink[KafkaSinkMessage[Optional[bytes], Optional[bytes]], int]
):
    """Kafka output sink with recovery support.

    Uses :class:`FixedPartitionedSink` so that the sink participates
    in the recovery system's epoch gating.  Each Kafka partition is a
    Bytewax partition and gets its own :class:`_StatefulKafkaSinkPartition`.

    Supports multiple target topics.  Items are routed to the correct
    partition via a ``"topic:message_key"`` routing key set upstream
    (see :func:`~bytewax.connectors.kafka.operators.stateful_output`).

    Enables idempotent producing by default to prevent duplicates from
    librdkafka internal retries.

    Can support at-least-once processing.  Messages from the resume
    epoch will be duplicated right after resume.

    """

    def __init__(
        self,
        brokers: Iterable[str],
        topics: List[str],
        add_config: Optional[Dict[str, str]] = None,
    ):
        """Init.

        :arg brokers: List of ``host:port`` strings of Kafka brokers.

        :arg topics: List of topics to produce to.

        :arg add_config: Any additional configuration properties.  See
            the `rdkafka documentation
            <https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md>`_
            for options.

        """
        if isinstance(brokers, str):
            msg = "brokers must be an iterable and not a string"
            raise TypeError(msg)
        if isinstance(topics, str):
            msg = "topics must be an iterable and not a string"
            raise TypeError(msg)

        self._brokers = list(brokers)
        self._topics = list(topics)
        self._add_config: Dict[str, str] = (
            {} if add_config is None else dict(add_config)
        )

        # Discover partition layout for multi-topic routing.
        admin_config = {"bootstrap.servers": ",".join(self._brokers)}
        admin_config.update(self._add_config)
        admin_config.pop("group.id", None)
        client = AdminClient(admin_config)
        client.poll(0)
        self._parts: List[str] = list(_list_parts(client, self._topics))

        # Build topic → (offset_in_parts_list, partition_count) for part_fn.
        self._topic_ranges: Dict[str, Tuple[int, int]] = {}
        offset = 0
        for topic in self._topics:
            count = sum(1 for p in self._parts if p.endswith(f"-{topic}"))
            self._topic_ranges[topic] = (offset, count)
            offset += count

    def list_parts(self) -> List[str]:
        """See ABC docstring."""
        return self._parts

    def part_fn(self, item_key: str) -> int:
        """Route items to the correct topic partition.

        Expects ``item_key`` in the format ``"topic:message_key"``.
        """
        topic, _, msg_key = item_key.partition(":")
        if topic not in self._topic_ranges:
            msg = f"Unknown topic '{topic}', expected one of {list(self._topic_ranges)}"
            raise ValueError(msg)
        offset, count = self._topic_ranges[topic]
        return offset + (adler32(msg_key.encode()) % count)

    def build_part(
        self,
        step_id: str,
        for_part: str,
        resume_state: Optional[int],
    ) -> _StatefulKafkaSinkPartition:
        """See ABC docstring."""
        config: Dict[str, str] = {
            "bootstrap.servers": ",".join(self._brokers),
            "enable.idempotence": "true",
        }
        config.update(self._add_config)
        config.pop("group.id", None)
        config["error_cb"] = lambda err: logger.error(
            "StatefulKafkaSink librdkafka error: %s", err
        )
        # Parse "0-topicname" → topic name.
        _, _, topic = for_part.partition("-")
        producer = Producer(config)
        return _StatefulKafkaSinkPartition(producer, topic, resume_state)
