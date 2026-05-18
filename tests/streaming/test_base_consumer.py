"""Tests for BaseKafkaConsumer."""

import time
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaError, KafkaException

from src.streaming.consumers.base_consumer import BaseKafkaConsumer


class _ConcreteConsumer(BaseKafkaConsumer):
    """Minimal concrete subclass for testing the abstract base."""


def _make_message(value: bytes, key: bytes = b"k", partition: int = 0, offset: int = 0, topic: str = "t"):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = value
    msg.key.return_value = key
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.topic.return_value = topic
    return msg


@pytest.fixture
def mock_consumer_cls():
    with patch("src.streaming.consumers.base_consumer.Consumer") as cls:
        yield cls


@pytest.fixture
def consumer(mock_consumer_cls):
    mock_consumer_cls.return_value = MagicMock()
    c = _ConcreteConsumer(
        bootstrap_servers="localhost:9092",
        topic="orders_raw",
        group_id="test-group",
    )
    return c


class TestBaseKafkaConsumerInit:
    def test_constructor_creates_consumer(self, mock_consumer_cls):
        mock_consumer_cls.return_value = MagicMock()
        c = _ConcreteConsumer(
            bootstrap_servers="b:9092",
            topic="t",
            group_id="g",
            auto_offset_reset="latest",
        )
        mock_consumer_cls.assert_called_once_with(
            {
                "bootstrap.servers": "b:9092",
                "group.id": "g",
                "auto.offset.reset": "latest",
                "enable.auto.commit": False,
            }
        )

    def test_constructor_subscribes_to_topic(self, mock_consumer_cls):
        mock_inner = MagicMock()
        mock_consumer_cls.return_value = mock_inner
        _ConcreteConsumer(bootstrap_servers="b:9092", topic="my-topic", group_id="g")
        mock_inner.subscribe.assert_called_once_with(["my-topic"])


class TestBaseKafkaConsumerConsume:
    def test_consume_returns_none_on_timeout(self, consumer):
        consumer._consumer.poll.return_value = None
        assert consumer.consume(timeout=0.1) is None

    def test_consume_returns_message_on_success(self, consumer):
        msg = _make_message(b"hello")
        consumer._consumer.poll.return_value = msg
        result = consumer.consume()
        assert result is msg

    def test_consume_returns_none_on_eof(self, consumer):
        err = MagicMock()
        err.code.return_value = KafkaError._PARTITION_EOF
        msg = MagicMock()
        msg.error.return_value = err
        consumer._consumer.poll.return_value = msg
        assert consumer.consume() is None

    def test_consume_raises_on_fatal_error(self, consumer):
        err = MagicMock()
        err.code.return_value = KafkaError.UNKNOWN_TOPIC_OR_PART
        msg = MagicMock()
        msg.error.return_value = err
        consumer._consumer.poll.return_value = msg
        with pytest.raises(KafkaException):
            consumer.consume()


class TestBaseKafkaConsumerBatch:
    def test_batch_respects_size(self, consumer):
        msgs = [_make_message(f"msg-{i}".encode(), offset=i) for i in range(5)]
        consumer._consumer.poll.side_effect = msgs + [None] * 50
        batch = consumer.consume_batch(batch_size=3, max_wait_sec=5)
        assert len(batch) == 3

    def test_batch_commits_after_collection(self, consumer):
        msg = _make_message(b"x")
        consumer._consumer.poll.side_effect = [msg, None] * 20
        consumer.consume_batch(batch_size=1, max_wait_sec=5)
        consumer._consumer.commit.assert_called_once_with(asynchronous=False)

    def test_batch_returns_empty_on_no_messages(self, consumer):
        consumer._consumer.poll.return_value = None
        batch = consumer.consume_batch(batch_size=10, max_wait_sec=0.1)
        assert batch == []
        consumer._consumer.commit.assert_not_called()

    def test_batch_message_dict_structure(self, consumer):
        msg = _make_message(b"payload", key=b"key1", partition=0, offset=42, topic="orders_raw")
        consumer._consumer.poll.side_effect = [msg, None] * 20
        batch = consumer.consume_batch(batch_size=1, max_wait_sec=5)
        assert len(batch) == 1
        item = batch[0]
        assert item["key"] == b"key1"
        assert item["value"] == b"payload"
        assert item["offset"] == 42
        assert item["topic"] == "orders_raw"


class TestBaseKafkaConsumerClose:
    def test_close_calls_consumer_close(self, consumer):
        consumer.close()
        consumer._consumer.close.assert_called_once()
