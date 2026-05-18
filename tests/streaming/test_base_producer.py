"""Tests for BaseKafkaProducer."""

from unittest.mock import MagicMock, patch

import pytest

from src.streaming.producers.base_producer import BaseKafkaProducer


class _ConcreteProducer(BaseKafkaProducer):
    """Minimal concrete subclass for testing the abstract base."""


@pytest.fixture
def mock_producer_cls():
    with patch("src.streaming.producers.base_producer.Producer") as cls:
        yield cls


@pytest.fixture
def producer(mock_producer_cls):
    mock_producer_cls.return_value = MagicMock()
    p = _ConcreteProducer(bootstrap_servers="localhost:9092", topic="test-topic")
    return p


class TestBaseKafkaProducerInit:
    def test_constructor_creates_producer(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        p = _ConcreteProducer(bootstrap_servers="broker:9092", topic="t", client_id="ci")
        mock_producer_cls.assert_called_once_with(
            {"bootstrap.servers": "broker:9092", "client.id": "ci"}
        )

    def test_counters_start_at_zero(self, producer):
        assert producer._delivered == 0
        assert producer._failed == 0


class TestBaseKafkaProducerPublish:
    def test_publish_calls_produce(self, producer):
        producer.publish(key="k1", value=b"hello")
        producer._producer.produce.assert_called_once()
        call_kwargs = producer._producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "test-topic"
        assert call_kwargs["key"] == b"k1"
        assert call_kwargs["value"] == b"hello"

    def test_publish_encodes_string_key(self, producer):
        producer.publish(key="abc", value=b"v")
        call_kwargs = producer._producer.produce.call_args.kwargs
        assert call_kwargs["key"] == b"abc"

    def test_publish_calls_poll_zero(self, producer):
        producer.publish(key="x", value=b"y")
        producer._producer.poll.assert_called_with(0)

    def test_publish_uses_override_topic(self, producer):
        producer.publish(key="k", value=b"v", topic="other-topic")
        call_kwargs = producer._producer.produce.call_args.kwargs
        assert call_kwargs["topic"] == "other-topic"

    def test_publish_raises_on_kafka_exception(self, producer):
        from confluent_kafka import KafkaException
        producer._producer.produce.side_effect = KafkaException("queue full")
        with pytest.raises(KafkaException):
            producer.publish(key="k", value=b"v")


class TestBaseKafkaProducerDeliveryCallback:
    def test_on_delivery_success_increments_delivered(self, producer):
        msg = MagicMock()
        producer._on_delivery(None, msg)
        assert producer._delivered == 1
        assert producer._failed == 0

    def test_on_delivery_error_increments_failed(self, producer):
        msg = MagicMock()
        producer._on_delivery("some error", msg)
        assert producer._failed == 1
        assert producer._delivered == 0


class TestBaseKafkaProducerFlushClose:
    def test_flush_returns_pending_count(self, producer):
        producer._producer.flush.return_value = 0
        result = producer.flush()
        assert result == 0

    def test_close_calls_flush(self, producer):
        producer._producer.flush.return_value = 0
        producer.close()
        producer._producer.flush.assert_called()
