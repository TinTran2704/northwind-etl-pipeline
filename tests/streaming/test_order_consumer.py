"""Tests for OrderConsumer."""

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.streaming.consumers.order_consumer import OrderConsumer
from src.streaming.schemas.order_event import OrderEvent


def _make_order_event_bytes(order_id: int = 10248) -> bytes:
    event = OrderEvent(
        event_type="ORDER_CREATED",
        payload={"OrderID": order_id, "CustomerID": "ALFKI", "Freight": 32.38},
    )
    return event.to_json()


def _make_kafka_msg(value: bytes, key: bytes = b"10248"):
    msg = MagicMock()
    msg.error.return_value = None
    msg.value.return_value = value
    msg.key.return_value = key
    msg.partition.return_value = 0
    msg.offset.return_value = 0
    msg.topic.return_value = "orders_raw"
    return msg


@pytest.fixture
def mock_consumer_cls():
    with patch("src.streaming.consumers.base_consumer.Consumer") as cls:
        yield cls


@pytest.fixture
def consumer(mock_consumer_cls):
    mock_inner = MagicMock()
    mock_consumer_cls.return_value = mock_inner
    c = OrderConsumer(bootstrap_servers="localhost:9092")
    c._consumer = mock_inner
    return c


class TestOrderConsumerInit:
    def test_topic_is_orders_raw(self, mock_consumer_cls):
        mock_consumer_cls.return_value = MagicMock()
        c = OrderConsumer()
        assert c.topic == "orders_raw"

    def test_group_id_is_correct(self, mock_consumer_cls):
        mock_consumer_cls.return_value = MagicMock()
        c = OrderConsumer()
        assert c.group_id == "etl_order_consumer_group"


class TestOrderConsumerToParquet:
    def test_writes_parquet_file(self, consumer, tmp_path):
        msg = _make_kafka_msg(_make_order_event_bytes())
        consumer._consumer.poll.side_effect = [msg, None] * 20
        consumer._consumer.commit = MagicMock()

        written = consumer.consume_to_parquet(tmp_path, batch_size=1, max_batches=1)

        parquet_files = list(tmp_path.glob("orders_*.parquet"))
        assert len(parquet_files) == 1
        assert written == 1

    def test_parquet_filename_format(self, consumer, tmp_path):
        msg = _make_kafka_msg(_make_order_event_bytes())
        consumer._consumer.poll.side_effect = [msg, None] * 20
        consumer._consumer.commit = MagicMock()

        consumer.consume_to_parquet(tmp_path, batch_size=1, max_batches=1)

        files = list(tmp_path.glob("orders_*.parquet"))
        assert len(files) == 1
        name = files[0].name
        assert name.startswith("orders_")
        assert name.endswith(".parquet")

    def test_parquet_contains_correct_columns(self, consumer, tmp_path):
        msg = _make_kafka_msg(_make_order_event_bytes())
        consumer._consumer.poll.side_effect = [msg, None] * 20
        consumer._consumer.commit = MagicMock()

        consumer.consume_to_parquet(tmp_path, batch_size=1, max_batches=1)

        df = pd.read_parquet(list(tmp_path.glob("orders_*.parquet"))[0])
        assert "event_id" in df.columns
        assert "event_type" in df.columns
        assert "OrderID" in df.columns

    def test_multiple_batches_write_multiple_files(self, consumer, tmp_path):
        msgs = [_make_kafka_msg(_make_order_event_bytes(10248 + i)) for i in range(4)]
        consumer._consumer.poll.side_effect = msgs + [None] * 40
        consumer._consumer.commit = MagicMock()

        written = consumer.consume_to_parquet(tmp_path, batch_size=2, max_batches=2)

        parquet_files = list(tmp_path.glob("orders_*.parquet"))
        assert len(parquet_files) == 2
        assert written == 4

    def test_stops_when_no_messages(self, consumer, tmp_path):
        consumer._consumer.poll.return_value = None
        consumer._consumer.commit = MagicMock()

        written = consumer.consume_to_parquet(tmp_path, batch_size=10, max_batches=None)

        assert written == 0
        assert list(tmp_path.glob("*.parquet")) == []

    def test_creates_output_dir_if_missing(self, consumer, tmp_path):
        output_dir = tmp_path / "new" / "nested" / "dir"
        msg = _make_kafka_msg(_make_order_event_bytes())
        consumer._consumer.poll.side_effect = [msg, None] * 20
        consumer._consumer.commit = MagicMock()

        consumer.consume_to_parquet(output_dir, batch_size=1, max_batches=1)

        assert output_dir.exists()

    def test_malformed_message_is_skipped(self, consumer, tmp_path):
        bad_msg = _make_kafka_msg(b"not valid json at all")
        good_msg = _make_kafka_msg(_make_order_event_bytes())
        consumer._consumer.poll.side_effect = [bad_msg, good_msg, None] * 10
        consumer._consumer.commit = MagicMock()

        written = consumer.consume_to_parquet(tmp_path, batch_size=2, max_batches=1)

        assert written == 1  # only the good message
