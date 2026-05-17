"""Tests for OrderProducer."""

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from src.streaming.producers.order_producer import OrderProducer


@pytest.fixture
def mock_confluent_producer():
    with patch("src.streaming.producers.base_producer.Producer") as cls:
        mock_p = MagicMock()
        mock_p.flush.return_value = 0
        cls.return_value = mock_p
        yield mock_p


@pytest.fixture
def seed_csv(tmp_path) -> Path:
    csv_content = textwrap.dedent("""\
        orderID,customerID,employeeID,orderDate,requiredDate,shippedDate,shipVia,freight,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry
        10248,VINET,5,1996-07-04,1996-08-01,1996-07-16,3,32.38,Vins et alcools Chevalier,59 rue de l'Abbaye,Reims,,51100,France
        10249,TOMSP,6,1996-07-05,1996-08-16,1996-07-10,1,11.61,Toms Spezialitäten,Luisenstr. 48,Münster,,44087,Germany
        10250,HANAR,4,1996-07-08,1996-08-05,1996-07-12,2,65.83,Hanari Carnes,Rua do Paço 67,Rio de Janeiro,RJ,05454-876,Brazil
    """)
    p = tmp_path / "orders.csv"
    p.write_text(csv_content, encoding="utf-8")
    return p


class TestOrderProducerReadsSeedCSV:
    def test_reads_all_rows_by_default(self, mock_confluent_producer, seed_csv):
        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            count = producer.produce_from_seed(delay_range=(0, 0))
        assert count == 3

    def test_limit_caps_published_count(self, mock_confluent_producer, seed_csv):
        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            count = producer.produce_from_seed(delay_range=(0, 0), limit=2)
        assert count == 2

    def test_produce_calls_publish_once_per_row(self, mock_confluent_producer, seed_csv):
        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            producer.produce_from_seed(delay_range=(0, 0))
        assert mock_confluent_producer.produce.call_count == 3

    def test_missing_seed_raises_file_not_found(self, mock_confluent_producer, tmp_path):
        producer = OrderProducer(seed_path=tmp_path / "missing.csv")
        with pytest.raises(FileNotFoundError):
            producer.produce_from_seed()

    def test_key_is_order_id_string(self, mock_confluent_producer, seed_csv):
        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            producer.produce_from_seed(delay_range=(0, 0), limit=1)
        produce_call = mock_confluent_producer.produce.call_args
        assert produce_call.kwargs["key"] == b"10248"

    def test_column_mapping_applied(self, mock_confluent_producer, seed_csv):
        """Payload keys must use PascalCase (OrderID, not orderID)."""
        import json

        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            producer.produce_from_seed(delay_range=(0, 0), limit=1)
        raw_value = mock_confluent_producer.produce.call_args.kwargs["value"]
        payload = json.loads(raw_value)["payload"]
        assert "OrderID" in payload
        assert "orderID" not in payload

    def test_null_shipped_date_becomes_none(self, mock_confluent_producer, tmp_path):
        import json

        csv_content = (
            "orderID,customerID,employeeID,orderDate,requiredDate,shippedDate,"
            "shipVia,freight,shipName,shipAddress,shipCity,shipRegion,shipPostalCode,shipCountry\n"
            "10248,VINET,5,1996-07-04,1996-08-01,,3,32.38,Name,Addr,City,,51100,France\n"
        )
        p = tmp_path / "orders_null.csv"
        p.write_text(csv_content, encoding="utf-8")
        producer = OrderProducer(seed_path=p)
        with patch("time.sleep"):
            producer.produce_from_seed(delay_range=(0, 0))
        raw_value = mock_confluent_producer.produce.call_args.kwargs["value"]
        payload = json.loads(raw_value)["payload"]
        assert payload["ShippedDate"] is None

    def test_flush_called_after_all_rows(self, mock_confluent_producer, seed_csv):
        producer = OrderProducer(seed_path=seed_csv)
        with patch("time.sleep"):
            producer.produce_from_seed(delay_range=(0, 0))
        mock_confluent_producer.flush.assert_called()
