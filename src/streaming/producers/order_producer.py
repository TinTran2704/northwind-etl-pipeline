"""
Order producer — reads seed CSV and publishes ORDER_CREATED events to Kafka.

Topic: orders_raw
Spec: docs/12-phase2-kafka-spark.md §12.6
"""

import logging
import random
import time
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from src.streaming.producers.base_producer import BaseKafkaProducer, _DEFAULT_BOOTSTRAP
from src.streaming.schemas.order_event import OrderEvent

logger = logging.getLogger(__name__)

_SEED_ORDERS = Path("data/seed/northwind/orders.csv")

# Seed CSV column → OrderEvent payload key mapping
_COL_MAP = {
    "orderID":        "OrderID",
    "customerID":     "CustomerID",
    "employeeID":     "EmployeeID",
    "orderDate":      "OrderDate",
    "requiredDate":   "RequiredDate",
    "shippedDate":    "ShippedDate",
    "shipVia":        "ShipVia",
    "freight":        "Freight",
    "shipName":       "ShipName",
    "shipAddress":    "ShipAddress",
    "shipCity":       "ShipCity",
    "shipRegion":     "ShipRegion",
    "shipPostalCode": "ShipPostalCode",
    "shipCountry":    "ShipCountry",
}


class OrderProducer(BaseKafkaProducer):
    """Publish Northwind order records to Kafka topic 'orders_raw'.

    Each row in the seed CSV becomes one ORDER_CREATED event.

    Args:
        bootstrap_servers: Kafka broker address (default from env).
        seed_path:         Path to orders seed CSV (override for testing).
    """

    topic = "orders_raw"

    def __init__(
        self,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
        seed_path: Optional[Path] = None,
    ) -> None:
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            topic=self.topic,
            client_id="etl-order-producer",
        )
        self._seed_path = seed_path or _SEED_ORDERS

    def produce_from_seed(
        self,
        delay_range: Tuple[float, float] = (0.1, 0.5),
        limit: Optional[int] = None,
    ) -> int:
        """Read seed CSV and publish one ORDER_CREATED event per row.

        Args:
            delay_range: (min, max) seconds to sleep between messages.
            limit:       Max number of messages to publish (None = all rows).

        Returns:
            Total number of messages published.

        Raises:
            FileNotFoundError: If seed CSV does not exist.
        """
        if not self._seed_path.exists():
            raise FileNotFoundError(f"Seed file not found: {self._seed_path}")

        df = pd.read_csv(self._seed_path, on_bad_lines="skip")
        if limit is not None:
            df = df.head(limit)

        total = len(df)
        logger.info("[ORDER_PRODUCER] publishing %d orders from %s", total, self._seed_path)

        published = 0
        for _, row in df.iterrows():
            payload = {
                _COL_MAP.get(col, col): (
                    None if pd.isna(val) else
                    str(val) if not isinstance(val, (int, float, bool)) else val
                )
                for col, val in row.items()
            }

            event = OrderEvent(
                event_type="ORDER_CREATED",
                payload=payload,
            )

            key = str(int(payload.get("OrderID", 0)))
            self.publish(key=key, value=event.to_json())
            published += 1

            if published % 50 == 0:
                print(f"[ORDER_PRODUCER] Published {published}/{total} messages")

            sleep_secs = random.uniform(*delay_range)
            time.sleep(sleep_secs)

        self.flush()
        print(f"[ORDER_PRODUCER] Done — published {published}/{total} messages")
        logger.info("[ORDER_PRODUCER] finished — published=%d", published)
        return published
